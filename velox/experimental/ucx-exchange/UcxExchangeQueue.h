/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cudf/contiguous_split.hpp>
#include <atomic>
#include <cinttypes>
#include <cuda/stream>
#include <deque>
#include <memory>
#include "velox/common/base/Exceptions.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/experimental/ucx-exchange/CommElement.h"
#include "velox/experimental/ucx-exchange/ReceiveCreditPolicy.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::ucx_exchange {

/// Struct that bundles a packed_table with the CUDA stream that was used
/// to allocate its memory. This allows the receiver to reuse the same stream
/// for subsequent operations on the data.
struct PackedTableWithStream {
  std::unique_ptr<cudf::packed_table> packedTable;
  cuda::stream_ref stream{cudaStream_t{cudaStreamDefault}};

  /// Logical rows in 'packedTable', as reported by the producer. Authoritative:
  /// cudf::table_view::num_rows() derives the count from the columns and so
  /// returns 0 for a table with none, which is what an exchange fragment with
  /// an empty output layout sends. 32-bit because a cuDF table cannot hold more
  /// rows than a cudf::size_type can index.
  vector_size_t numRows{0};

  /// Bytes this table was charged against the receive credit limit when its
  /// receive was admitted. Released when a consumer driver dequeues it.
  uint64_t receiveCost{0};

  PackedTableWithStream() = default;
  PackedTableWithStream(
      std::unique_ptr<cudf::packed_table>&& table,
      cuda::stream_ref s,
      vector_size_t numRows)
      : packedTable(std::move(table)), stream(s), numRows(numRows) {}

  /// Returns the size of the GPU data buffer, or 0 if packedTable is null.
  size_t gpuDataSize() const {
    return packedTable ? packedTable->data.gpu_data->size() : 0;
  }
};

using PackedTableWithStreamPtr = std::unique_ptr<PackedTableWithStream>;

/// Result of UcxExchangeQueue::tryReserveLocked().
enum class ReceiveAdmission {
  /// The bytes are reserved; the caller may allocate and post the receive.
  kAdmitted,
  /// The caller is on the wait list and will be returned by a later release.
  kWaiting,
};

/// Holds the payloads a UcxExchangeClient has received until its consumer
/// drivers take them, and bounds the bytes held for payloads that are being
/// received or wait in the queue.
///
/// A source reserves a payload's cost before it allocates the receive buffer.
/// If the reservation would exceed the limit, the source joins a first-in,
/// first-out wait list. Every change that can free credit returns the head of
/// the list, which the caller reschedules after releasing 'mutex()'. The check
/// and joining the list happen under the same mutex as every release, so a
/// release cannot be lost between them.
class UcxExchangeQueue {
 public:
  /// @param numberOfConsumers Consumer drivers reading from this queue.
  /// @param configuredBytes exchange.max_buffer_size. Zero disables the
  /// receive credit limit.
  /// @param policy Computes the limit from the configured bytes, the open
  /// consumer drivers and the average payload cost.
  UcxExchangeQueue(
      int32_t numberOfConsumers,
      uint64_t configuredBytes,
      std::shared_ptr<const ReceiveCreditPolicy> policy);

  ~UcxExchangeQueue() {
    clearAllPromises();
  }

  std::mutex& mutex() {
    return mutex_;
  }

  /// Returns true if no packed table is queued. Safe to call without
  /// 'mutex_'.
  bool empty() const {
    return size() == 0;
  }

  /// Enqueues 'data' to the queue. One random promise(top of promise queue)
  /// associated with the future that is waiting for the data from the queue
  /// is returned in 'promises' if 'data' is not nullptr. When 'data' is
  /// nullptr and the queue is completed serving data, all left over promises
  /// will be returned in 'promises'. When 'data' is nullptr and the queue is
  /// not completed serving data, no 'promises' will be added and returned.
  ///
  /// Moves 'data->receiveCost' from reserved to queued bytes. Returns the head
  /// of the credit wait list if the enqueue may have raised the limit, else
  /// nullptr.
  std::shared_ptr<CommElement> enqueueLocked(
      PackedTableWithStreamPtr&& data,
      std::vector<ContinuePromise>& promises);

  /// Reserves 'cost' bytes for a payload that 'source' is about to receive.
  /// Admits it if the queue holds nothing, or if no other source is waiting
  /// ahead of 'source' and the reserved and queued bytes plus 'cost' fit the
  /// limit. Otherwise puts 'source' on the wait list, at the head if it was
  /// just taken from there, and at the tail if not.
  ReceiveAdmission tryReserveLocked(
      uint64_t cost,
      const std::shared_ptr<CommElement>& source);

  /// Gives back a reservation of 'cost' bytes whose payload was not enqueued.
  /// Returns the head of the wait list, or nullptr.
  std::shared_ptr<CommElement> releaseLocked(uint64_t cost);

  /// Puts 'source' at the tail of the wait list without admitting it, for a
  /// source whose buffer allocation failed and that must wait for memory to be
  /// freed. Returns false, and leaves the list unchanged, if nothing is
  /// reserved or queued: no release can then follow, so waiting would hang.
  bool waitForReleaseLocked(const std::shared_ptr<CommElement>& source);

  /// Removes 'source' from the wait list and gives back 'cost' reserved
  /// bytes, for a source that stops. Returns the head of the wait list, or
  /// nullptr.
  std::shared_ptr<CommElement> cancelLocked(
      const CommElement* source,
      uint64_t cost);

  /// Changes a reservation from 'fromCost' to 'toCost' bytes without an
  /// admission check, for an intra-node source that reserved an estimate
  /// before its payload's size was known. Returns the head of the wait list,
  /// or nullptr.
  std::shared_ptr<CommElement> resizeReservationLocked(
      uint64_t fromCost,
      uint64_t toCost);

  /// Returns the average cost of the payloads received so far, or zero.
  uint64_t averagePayloadCostLocked() const {
    return receivedTables_ > 0 ? receivedCost_ / receivedTables_ : 0;
  }

  /// Returns the policy that sets the limit and the cost of a payload.
  const ReceiveCreditPolicy& policy() const {
    return *policy_;
  }

  /// Records that consumer driver 'consumerId' has closed. Drops its waiting
  /// promise, if any, into 'promises' so that the caller fulfils it. Also adds
  /// a waiting driver if a queued payload no longer has a driver to take it,
  /// and returns the head of the wait list, since the limit may have changed.
  std::shared_ptr<CommElement> consumerClosedLocked(
      int consumerId,
      std::vector<ContinuePromise>& promises);

  /// Removes and returns every source on the wait list, for close and error,
  /// so that each can be rescheduled and clean up.
  std::vector<std::shared_ptr<CommElement>> takeAllWaitersLocked();

  /// Returns the current receive credit limit in bytes.
  uint64_t limitLocked() const;

  /// Returns the bytes reserved for payloads whose receive is in progress.
  uint64_t reservedBytesLocked() const {
    return reservedBytes_;
  }

  /// Returns the bytes charged for payloads waiting in the queue.
  uint64_t queuedCostLocked() const {
    return queuedCost_;
  }

  /// Returns the number of sources waiting for credit.
  size_t numWaitersLocked() const {
    return creditWaiters_.size();
  }

  /// If data is permanently not available, e.g. the source cannot be
  /// contacted, this registers an error message and causes the reading
  /// Exchanges to throw with the message.
  void setError(std::string_view error);

  bool isInError() {
    return !error_.empty();
  }
  /// Returns a PackedTableWithStream object.
  ///
  /// Returns a nullptr if no data is available. If data is still expected,
  /// sets 'atEnd' to false and 'future' to a Future that will complete when
  /// data arrives. If no more data is expected, sets 'atEnd' to true. Returns
  /// one PackedTableWithStream if data is available.
  /// It's possible that the same consumer is already waiting for data. In this
  /// case, a stalePromise is returned which needs to be cleaned up.
  /// Releases the dequeued table's receive cost and returns the head of the
  /// credit wait list in 'waiter', or nullptr.
  PackedTableWithStreamPtr dequeueLocked(
      int consumerId,
      bool* atEnd,
      ContinueFuture* future,
      ContinuePromise* stalePromise,
      std::shared_ptr<CommElement>* waiter);

  /// Returns the number of queued packed tables. Safe to call without
  /// 'mutex_'.
  int32_t size() const {
    return size_.load(std::memory_order_relaxed);
  }

  /// Returns the total bytes held by packed tables in 'this'.
  int64_t totalBytes() const {
    return totalBytes_;
  }

  /// Returns the maximum value of total bytes.
  uint64_t peakBytes() const {
    return peakBytes_;
  }

  /// Returns total number of packed tables received from all sources.
  uint64_t receivedTables() const {
    return receivedTables_;
  }

  /// Returns an average size of tables. Returns 0 if hasn't received
  /// any tables yet.
  uint64_t averageReceivedTablesBytes() const {
    return receivedTables_ > 0 ? receivedBytes_ / receivedTables_ : 0;
  }

  void addSourceLocked() {
    VELOX_CHECK(!noMoreSources_, "addSource called after noMoreSources");
    numSources_++;
  }

  void noMoreSources();

  void close();

 private:
  // Publishes the depth of 'queue_' to 'size_'. Call with 'mutex_' held after
  // every mutation of 'queue_'.
  void publishSizeLocked() {
    size_.store(static_cast<int32_t>(queue_.size()), std::memory_order_relaxed);
  }

  std::vector<ContinuePromise> closeLocked() {
    queue_.clear();
    publishSizeLocked();
    queuedCost_ = 0;
    return clearAllPromisesLocked();
  }

  // Returns the head of the wait list if any credit is available for it, so
  // that the caller can reschedule it. The head stays on the list until it
  // asks again, which keeps a newcomer from overtaking it meanwhile.
  std::shared_ptr<CommElement> headWaiterLocked();

  // Wakes as many waiting consumer drivers as there are queued tables that no
  // running driver will take.
  void wakeConsumersLocked(std::vector<ContinuePromise>& promises);

  std::vector<ContinuePromise> checkCompleteLocked() {
    if (noMoreSources_ && numCompleted_ == numSources_) {
      atEnd_ = true;
      return clearAllPromisesLocked();
    }
    return {};
  }

  void addPromiseLocked(
      int consumerId,
      ContinueFuture* future,
      ContinuePromise* stalePromise);

  void clearAllPromises() {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(mutex_);
      promises = clearAllPromisesLocked();
    }
    clearPromises(promises);
  }

  std::vector<ContinuePromise> clearAllPromisesLocked() {
    std::vector<ContinuePromise> promises;
    promises.reserve(promises_.size());
    auto it = promises_.begin();
    while (it != promises_.end()) {
      promises.push_back(std::move(it->second));
      it = promises_.erase(it);
    }
    VELOX_CHECK(promises_.empty());
    return promises;
  }

  static void clearPromises(std::vector<ContinuePromise>& promises) {
    for (auto& promise : promises) {
      promise.setValue();
    }
  }

  // Consumer drivers that have not closed. A driver that has closed never
  // comes back for a table, so the wake-up count must not include it.
  int32_t numOpenConsumers_;

  const uint64_t configuredBytes_;
  const std::shared_ptr<const ReceiveCreditPolicy> policy_;
  // Bytes reserved for payloads whose receive has been admitted but not yet
  // enqueued.
  uint64_t reservedBytes_{0};
  // Bytes charged for the tables in 'queue_'.
  uint64_t queuedCost_{0};
  // Total cost of all payloads received, for the average the policy uses.
  uint64_t receivedCost_{0};
  // Sources waiting for credit, oldest first. Weak, so that a waiting source
  // that is destroyed is skipped rather than kept alive.
  std::deque<std::weak_ptr<CommElement>> creditWaiters_;

  int numCompleted_{0};
  int numSources_{0};
  bool noMoreSources_{false};
  bool atEnd_{false};

  std::mutex mutex_;
  std::deque<PackedTableWithStreamPtr> queue_;
  // Depth of 'queue_', maintained under 'mutex_' so that size() and empty() can
  // be read without it. The Communicator progress thread polls the depth on
  // every UcxExchangeSource::process() call to decide backpressure, and taking
  // 'mutex_' there would contend with dequeueLocked() on the driver threads.
  // Reading queue_.size() unlocked is not an option: std::deque computes it by
  // subtracting iterators over the node map, so a concurrent push_back or
  // pop_front that crosses a node boundary can yield an arbitrary value.
  std::atomic<int32_t> size_{0};
  // The map from consumer id to the waiting promise
  folly::F14FastMap<int, ContinuePromise> promises_;

  // When set, all promises will be realized and the next dequeue will
  // throw an exception with this message.
  std::string error_;
  // Total size of packed tables in queue.
  int64_t totalBytes_{0};
  // Number of packed tables received.
  int64_t receivedTables_{0};
  // Total size of packed tables received. Used to calculate an average
  // expected size.
  int64_t receivedBytes_{0};
  // Maximum value of totalBytes_.
  int64_t peakBytes_{0};
  // Peak queue size (number of items). Used for high-water-mark alerts.
  int64_t peakSize_{0};
};

} // namespace facebook::velox::ucx_exchange
