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
#include <cudf/table/table.hpp>
#include <deque>
#include <functional>
#include <memory>
#include <rmm/cuda_stream_view.hpp>
#include <vector>
#include "velox/core/PlanNode.h"
#include "velox/exec/OutputBuffer.h" // for the Stats structure
#include "velox/exec/Task.h"

namespace facebook::velox::cudf_exchange {

/// @brief Holds a view into a cudf::table along with shared ownership of the
/// source table. This enables zero-copy partitioning using cudf::split() where
/// multiple partitions share the same underlying table data.
struct TableWithMetadata {
  /// Metadata describing the table structure.
  std::unique_ptr<std::vector<uint8_t>> metadata;
  /// View into the source table's data. Does not own the data.
  cudf::table_view tableView;
  /// Shared ownership of the source table. Keeps GPU memory alive until all
  /// partitions referencing this table have been sent.
  std::shared_ptr<cudf::table> sourceTable;
  /// Number of rows in this partition (may be subset of sourceTable).
  cudf::size_type numRows;
  /// The CUDA stream used to create/partition this data. Operations on this
  /// data should use this stream to ensure proper synchronization.
  rmm::cuda_stream_view stream{rmm::cuda_stream_default};
  /// Chunk sequence number (increments for each input batch/table).
  uint64_t chunkSeq{0};
  /// Partition index this data belongs to.
  int32_t partition{0};
  /// Driver ID that produced this data (for tracing).
  int32_t driverId{0};

  /// Returns the GPU memory size of the tableView's columns.
  /// Note: This returns the size of the viewed data, not the entire sourceTable.
  size_t gpuDataSize() const {
    if (!sourceTable) {
      return 0;
    }
    // Recursive lambda to calculate column size including nested children
    std::function<size_t(cudf::column_view const&)> columnSize =
        [&](cudf::column_view const& col) -> size_t {
      size_t size = 0;
      // Data buffer size for fixed-width types only
      if (col.size() > 0 && cudf::is_fixed_width(col.type())) {
        size += col.size() * cudf::size_of(col.type());
      }
      // Null mask size if present
      if (col.nullable()) {
        size += cudf::bitmask_allocation_size_bytes(col.size());
      }
      // Recursively add children sizes (e.g., for strings: offsets + chars)
      for (int c = 0; c < col.num_children(); ++c) {
        size += columnSize(col.child(c));
      }
      return size;
    };

    size_t totalSize = 0;
    for (int i = 0; i < tableView.num_columns(); ++i) {
      totalSize += columnSize(tableView.column(i));
    }
    return totalSize;
  }
};

/// @brief  Callback function for getting data from the queues.
/// A nullptr indicates that there is no more data.
/// The remainingBytes vector contains the sizes for the
/// TableWithMetadata elements remaining in the queue.
using CudfDataAvailableCallback = std::function<void(
    std::unique_ptr<TableWithMetadata> data,
    std::vector<int64_t> remainingBytes)>;

struct CudfDataAvailable {
  CudfDataAvailableCallback callback{nullptr};
  std::unique_ptr<TableWithMetadata> data;
  std::vector<int64_t> remainingBytes;

  void notify() {
    if (callback) {
      callback(std::move(data), remainingBytes);
    }
  }
};

/// @brief The CudfDestinationQueue stores TableWithMetadata for a single
/// downstream task. The data is enqueued by one or more parallel
/// CudfPartitionedOutput operators and dequeued again by the
/// CudfExchangeServer. The CudfDestinationQueue corresponds to the
/// DestinationBuffer of Velox. In Cudf, no serialization/deserialization is
/// needed, the table data is kept in its native form.
class CudfDestinationQueue {
 public:
  struct Stats {
    void recordEnqueue(const TableWithMetadata* data);

    void recordDequeue(const TableWithMetadata* data);

    // what has been queued
    int64_t bytesQueued{0};
    int64_t tablesQueued{0};

    // what has been dequeued
    int64_t bytesSent{0};
    int64_t tablesSent{0};
  };

  /// @brief Enqueues the data to the back of the queue.
  /// @param data Corresponds to a RowVector
  void enqueueBack(std::unique_ptr<TableWithMetadata> data);

  /// @brief Enqueues the data to the front of the queue. This is needed when
  /// a transfer fails.
  /// @param data
  void enqueueFront(std::unique_ptr<TableWithMetadata> data);

  struct Data {
    std::unique_ptr<TableWithMetadata> data;
    std::vector<int64_t> remainingBytes;
    /// Whether the result is returned immediately without invoking the `notify'
    /// callback.
    bool immediate{false};
  };

  /// @brief Removes the data from the front of the queue and transfers
  /// ownership to the caller. If there is no data, 'notify' is installed and it
  /// will be called when data becomes available. In this case, a nullptr is
  /// returned.
  [[nodiscard]] Data getData(CudfDataAvailableCallback notify);

  /// Removes all remaining data from the queue.
  void deleteResults();

  /// Returns and clears the notify callback, if any, along with arguments for
  /// the callback.
  CudfDataAvailable getAndClearNotify();

  /// Finishes this destination buffer, set finished stats.
  void finish();

  /// Returns the stats of this buffer.
  Stats stats() const;

  std::string toString();

 private:
  void clearNotify();

  std::deque<std::unique_ptr<TableWithMetadata>> queue_;
  CudfDataAvailableCallback notify_{nullptr};
  Stats stats_;
};

/// @brief The CudfOutputQueue manages all data coming from a single task that
/// are destined to one or more downstream sink tasks. The CudfOutputQueue uses
/// a vector of DestinationQueues, one for each destination. The CudfOutputQueue
/// is also responsible for tracking the number of drivers that produce data.
/// The number of drivers may change dynamically, so tracking this happens at
/// two levels:
/// - updateNumDrivers is used to track the number of drivers.
/// - noMoreData is called by each driver when the driver is done and has no
/// more data will be added.
class CudfOutputQueue {
 public:
  /// @brief Creates a new output queue for a data-producing task.
  /// @param taskId The id of the source task that produces the data
  /// @param numDestinations The number of destinations, i.e. the partitions.
  /// @param numDrivers The initial number of drivers.
  CudfOutputQueue(
      std::shared_ptr<exec::Task> task,
      uint32_t numDestinations,
      uint32_t numDrivers);

  /// @brief initializes an unitialized queue. This is needed in order to
  /// support delayed construction, i.e. if a "getData" arrives before the queue
  /// exists, the queue manager can create an unitialized queue just for the
  /// sake of storing the callback notification. The queue is then initialized
  /// later properly, and eventually the callback fires.
  /// @return True, if initialization was successful, i.e. the queue wasn't
  /// already initialized.
  bool initialize(
      std::shared_ptr<exec::Task> task,
      uint32_t numDestinations,
      uint32_t numDrivers);

  core::PartitionedOutputNode::Kind kind() const {
    // TODO: Need to support the other modes as well
    return core::PartitionedOutputNode::Kind::kPartitioned;
  }

  /// @brief When we understand the final number of split groups (for grouped
  /// execution only), we need to update the number of producing drivers here.
  void updateNumDrivers(uint32_t newNumDrivers);

  /// @brief Enqueues the data for the given destination. Currently, only
  /// partitioned output mode is supported where the number of destinations is
  /// fixed. Is is an error to provide a destination larger than the initial
  /// number of destinations. This will change in the future and if destination
  /// > numDestinations, then this will be dynamically adapted like it is done
  /// in OutputQueue.
  /// @param destination The destination, must be < numDestinations.
  /// @param data The data.
  void enqueue(int destination, std::unique_ptr<TableWithMetadata> data);

  /// @brief Checks if the queue is over capacity and returns a future if so.
  /// This should be called after enqueueing all partitions for a batch.
  /// @param future Output parameter - populated with a future if blocked.
  /// @return True if blocked (queue over capacity), false otherwise.
  bool checkBlocked(ContinueFuture* future);

  /// @brief Returns the data for the given destination through the callback
  /// function. If data is available, notify will be called immediately. If
  /// there is no data, 'notify' is installed and it will be called when data
  /// becomes available.
  void getData(int destination, CudfDataAvailableCallback notify);

  /// @brief Indicates that a driver is done and won't enqueue any more data.
  void noMoreData();

  /// @brief Returns true if the OutputQueue is finished. Thread-safe.
  bool isFinished();

  /// @brief Same as isFinished but must only be called when owning the lock.
  bool isFinishedLocked();

  /// @brief Deletes all queued data and makes all subsequent getData requests
  /// for 'destination' return empty results.
  void deleteResults(int destination);

  /// Continues any possibly waiting producers. Called when the producer task
  /// has an error or is cancelled.
  void terminate();

  std::string toString();

  /// @brief The stats of this output queue are shoe-horned into the stats
  /// object of OutputBuffer. Since the OutputBuffer's stat object is part of
  /// the Task stats and eventually processed at the Presto layer, this is the
  /// least intrusive way to convey stats information. The stats info from the
  /// CudfDestinationQueue are omitted since also the DestinationBuffer's stats
  /// are never processed by Presto.
  exec::OutputBuffer::Stats stats();

 private:
  // Percentage of maxSize below which a blocked producer should
  // be unblocked.
  static constexpr int32_t kContinuePct = 90;

  // Methods that update the statistics.
  void updateStatsWithEnqueuedLocked(int64_t bytes, int64_t rows);

  // updates the counters and returns promises if the queuedBytes_ counter falls
  // below the continueSize_ low water mark. These promises then need to be
  // realized outside the lock.
  void updateStatsWithFreedLocked(
      int64_t bytes,
      int64_t numTables,
      std::vector<ContinuePromise>& promises);

  void updateTotalQueuedBytesMsLocked();

  int64_t getAverageQueueTimeMsLocked() const;

  // internal function that is called when all drivers are done.
  void noMoreDrivers();

  // If this is called due to a driver processed all its data (no more data),
  // we increment the number of finished drivers. If it is called due to us
  // updating the total number of drivers, we don't.
  void checkIfDone(bool oneDriverFinished);

  bool enqueuePartitionedOutputLocked(
      int destination,
      std::unique_ptr<TableWithMetadata> data,
      std::vector<CudfDataAvailable>& dataAvailableCbs);

  // Reference to the task that owns this CudfQueue.
  std::shared_ptr<exec::Task> task_{nullptr};

  /// If 'queuedBytes_' > 'maxSize_', each producer is blocked after adding
  /// data.
  uint64_t maxSize_;
  // When 'queuedBytes_' goes below 'continueSize_', blocked producers are
  // resumed.
  uint64_t continueSize_;

  // Total number of drivers expected to produce results. This number will
  // decrease in the end of grouped execution, when we understand the real
  // number of producer drivers (depending on the number of split groups).
  uint32_t numDrivers_{0};

  // If true, then we don't allow to add new destination buffers. This only
  // applies for non-partitioned output buffer type.
  bool noMoreQueues_{false};

  // For governing multi-threaded access.
  std::mutex mutex_;

  // One buffer per destination.
  std::vector<std::unique_ptr<CudfDestinationQueue>> queues_;

  // keep track of the number of drivers that have finished.
  uint32_t numFinished_{0};

  bool atEnd_ = false;

  // promises when buffer reached capacity and blocked further enqueueing.
  std::vector<ContinuePromise> promises_;

  // actual data in 'queues_'
  int64_t queuedBytes_{0};
  int64_t queuedTables_{0};

  // The total number of bytes/rows/tables sent via this output queue.
  int64_t totalBytesSent_{0};
  int64_t totalRowsSent_{0};
  int64_t totalTablesSent_{0};

  // Time since last change in queuedBytes_. Used to compute total time data
  // is queued. Ignored if queuedBytes_ is zero.
  uint64_t queueStartMs_;

  // Total time data is queued as bytes * time.
  double totalQueuedBytesMs_;
};

} // namespace facebook::velox::cudf_exchange
