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
#include "velox/experimental/ucx-exchange/UcxExchangeQueue.h"

#include <limits>

namespace facebook::velox::ucx_exchange {

UcxExchangeQueue::UcxExchangeQueue(
    int32_t numberOfConsumers,
    uint64_t configuredBytes,
    std::shared_ptr<const ReceiveCreditPolicy> policy)
    : numOpenConsumers_{numberOfConsumers},
      configuredBytes_{configuredBytes},
      policy_{std::move(policy)} {
  VELOX_CHECK_GE(numberOfConsumers, 1);
  VELOX_CHECK_NOT_NULL(policy_);
}

uint64_t UcxExchangeQueue::limitLocked() const {
  if (configuredBytes_ == 0) {
    return std::numeric_limits<uint64_t>::max();
  }
  return policy_->limitBytes(
      {.configuredBytes = configuredBytes_,
       .numConsumers = static_cast<uint32_t>(numOpenConsumers_),
       .averagePayloadCost =
           receivedTables_ > 0 ? receivedCost_ / receivedTables_ : 0});
}

ReceiveAdmission UcxExchangeQueue::tryReserveLocked(
    uint64_t cost,
    const std::shared_ptr<CommElement>& source) {
  VELOX_CHECK_NOT_NULL(source);
  // Drop waiters that have been destroyed, so that the head is live.
  while (!creditWaiters_.empty() && creditWaiters_.front().expired()) {
    creditWaiters_.pop_front();
  }
  const bool atHead = !creditWaiters_.empty() &&
      creditWaiters_.front().lock().get() == source.get();
  const bool othersAhead = !creditWaiters_.empty() && !atHead;

  const uint64_t held = reservedBytes_ + queuedCost_;
  const uint64_t limit = limitLocked();
  const bool fits = held <= limit && cost <= limit - held;
  if (!othersAhead && (held == 0 || fits)) {
    if (atHead) {
      creditWaiters_.pop_front();
    }
    reservedBytes_ += cost;
    return ReceiveAdmission::kAdmitted;
  }
  if (!atHead) {
    creditWaiters_.push_back(source);
  }
  return ReceiveAdmission::kWaiting;
}

std::shared_ptr<CommElement> UcxExchangeQueue::releaseLocked(uint64_t cost) {
  VELOX_CHECK_GE(
      reservedBytes_, cost, "UCX receive reservation released twice");
  reservedBytes_ -= cost;
  return headWaiterLocked();
}

std::shared_ptr<CommElement> UcxExchangeQueue::headWaiterLocked() {
  while (!creditWaiters_.empty()) {
    if (auto head = creditWaiters_.front().lock()) {
      return head;
    }
    creditWaiters_.pop_front();
  }
  return nullptr;
}

std::vector<std::shared_ptr<CommElement>>
UcxExchangeQueue::takeAllWaitersLocked() {
  std::vector<std::shared_ptr<CommElement>> waiters;
  waiters.reserve(creditWaiters_.size());
  for (auto& weakWaiter : creditWaiters_) {
    if (auto waiter = weakWaiter.lock()) {
      waiters.push_back(std::move(waiter));
    }
  }
  creditWaiters_.clear();
  return waiters;
}

std::shared_ptr<CommElement> UcxExchangeQueue::consumerClosedLocked(
    std::vector<ContinuePromise>& promises) {
  VELOX_CHECK_GT(numOpenConsumers_, 0, "More consumers closed than opened");
  --numOpenConsumers_;
  wakeConsumersLocked(promises);
  return headWaiterLocked();
}

void UcxExchangeQueue::wakeConsumersLocked(
    std::vector<ContinuePromise>& promises) {
  size_t wokenConsumers = 0;
  while (!promises_.empty()) {
    VELOX_CHECK_LE(promises_.size(), numOpenConsumers_);
    const int32_t unblockedConsumers = numOpenConsumers_ - promises_.size();
    const int64_t unassignedTables = queue_.size() - unblockedConsumers;
    if (unassignedTables <= 0) {
      break;
    }
    // Resume one of the waiting drivers.
    auto it = promises_.begin();
    promises.push_back(std::move(it->second));
    promises_.erase(it);
    ++wokenConsumers;
  }
  if (wokenConsumers > 0) {
    VLOG(2) << "[EX-QUEUE] waking " << wokenConsumers << " consumers"
            << " queueSize=" << queue_.size();
  }
}

void UcxExchangeQueue::noMoreSources() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    noMoreSources_ = true;
    promises = checkCompleteLocked();
  }
  clearPromises(promises);
}

void UcxExchangeQueue::close() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    promises = closeLocked();
  }
  clearPromises(promises);
}

std::shared_ptr<CommElement> UcxExchangeQueue::enqueueLocked(
    PackedTableWithStreamPtr&& data,
    std::vector<ContinuePromise>& promises) {
  if (data == nullptr) {
    ++numCompleted_;
    VLOG(2) << "[EX-QUEUE] source completed (null enqueued)"
            << " numCompleted=" << numCompleted_
            << " numSources=" << numSources_
            << " noMoreSources=" << noMoreSources_;
    auto completedPromises = checkCompleteLocked();
    promises.reserve(promises.size() + completedPromises.size());
    for (auto& promise : completedPromises) {
      promises.push_back(std::move(promise));
    }
    return nullptr;
  }

  const auto cost = data->receiveCost;
  VELOX_CHECK_GE(
      reservedBytes_, cost, "UCX receive enqueued more than it reserved");
  reservedBytes_ -= cost;
  queuedCost_ += cost;
  receivedCost_ += cost;

  auto dataSize = data->gpuDataSize();
  totalBytes_ += dataSize;
  if (peakBytes_ < totalBytes_) {
    peakBytes_ = totalBytes_;
  }

  ++receivedTables_;
  receivedBytes_ += dataSize;

  queue_.push_back(std::move(data));
  publishSizeLocked();

  // High-water-mark alerts: log when queue size crosses thresholds.
  auto newSize = static_cast<int64_t>(queue_.size());
  if (newSize > peakSize_) {
    if ((peakSize_ < 100 && newSize >= 100) ||
        (peakSize_ < 1000 && newSize >= 1000) ||
        (peakSize_ < 10000 && newSize >= 10000)) {
      VLOG(1) << "[EX-QUEUE] high water mark: queueSize=" << newSize
              << " peakBytes=" << peakBytes_
              << " receivedTables=" << receivedTables_;
    }
    peakSize_ = newSize;
  }

  wakeConsumersLocked(promises);
  // The new table updates the average payload cost, which can raise the
  // limit.
  return headWaiterLocked();
}

void UcxExchangeQueue::addPromiseLocked(
    int consumerId,
    ContinueFuture* future,
    ContinuePromise* stalePromise) {
  ContinuePromise promise{"UcxExchangeQueue::dequeue"};
  *future = promise.getSemiFuture();
  auto it = promises_.find(consumerId);
  if (it != promises_.end()) {
    // resolve stale promises outside the lock to avoid broken promises
    *stalePromise = std::move(it->second);
    it->second = std::move(promise);
  } else {
    promises_[consumerId] = std::move(promise);
  }
  VELOX_CHECK_LE(promises_.size(), numOpenConsumers_);
}

PackedTableWithStreamPtr UcxExchangeQueue::dequeueLocked(
    int consumerId,
    bool* atEnd,
    ContinueFuture* future,
    ContinuePromise* stalePromise,
    std::shared_ptr<CommElement>* waiter) {
  VELOX_CHECK_NOT_NULL(future);
  VELOX_CHECK_NOT_NULL(waiter);
  if (!error_.empty()) {
    *atEnd = true;
    VELOX_FAIL(error_);
  }

  *atEnd = false;

  // check whether the queue is empty.
  PackedTableWithStreamPtr data = nullptr;
  if (queue_.empty()) {
    if (atEnd_) {
      *atEnd = true;
    } else {
      VLOG(2) << "[EX-QUEUE] consumer=" << consumerId
              << " blocked (empty queue, waiting for data)"
              << " numSources=" << numSources_
              << " numCompleted=" << numCompleted_
              << " waitingConsumers=" << (promises_.size() + 1);
      addPromiseLocked(consumerId, future, stalePromise);
    }
    return data;
  }

  data = std::move(queue_.front());
  queue_.pop_front();
  publishSizeLocked();
  totalBytes_ -= data->gpuDataSize();
  queuedCost_ -= data->receiveCost;
  *waiter = headWaiterLocked();

  return data;
}

void UcxExchangeQueue::setError(std::string_view error) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!error_.empty()) {
      return;
    }
    error_ = error;
    atEnd_ = true;
    // NOTE: clear the serialized page queue as we won't consume from an
    // errored queue.
    queue_.clear();
    publishSizeLocked();
    queuedCost_ = 0;
    promises = clearAllPromisesLocked();
  }
  clearPromises(promises);
}

} // namespace facebook::velox::ucx_exchange
