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
#include "velox/experimental/ucx-exchange/UcxExchangeClient.h"

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::velox::ucx_exchange {

UcxExchangeClient::UcxExchangeClient(
    std::string taskId,
    int destination,
    int32_t numberOfConsumers,
    uint64_t maxBufferedBytes,
    std::shared_ptr<const ReceiveCreditPolicy> creditPolicy)
    : taskId_{std::move(taskId)},
      queue_(
          std::make_shared<UcxExchangeQueue>(
              numberOfConsumers,
              maxBufferedBytes,
              std::move(creditPolicy))) {
  VELOX_CHECK_GE(
      destination, 0, "Exchange client destination must not be negative");
}

void UcxExchangeClient::addRemoteTaskId(std::string_view remoteTaskId) {
  std::shared_ptr<UcxExchangeSource> toClose;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());

    const auto [remoteTaskIdIt, inserted] =
        remoteTaskIds_.emplace(remoteTaskId);
    if (!inserted) {
      // Do not add sources twice. Presto protocol may add duplicate sources
      // and the task updates have no guarantees of arriving in order.
      return;
    }

    std::shared_ptr<UcxExchangeSource> source;
    source = UcxExchangeSource::create(taskId_, *remoteTaskIdIt, queue_);

    if (closed_) {
      toClose = std::move(source);
    } else {
      sources_.push_back(source);
      queue_->addSourceLocked();
      source->setRegistered();
      VLOG(3) << "@" << taskId_
              << " Added remote split for task: " << remoteTaskId;
    }
  }

  // Outside of lock.
  if (toClose) {
    toClose->close();
  }
}

void UcxExchangeClient::noMoreRemoteTasks() {
  VLOG(3) << "@" << taskId_ << " UcxExchangeClient::noMoreRemoteTasks called.";
  queue_->noMoreSources();
}

void UcxExchangeClient::close() {
  std::vector<std::shared_ptr<UcxExchangeSource>> sources;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (closed_) {
      return;
    }
    closed_ = true;
    sources = std::move(sources_);
  }

  // Outside of mutex.
  for (auto& source : sources) {
    source->close();
  }
  queue_->close();
}

void UcxExchangeClient::consumerClosed(int consumerId) {
  std::vector<ContinuePromise> promises;
  std::shared_ptr<CommElement> waiter;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    waiter = queue_->consumerClosedLocked(consumerId, promises);
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
  UcxExchangeSource::rescheduleWaiter(waiter);
}

folly::F14FastMap<std::string, RuntimeMetric> UcxExchangeClient::stats() const {
  // TODO: Implement stats collection.
  folly::F14FastMap<std::string, RuntimeMetric> stats;
  return stats;
}

PackedTableWithStreamPtr
UcxExchangeClient::next(int consumerId, bool* atEnd, ContinueFuture* future) {
  VLOG(3) << "@" << taskId_ << " UcxExchangeClient::next called for consumerId "
          << consumerId;
  PackedTableWithStreamPtr data;
  ContinuePromise stalePromise = ContinuePromise::makeEmpty();
  std::shared_ptr<CommElement> creditWaiter;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (closed_) {
      *atEnd = true;
      return data;
    }

    // not closed, get packed table from the queue.
    *atEnd = false;
    data = queue_->dequeueLocked(
        consumerId, atEnd, future, &stalePromise, &creditWaiter);
    if (*atEnd) {
      // queue is closed!
      return data;
    }

    // Per-stage progress counters.
    if (data != nullptr) {
      ++totalDequeued_;
      if (totalDequeued_ % 1000 == 0) {
        VLOG(1) << "[PROGRESS] @" << taskId_ << " consumer=" << consumerId
                << " dequeued=" << totalDequeued_
                << " queueSize=" << queue_->size()
                << " queueBytes=" << queue_->totalBytes();
      }
    }
  }

  // Outside of lock: reschedule the source this dequeue freed credit for, and
  // fulfill the stale promise. Communicator::addToWorkQueue() takes its own
  // lock, so it must not run under the queue's mutex.
  UcxExchangeSource::rescheduleWaiter(creditWaiter);
  if (stalePromise.valid()) {
    stalePromise.setValue();
  }
  return data;
}

UcxExchangeClient::~UcxExchangeClient() {
  close();
}

std::string UcxExchangeClient::toString() const {
  std::stringstream out;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    for (auto& source : sources_) {
      out << source->toString() << std::endl;
    }
  }
  return out.str();
}

folly::dynamic UcxExchangeClient::toJson() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["taskId"] = taskId_;
  folly::dynamic clientsObj = folly::dynamic::object;
  int index = 0;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    obj["closed"] = closed_;
    for (auto& source : sources_) {
      clientsObj[std::to_string(index++)] = source->toJson();
    }
  }
  obj["clients"] = clientsObj;
  return obj;
}

} // namespace facebook::velox::ucx_exchange
