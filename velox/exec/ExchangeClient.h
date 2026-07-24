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

#include <folly/container/F14Map.h>
#include <folly/dynamic.h>
#include <string>

#include "velox/common/base/RuntimeMetrics.h"

namespace facebook::velox::exec {

/// Control-plane handle for the set of upstream producers feeding one exchange
/// plan node. One instance per ExchangeNode, owned by Task and shared across
/// every driver thread running that node's exchange operator. Concrete
/// implementations (InMemoryExchangeClient, UcxExchangeClient) add the
/// transport-specific data plane (fetch / queue) and are paired with their
/// operator in ExchangeTransportRegistry.
///
/// Thread safety: one instance is driven concurrently from all of a node's
/// driver threads, so every method must be safe under concurrent calls.
///
/// Lifecycle: addRemoteTaskId() any number of times, then noMoreRemoteTasks()
/// once no more upstream tasks will arrive; close() exactly once at teardown
/// (idempotent). Completion is defined by noMoreRemoteTasks() + drained data,
/// surfaced through the concrete data plane, not by this interface.
class ExchangeClient {
 public:
  virtual ~ExchangeClient() = default;

  /// Starts fetching from upstream task 'remoteTaskId'. Idempotent: repeated
  /// calls with the same id are ignored. Safe to call after close() (the source
  /// is created and immediately closed to notify the producer).
  virtual void addRemoteTaskId(const std::string& remoteTaskId) = 0;

  /// Signals that no further addRemoteTaskId() calls will occur.
  virtual void noMoreRemoteTasks() = 0;

  /// Closes the client and its sources. Idempotent.
  virtual void close() = 0;

  /// Runtime statistics aggregated across sources, as a transport-neutral map.
  /// Implementations report background CPU time under
  /// Operator::kBackgroundCpuTimeNanos.
  virtual folly::F14FastMap<std::string, RuntimeMetric> stats() = 0;

  /// Human-readable JSON dump of the client's state. Consumed by Task::toJson()
  /// for debugging; the shape is implementation-defined.
  virtual folly::dynamic toJson() const = 0;
};

} // namespace facebook::velox::exec
