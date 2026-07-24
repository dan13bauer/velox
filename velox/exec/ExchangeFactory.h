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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace facebook::velox::memory {
class MemoryPool;
}
namespace facebook::velox::core {
class ExchangeNode;
class QueryConfig;
} // namespace facebook::velox::core
namespace folly {
class Executor;
}

namespace facebook::velox::exec {

struct DriverCtx;
class Operator;
class ExchangeClient;

/// Task-supplied context for building a per-node exchange client. Transport
/// implementations read only the fields they need. The caller sizes the
/// exchange buffer: the plain-exchange path sets 'maxExchangeBufferSize' /
/// 'minExchangeOutputBatchBytes' from 'queryConfig', while the merge path sets
/// them to its per-source budget and zero (deliver immediately).
struct ExchangeClientContext {
  std::string taskId;
  int destination;
  int32_t numberOfConsumers;
  /// Total bytes the client may buffer before applying backpressure.
  int64_t maxExchangeBufferSize;
  /// Minimum accumulated bytes before a batch is delivered; zero delivers each
  /// page immediately.
  uint64_t minExchangeOutputBatchBytes;
  memory::MemoryPool* pool;
  folly::Executor* executor;
  const core::QueryConfig& queryConfig;
};

/// Builds the per-node exchange client. Task calls this once per ExchangeNode
/// and owns the result (shared across drivers).
using ExchangeClientFactory =
    std::function<std::shared_ptr<ExchangeClient>(const ExchangeClientContext&)>;

/// Builds the exchange operator for a node, bound to the client Task created.
/// The two are registered together in ExchangeTransportRegistry so they cannot
/// diverge; the operator downcasts 'client' to its concrete type.
using ExchangeFactory = std::function<std::unique_ptr<Operator>(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::ExchangeNode>& node,
    std::shared_ptr<ExchangeClient> client)>;

} // namespace facebook::velox::exec
