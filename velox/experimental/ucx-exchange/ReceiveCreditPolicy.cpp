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
#include "velox/experimental/ucx-exchange/ReceiveCreditPolicy.h"

#include <algorithm>
#include <limits>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::ucx_exchange {
namespace {

constexpr uint64_t kMaxBytes = std::numeric_limits<uint64_t>::max();

uint64_t saturatingAdd(uint64_t left, uint64_t right) {
  return left > kMaxBytes - right ? kMaxBytes : left + right;
}

uint64_t saturatingMultiply(uint64_t left, uint64_t right) {
  if (left != 0 && right > kMaxBytes / left) {
    return kMaxBytes;
  }
  return left * right;
}

} // namespace

uint64_t ReceiveCreditPolicy::payloadCost(
    uint64_t gpuDataBytes,
    uint64_t metadataBytes) const {
  // A payload without GPU data, as zero-column output produces, still holds
  // its metadata on the host, so it is never free.
  return saturatingAdd(gpuDataBytes, metadataBytes);
}

uint64_t PrefetchPerConsumerPolicy::limitBytes(
    const ReceiveCreditInputs& inputs) const {
  const auto prefetch = saturatingMultiply(
      static_cast<uint64_t>(inputs.numConsumers) + 1,
      inputs.averagePayloadCost);
  return std::max(inputs.configuredBytes, prefetch);
}

uint64_t ConfiguredBytesPolicy::limitBytes(
    const ReceiveCreditInputs& inputs) const {
  return inputs.configuredBytes;
}

std::shared_ptr<const ReceiveCreditPolicy> makeReceiveCreditPolicy(
    std::string_view name) {
  if (name == PrefetchPerConsumerPolicy::kName) {
    return std::make_shared<PrefetchPerConsumerPolicy>();
  }
  if (name == ConfiguredBytesPolicy::kName) {
    return std::make_shared<ConfiguredBytesPolicy>();
  }
  VELOX_USER_FAIL(
      "Unknown UCX receive credit policy. Expected '{}' or '{}', got: {}",
      PrefetchPerConsumerPolicy::kName,
      ConfiguredBytesPolicy::kName,
      name);
}

std::shared_ptr<const ReceiveCreditPolicy> defaultReceiveCreditPolicy() {
  static const auto policy = std::make_shared<PrefetchPerConsumerPolicy>();
  return policy;
}

} // namespace facebook::velox::ucx_exchange
