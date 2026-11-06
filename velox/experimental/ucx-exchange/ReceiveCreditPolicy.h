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
#include <memory>
#include <string_view>

namespace facebook::velox::ucx_exchange {

/// Inputs to the receive credit limit of one UcxExchangeClient. The exchange
/// queue samples them under its mutex and passes them to a
/// ReceiveCreditPolicy.
struct ReceiveCreditInputs {
  /// exchange.max_buffer_size for this client.
  uint64_t configuredBytes{0};

  /// Consumer drivers of this client that have not closed.
  uint32_t numConsumers{0};

  /// Running average cost of the payloads received so far. Zero before the
  /// first payload.
  uint64_t averagePayloadCost{0};
};

/// Decides how many bytes a UcxExchangeClient may hold in payloads that are
/// being received or wait for a consumer driver, and what each payload costs
/// against that limit. Implementations are pure functions of their inputs:
/// they take no locks and keep no state, so the formula can be changed in one
/// place and tested without a GPU.
class ReceiveCreditPolicy {
 public:
  virtual ~ReceiveCreditPolicy() = default;

  /// Returns the limit in bytes. The exchange queue admits one payload
  /// whenever nothing is reserved or queued, whatever this returns, so a
  /// payload larger than the limit is never refused forever.
  virtual uint64_t limitBytes(const ReceiveCreditInputs& inputs) const = 0;

  /// Returns the bytes one payload costs against the limit.
  /// @param gpuDataBytes Size of the payload's packed GPU buffer.
  /// @param metadataBytes Size of the payload's serialized cuDF metadata.
  virtual uint64_t payloadCost(uint64_t gpuDataBytes, uint64_t metadataBytes)
      const;

  /// Returns the policy's name, as used by makeReceiveCreditPolicy().
  virtual std::string_view name() const = 0;
};

/// Limit of max(configured bytes, (consumer drivers + 1) * average payload
/// cost): one payload for every consumer driver and one more on the way, but
/// never less than the configured bytes.
class PrefetchPerConsumerPolicy : public ReceiveCreditPolicy {
 public:
  static constexpr std::string_view kName{"prefetch_per_consumer"};

  uint64_t limitBytes(const ReceiveCreditInputs& inputs) const override;

  std::string_view name() const override {
    return kName;
  }
};

/// Limit of exactly the configured bytes, so that exchange.max_buffer_size is
/// a maximum. With payloads larger than the configured bytes, this admits one
/// payload at a time.
class ConfiguredBytesPolicy : public ReceiveCreditPolicy {
 public:
  static constexpr std::string_view kName{"configured_bytes"};

  uint64_t limitBytes(const ReceiveCreditInputs& inputs) const override;

  std::string_view name() const override {
    return kName;
  }
};

/// Returns the policy registered under 'name'. Throws a user error for an
/// unknown name.
std::shared_ptr<const ReceiveCreditPolicy> makeReceiveCreditPolicy(
    std::string_view name);

/// Returns the policy used when none is configured.
std::shared_ptr<const ReceiveCreditPolicy> defaultReceiveCreditPolicy();

} // namespace facebook::velox::ucx_exchange
