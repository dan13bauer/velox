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

#include <gtest/gtest.h>

#include <limits>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::ucx_exchange {
namespace {

constexpr uint64_t kMiB = 1ULL << 20;
constexpr uint64_t kGiB = 1ULL << 30;
constexpr uint64_t kMaxBytes = std::numeric_limits<uint64_t>::max();

TEST(ReceiveCreditPolicyTest, payloadCostAddsMetadata) {
  const PrefetchPerConsumerPolicy policy;
  EXPECT_EQ(policy.payloadCost(4 * kMiB, 512), 4 * kMiB + 512);

  // A zero-column payload still costs its metadata.
  EXPECT_EQ(policy.payloadCost(0, 512), 512);

  EXPECT_EQ(policy.payloadCost(kMaxBytes, 1), kMaxBytes);
}

TEST(ReceiveCreditPolicyTest, prefetchPerConsumer) {
  const PrefetchPerConsumerPolicy policy;

  // Large payloads: one per consumer driver, plus one.
  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 32 * kMiB,
           .numConsumers = 1,
           .averagePayloadCost = 2 * kGiB}),
      4 * kGiB);
  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 32 * kMiB,
           .numConsumers = 8,
           .averagePayloadCost = 2 * kGiB}),
      18 * kGiB);

  // Small payloads: the configured bytes are the floor.
  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 32 * kMiB,
           .numConsumers = 1,
           .averagePayloadCost = 4 * kMiB}),
      32 * kMiB);

  // Before the first payload, the average is zero.
  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 32 * kMiB,
           .numConsumers = 4,
           .averagePayloadCost = 0}),
      32 * kMiB);

  // Every consumer driver has closed: one payload is still allowed.
  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 32 * kMiB,
           .numConsumers = 0,
           .averagePayloadCost = 2 * kGiB}),
      2 * kGiB);

  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 0,
           .numConsumers = 1,
           .averagePayloadCost = kMaxBytes}),
      kMaxBytes);
}

TEST(ReceiveCreditPolicyTest, configuredBytes) {
  const ConfiguredBytesPolicy policy;
  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 32 * kMiB,
           .numConsumers = 8,
           .averagePayloadCost = 2 * kGiB}),
      32 * kMiB);
  EXPECT_EQ(
      policy.limitBytes(
          {.configuredBytes = 8 * kGiB,
           .numConsumers = 1,
           .averagePayloadCost = 2 * kGiB}),
      8 * kGiB);
}

TEST(ReceiveCreditPolicyTest, makeByName) {
  EXPECT_EQ(
      makeReceiveCreditPolicy("prefetch_per_consumer")->name(),
      PrefetchPerConsumerPolicy::kName);
  EXPECT_EQ(
      makeReceiveCreditPolicy("configured_bytes")->name(),
      ConfiguredBytesPolicy::kName);
  EXPECT_EQ(
      defaultReceiveCreditPolicy()->name(), PrefetchPerConsumerPolicy::kName);
  VELOX_ASSERT_USER_THROW(
      makeReceiveCreditPolicy("count"),
      "Unknown UCX receive credit policy. Expected 'prefetch_per_consumer' or "
      "'configured_bytes', got: count");
}

} // namespace
} // namespace facebook::velox::ucx_exchange
