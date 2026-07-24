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

#include "velox/exec/ExchangeTransportRegistry.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/ExchangeClient.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
namespace {

using ::testing::Key;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class MockExchangeClient : public ExchangeClient {
 public:
  void addRemoteTaskId(const std::string&) override {}
  void noMoreRemoteTasks() override {}
  void close() override {}
  folly::F14FastMap<std::string, RuntimeMetric> stats() override {
    return {};
  }
  folly::dynamic toJson() const override {
    return folly::dynamic::object;
  }
};

std::shared_ptr<ExchangeTransportEntry> makeEntry() {
  return std::make_shared<ExchangeTransportEntry>(ExchangeTransportEntry{
      [](const ExchangeClientContext&) -> std::shared_ptr<ExchangeClient> {
        return std::make_shared<MockExchangeClient>();
      },
      [](int32_t,
         DriverCtx*,
         const std::shared_ptr<const core::ExchangeNode>&,
         std::shared_ptr<ExchangeClient>) -> std::unique_ptr<Operator> {
        return nullptr;
      }});
}

TEST(ExchangeTransportRegistryTest, registryOperations) {
  ExchangeTransportRegistry::unregisterAll();

  const int32_t numTransports = 5;
  for (int32_t i = 0; i < numTransports; ++i) {
    ExchangeTransportRegistry::global().insert(
        fmt::format("t-{}", i), makeEntry());
  }
  for (int32_t i = 0; i < numTransports; ++i) {
    EXPECT_NE(
        ExchangeTransportRegistry::tryGet(fmt::format("t-{}", i)), nullptr);
  }
  EXPECT_EQ(ExchangeTransportRegistry::tryGet("nonexistent"), nullptr);
  // getAll includes the always-available built-in in-memory default.
  EXPECT_THAT(ExchangeTransportRegistry::getAll(), SizeIs(numTransports + 1));

  ExchangeTransportRegistry::unregisterAll();
  EXPECT_THAT(
      ExchangeTransportRegistry::getAll(),
      UnorderedElementsAre(Key(std::string(core::TransportKind::kInMemory))));
}

TEST(ExchangeTransportRegistryTest, builtinInMemoryAlwaysPresent) {
  ExchangeTransportRegistry::unregisterAll();
  EXPECT_NE(
      ExchangeTransportRegistry::tryGet(
          std::string{core::TransportKind::kInMemory}),
      nullptr);
}

class ExchangeTransportRegistryFixture : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    ExchangeTransportRegistry::unregisterAll();
  }

  void TearDown() override {
    ExchangeTransportRegistry::unregisterAll();
  }

  std::shared_ptr<core::QueryCtx> queryCtxWithRegistry(
      std::shared_ptr<ExchangeTransportRegistry::Registry> registry) {
    auto queryCtx = core::QueryCtx::create();
    queryCtx->setRegistry(
        ExchangeTransportRegistry::kRegistryKey, std::move(registry));
    return queryCtx;
  }
};

TEST_F(ExchangeTransportRegistryFixture, queryScopedResolution) {
  auto globalEntry = makeEntry();
  ExchangeTransportRegistry::global().insert("shared", globalEntry);
  ExchangeTransportRegistry::global().insert("global-only", globalEntry);

  EXPECT_EQ(
      ExchangeTransportRegistry::tryGet(*core::QueryCtx::create(), "shared"),
      globalEntry);

  auto queryEntry = makeEntry();
  auto queryRegistry =
      ExchangeTransportRegistry::create(&ExchangeTransportRegistry::global());
  queryRegistry->insert("shared", queryEntry);
  auto queryCtx = queryCtxWithRegistry(queryRegistry);

  EXPECT_EQ(
      ExchangeTransportRegistry::tryGet(*queryCtx, "shared"), queryEntry);
  EXPECT_EQ(
      ExchangeTransportRegistry::tryGet(*queryCtx, "global-only"),
      globalEntry);
  EXPECT_EQ(ExchangeTransportRegistry::tryGet("shared"), globalEntry);
}

TEST_F(ExchangeTransportRegistryFixture, queryScopedUnregisterAll) {
  auto globalEntry = makeEntry();
  ExchangeTransportRegistry::global().insert("exchange", globalEntry);

  auto queryRegistry =
      ExchangeTransportRegistry::create(&ExchangeTransportRegistry::global());
  queryRegistry->insert("exchange", makeEntry());
  auto queryCtx = queryCtxWithRegistry(queryRegistry);

  ExchangeTransportRegistry::unregisterAll(*queryCtx);

  EXPECT_EQ(
      ExchangeTransportRegistry::tryGet(*queryCtx, "exchange"), globalEntry);
  EXPECT_EQ(ExchangeTransportRegistry::tryGet("exchange"), globalEntry);
}

TEST_F(ExchangeTransportRegistryFixture, queryScopedGetAll) {
  auto globalEntry = makeEntry();
  ExchangeTransportRegistry::global().insert("global-only", globalEntry);
  ExchangeTransportRegistry::global().insert("shared", globalEntry);

  auto queryRegistry =
      ExchangeTransportRegistry::create(&ExchangeTransportRegistry::global());
  queryRegistry->insert("query-only", makeEntry());
  queryRegistry->insert("shared", makeEntry());
  auto queryCtx = queryCtxWithRegistry(queryRegistry);

  // getAll() also lists the always-available built-in in-memory default.
  const std::string inMemory{core::TransportKind::kInMemory};
  EXPECT_THAT(
      ExchangeTransportRegistry::getAll(*queryCtx),
      UnorderedElementsAre(
          Key("global-only"), Key("query-only"), Key("shared"), Key(inMemory)));
  EXPECT_THAT(
      ExchangeTransportRegistry::getAll(),
      UnorderedElementsAre(Key("global-only"), Key("shared"), Key(inMemory)));
}

TEST_F(ExchangeTransportRegistryFixture, isolatedQueryHasNoDefault) {
  // Isolation mode (create(nullptr)) has no parent fallback, so not even the
  // built-in default is visible; an isolated query must register every
  // transport it uses.
  auto queryCtx =
      queryCtxWithRegistry(ExchangeTransportRegistry::create(nullptr));

  EXPECT_EQ(
      ExchangeTransportRegistry::tryGet(
          *queryCtx, std::string(core::TransportKind::kInMemory)),
      nullptr);
}

} // namespace
} // namespace facebook::velox::exec
