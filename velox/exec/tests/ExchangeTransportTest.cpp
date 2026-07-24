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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/exec/InMemoryExchangeClient.h"
#include "velox/exec/ExchangeSource.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;

namespace facebook::velox::exec::test {
namespace {

class ExchangeTransportTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    // The in-memory transport's client fetches from producer tasks over the
    // local:// scheme, so register the local exchange source factory.
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(createLocalExchangeSource);
  }

  void TearDown() override {
    ExchangeTransportRegistry::unregisterAll();
    OperatorTestBase::TearDown();
  }

  static exec::Split remoteSplit(const std::string& taskId) {
    return exec::Split(std::make_shared<RemoteConnectorSplit>(taskId));
  }

  // Registers 'transportKind' with an entry pairing an InMemoryExchangeClient
  // factory with an Exchange operator factory, both incrementing an
  // invocation counter so tests can assert the registry entry was used.
  void registerTestTransport(
      const std::string& transportKind,
      std::shared_ptr<std::atomic<int>> clientInvocations,
      std::shared_ptr<std::atomic<int>> operatorInvocations) {
    ExchangeTransportRegistry::global().insert(
        transportKind,
        std::make_shared<ExchangeTransportEntry>(ExchangeTransportEntry{
            [clientInvocations](const ExchangeClientContext& c)
                -> std::shared_ptr<ExchangeClient> {
              ++*clientInvocations;
              return std::make_shared<InMemoryExchangeClient>(
                  c.taskId,
                  c.destination,
                  c.queryConfig.maxExchangeBufferSize(),
                  c.numberOfConsumers,
                  c.queryConfig.minExchangeOutputBatchBytes(),
                  c.pool,
                  c.executor,
                  c.queryConfig.requestDataSizesMaxWaitSec(),
                  c.queryConfig.singleSourceExchangeOptimizationEnabled(),
                  c.queryConfig.exchangeLazyFetchingEnabled());
            },
            [operatorInvocations](
                int32_t operatorId,
                DriverCtx* ctx,
                const std::shared_ptr<const core::ExchangeNode>& node,
                std::shared_ptr<ExchangeClient> client)
                -> std::unique_ptr<Operator> {
              ++*operatorInvocations;
              auto inMemory = std::dynamic_pointer_cast<InMemoryExchangeClient>(
                  std::move(client));
              VELOX_CHECK_NOT_NULL(inMemory);
              return std::make_unique<Exchange>(
                  operatorId, ctx, node, std::move(inMemory));
            }}));
  }

  // Runs a leaf task producing 'data' through a single-partition
  // PartitionedOutput, then an exchange task consuming it via 'transportKind'.
  // Returns the cursor together with the rows collected by the exchange
  // consumer; the caller must keep the cursor alive while using the rows, which
  // borrow its memory.
  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> runExchange(
      const std::string& leafTaskId,
      const RowVectorPtr& data,
      const std::string& transportKind) {
    auto leafPlan = PlanBuilder()
                        .values({data})
                        .partitionedOutput({}, 1, /*outputLayout=*/{})
                        .planFragment();
    auto leafTask = Task::create(
        leafTaskId,
        std::move(leafPlan),
        0,
        core::QueryCtx::create(driverExecutor_.get()),
        Task::ExecutionMode::kParallel,
        exec::Consumer{});
    leafTask->start(1);

    core::PlanNodeId exchangeNodeId;
    CursorParameters params;
    params.planNode =
        PlanBuilder()
            .exchange(asRowType(data->type()), "Presto", transportKind)
            .capturePlanNodeId(exchangeNodeId)
            .planNode();
    params.maxDrivers = 1;
    params.queryCtx = core::QueryCtx::create(driverExecutor_.get());

    auto result = readCursor(params, [&](TaskCursor* taskCursor) {
      taskCursor->task()->addSplit(exchangeNodeId, remoteSplit(leafTaskId));
      taskCursor->task()->noMoreSplits(exchangeNodeId);
      // Signal the cursor's own no-more-splits flag so readCursor's outer
      // split-adding loop terminates after this single pass.
      taskCursor->setNoMoreSplits();
    });

    EXPECT_TRUE(waitForTaskCompletion(leafTask.get(), 3'000'000))
        << leafTask->taskId();
    EXPECT_TRUE(waitForTaskCompletion(result.first->task().get(), 3'000'000))
        << result.first->task()->taskId();
    return result;
  }
};

TEST_F(ExchangeTransportTest, selectsOperatorByTransportKind) {
  const std::string transportKind{"test-exchange-transport"};
  auto clientInvocations = std::make_shared<std::atomic<int>>(0);
  auto operatorInvocations = std::make_shared<std::atomic<int>>(0);
  registerTestTransport(transportKind, clientInvocations, operatorInvocations);

  auto data = makeRowVector({"c0"}, {makeFlatVector<int64_t>(
                     100, [](vector_size_t row) { return row; })});
  auto [cursor, results] =
      runExchange("local://selects-operator-leaf", data, transportKind);

  EXPECT_GT(clientInvocations->load(), 0);
  EXPECT_GT(operatorInvocations->load(), 0);
  EXPECT_TRUE(assertEqualResults({data}, results));
}

TEST_F(ExchangeTransportTest, usesInMemoryByDefault) {
  auto data = makeRowVector({"c0"}, {makeFlatVector<int64_t>(
                     100, [](vector_size_t row) { return row; })});
  auto [cursor, results] = runExchange(
      "local://uses-in-memory-leaf",
      data,
      std::string{core::TransportKind::kInMemory});

  EXPECT_TRUE(assertEqualResults({data}, results));
}

TEST_F(ExchangeTransportTest, errorsOnUnregisteredTransport) {
  const std::string transportKind{"unregistered-exchange-transport"};
  EXPECT_EQ(ExchangeTransportRegistry::tryGet(transportKind), nullptr);

  auto plan = PlanBuilder()
                  .exchange(ROW({"c0"}, {BIGINT()}), "Presto", transportKind)
                  .planNode();
  auto queryCtx = core::QueryCtx::create(driverExecutor_.get());
  auto task = Task::create(
      "local://errors-on-unregistered-transport",
      core::PlanFragment{plan},
      0,
      queryCtx,
      Task::ExecutionMode::kParallel,
      exec::Consumer{});
  VELOX_ASSERT_THROW(
      task->start(1, 1), "No exchange transport registered for transport");
}

TEST_F(ExchangeTransportTest, usesDefaultAfterRegistryClear) {
  // unregisterAll() resets the registry to its baseline -- the built-in
  // in-memory default -- so a default-transport exchange still runs after a
  // clear.
  ExchangeTransportRegistry::unregisterAll();

  auto data = makeRowVector({"c0"}, {makeFlatVector<int64_t>(
                     100, [](vector_size_t row) { return row; })});
  auto [cursor, results] = runExchange(
      "local://uses-default-after-clear-leaf",
      data,
      std::string{core::TransportKind::kInMemory});

  EXPECT_TRUE(assertEqualResults({data}, results));
}

} // namespace
} // namespace facebook::velox::exec::test
