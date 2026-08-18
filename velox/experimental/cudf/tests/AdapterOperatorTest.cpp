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
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/OperatorAdapters.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/tests/CudfFunctionBaseTest.h"

#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/exec/OutputTransportRegistry.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/VectorStream.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class AdapterOperatorTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    cudf_velox::CudfConfig::getInstance().allowCpuFallback = false;
    // Reset here as well as in TearDown so a case that leaves this set cannot
    // change what a later case in the same binary sees.
    cudf_velox::CudfConfig::getInstance().exchange = false;
    cudf_velox::registerCudf();
  }

  void TearDown() override {
    cudf_velox::CudfConfig::getInstance().exchange = false;
    cudf_velox::unregisterCudf();
    OperatorTestBase::TearDown();
  }
};

TEST_F(AdapterOperatorTest, adapterStatsMergedIntoPlanNode) {
  auto data = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3, 4, 5})});

  core::PlanNodeId projNodeId;
  auto plan = PlanBuilder()
                  .values({data})
                  .project({"c0 * 2 as x"})
                  .capturePlanNodeId(projNodeId)
                  .planNode();

  std::shared_ptr<exec::Task> task;
  AssertQueryBuilder(plan).copyResults(pool(), task);

  auto stats = toPlanStats(task->taskStats());
  auto& projStats = stats.at(projNodeId);

  EXPECT_TRUE(projStats.isMultiOperatorTypeNode());
  EXPECT_TRUE(projStats.operatorStats.count("CudfToVelox"));
}

namespace {
// Returns the registered adapter with 'name', or nullptr.
const cudf_velox::OperatorAdapter* findAdapterByName(const std::string& name) {
  for (const auto& adapter :
       cudf_velox::OperatorAdapterRegistry::getInstance().getAdapters()) {
    if (adapter->name() == name) {
      return adapter.get();
    }
  }
  return nullptr;
}
} // namespace

TEST_F(AdapterOperatorTest, mergeExchangeAdapterSelectsUcxOnly) {
  auto rowType = ROW({"c0"}, {BIGINT()});

  auto ucxNode =
      PlanBuilder()
          .mergeExchange(
              rowType,
              {"c0"},
              std::string(
                  VectorSerde::kindName(VectorSerde::Kind::kCompactRow)),
              std::string{core::TransportKind::kUcx})
          .planNode();
  auto inMemoryNode =
      PlanBuilder()
          .mergeExchange(
              rowType,
              {"c0"},
              std::string(
                  VectorSerde::kindName(VectorSerde::Kind::kCompactRow)),
              std::string{core::TransportKind::kInMemory})
          .planNode();

  const auto* adapter = findAdapterByName("MergeExchange");
  ASSERT_NE(adapter, nullptr) << "MergeExchangeAdapter is not registered";

  // The replacement pair is a GPU source: it takes no input, and its output is
  // device-resident. producesGpuOutput is load-bearing -- the driver adapter
  // appends a CudfToVelox off it, without which a host consumer downstream
  // would receive CudfVectors.
  EXPECT_FALSE(adapter->acceptsGpuInput());
  EXPECT_TRUE(adapter->producesGpuOutput());

  // canRunOnGPU ignores its operator and DriverCtx arguments, so this exercises
  // the transport gate without building a Driver or moving any data.
  cudf_velox::CudfConfig::getInstance().exchange = true;
  EXPECT_TRUE(adapter->canRunOnGPU(nullptr, ucxNode, nullptr));
  EXPECT_FALSE(adapter->canRunOnGPU(nullptr, inMemoryNode, nullptr));

  // With cuDF exchange off, even a kUcx node keeps the CPU MergeExchange.
  cudf_velox::CudfConfig::getInstance().exchange = false;
  EXPECT_FALSE(adapter->canRunOnGPU(nullptr, ucxNode, nullptr));
}

// The three tests below cover transportOperatorProperties(), which classifies
// the operators the transport registry builds -- UcxExchange and
// UcxPartitionedOutput. Those have no OperatorAdapter: Task and LocalPlanner
// construct them from the node's transport, so the driver adapter never sees an
// adapter for them and has only the plan node to go on.
TEST_F(AdapterOperatorTest, ucxExchangeNodeIsAGpuSource) {
  auto rowType = ROW({"c0"}, {BIGINT()});
  const std::string serde{
      VectorSerde::kindName(VectorSerde::Kind::kCompactRow)};

  auto ucxNode =
      PlanBuilder()
          .exchange(rowType, serde, std::string{core::TransportKind::kUcx})
          .planNode();

  auto properties = cudf_velox::transportOperatorProperties(ucxNode);
  ASSERT_TRUE(properties.has_value());
  EXPECT_TRUE(properties->canRunOnGPU);
  // A source consumes no input, and hands the next operator device-resident
  // data. producesGpuOutput is what stops the driver adapter inserting a
  // CudfFromVelox behind UcxExchange, which would convert a CudfVector as
  // though it were host data.
  EXPECT_FALSE(properties->acceptsGpuInput);
  EXPECT_TRUE(properties->producesGpuOutput);

  auto inMemoryNode =
      PlanBuilder()
          .exchange(rowType, serde, std::string{core::TransportKind::kInMemory})
          .planNode();
  EXPECT_FALSE(
      cudf_velox::transportOperatorProperties(inMemoryNode).has_value());
}

TEST_F(AdapterOperatorTest, ucxPartitionedOutputNodeIsAGpuSink) {
  auto data = makeRowVector({"c0"}, {makeFlatVector<int64_t>({1, 2, 3})});

  // outputLayout is spelled with an explicit vector: a braced {} would also
  // match the bool replicateNullsAndAny overload, which would bind the
  // transport argument to serdeKind and silently leave the node in-memory.
  auto ucxNode = PlanBuilder()
                     .values({data})
                     .partitionedOutput(
                         {},
                         1,
                         std::vector<std::string>{},
                         "Presto",
                         std::string{core::TransportKind::kUcx})
                     .planNode();

  auto properties = cudf_velox::transportOperatorProperties(ucxNode);
  ASSERT_TRUE(properties.has_value());
  EXPECT_TRUE(properties->canRunOnGPU);
  // A sink produces nothing downstream, so producesGpuOutput must stay false --
  // true would make the driver adapter append a CudfToVelox after the last
  // operator of the task. acceptsGpuInput is what stops it inserting a
  // CudfToVelox *before* UcxPartitionedOutput, which is what makes the operator
  // fail its CudfVector check on a GPU pipeline.
  EXPECT_TRUE(properties->acceptsGpuInput);
  EXPECT_FALSE(properties->producesGpuOutput);

  auto inMemoryNode = PlanBuilder()
                          .values({data})
                          .partitionedOutput(
                              {},
                              1,
                              std::vector<std::string>{},
                              "Presto",
                              std::string{core::TransportKind::kInMemory})
                          .planNode();
  EXPECT_FALSE(
      cudf_velox::transportOperatorProperties(inMemoryNode).has_value());
}

TEST_F(AdapterOperatorTest, mergeExchangeNodeIsLeftToItsAdapter) {
  // MergeExchangeNode derives from ExchangeNode, so a plain "is this an
  // ExchangeNode named kUcx" test would claim it. It must not: LocalPlanner
  // builds a CPU exec::MergeExchange for this node, and MergeExchangeAdapter
  // replaces it with UcxExchange + CudfOrderBy. Reporting the CPU operator as a
  // GPU source would misplace the conversions in the one exchange pipeline that
  // works today.
  auto ucxNode =
      PlanBuilder()
          .mergeExchange(
              ROW({"c0"}, {BIGINT()}),
              {"c0"},
              std::string(
                  VectorSerde::kindName(VectorSerde::Kind::kCompactRow)),
              std::string{core::TransportKind::kUcx})
          .planNode();

  EXPECT_FALSE(cudf_velox::transportOperatorProperties(ucxNode).has_value());
}

TEST_F(AdapterOperatorTest, ucxTransportsNotRegisteredWhenExchangeDisabled) {
  // SetUp() ran registerCudf() with exchange left at its default (false):
  // no kUcx transport is registered, while the built-in in-memory transport
  // still resolves -- this is the pure-HTTP default the gate must preserve.
  EXPECT_EQ(
      ExchangeTransportRegistry::tryGet(std::string{core::TransportKind::kUcx}),
      nullptr);
  EXPECT_EQ(
      OutputTransportRegistry::tryGet(std::string{core::TransportKind::kUcx}),
      nullptr);
  EXPECT_NE(
      ExchangeTransportRegistry::tryGet(
          std::string{core::TransportKind::kInMemory}),
      nullptr);
}

TEST_F(AdapterOperatorTest, ucxTransportsRegisteredWhenExchangeEnabled) {
  // registerCudf() early-returns once already registered, so unregister
  // first to force the registration body -- and the gated
  // registerUcxExchange() call inside it -- to run again with exchange
  // enabled. Going through initialize(), rather than assigning the exchange
  // field directly, exercises the session-config-key-in to
  // registered-transport-out path end to end.
  cudf_velox::unregisterCudf();
  cudf_velox::CudfConfig::getInstance().initialize(
      {{cudf_velox::CudfConfig::kUcxExchange, "true"}});
  cudf_velox::registerCudf();
  EXPECT_NE(
      ExchangeTransportRegistry::tryGet(std::string{core::TransportKind::kUcx}),
      nullptr);
  EXPECT_NE(
      OutputTransportRegistry::tryGet(std::string{core::TransportKind::kUcx}),
      nullptr);
}
