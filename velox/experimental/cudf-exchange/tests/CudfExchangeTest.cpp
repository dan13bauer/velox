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
#include <cudf/column/column_factories.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <folly/Executor.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <rmm/device_buffer.hpp>
#include <memory>
#include <vector>
#include "CudfTestHelpers.h"
#include "folly/experimental/EventCount.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/cudf-exchange/Communicator.h"
#include "velox/experimental/cudf-exchange/CudfOutputQueueManager.h"
#include "velox/experimental/cudf-exchange/tests/CudfPartitionedOutputMock.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestData.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestHelpers.h"
#include "velox/experimental/cudf-exchange/tests/SinkDriverMock.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace facebook::velox::cudf_exchange {

class CudfExchangeTest : public testing::TestWithParam<int> {
 protected:
  static constexpr uint16_t kCommunicatorPort = 21345;
  static constexpr auto kUnusedCoordinatorUrl =
      std::string_view("http://localhost:12345/bla");

  static std::shared_ptr<CudfOutputQueueManager> queueManager_;
  static std::shared_ptr<std::thread> communicatorThread_;
  static std::shared_ptr<Communicator> communicator_;

  static void SetUpTestCase() {
    VLOG(0) << "setup test case, creating queue manager, communicator, etc..";
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    queueManager_ = CudfOutputQueueManager::getInstanceRef();
    ContinueFuture future;
    communicator_ = facebook::velox::cudf_exchange::Communicator::initAndGet(
        kCommunicatorPort, std::string(kUnusedCoordinatorUrl), &future);
    if (communicator_) {
      communicatorThread_ = std::make_shared<std::thread>(
          &facebook::velox::cudf_exchange::Communicator::run,
          communicator_.get());
    } else {
      ADD_FAILURE() << "Communicator initialization failed";
    }
    future.wait();
  }

  static void TearDownTestCase() {
    communicator_->stop();
    communicator_.reset();
    communicatorThread_->join();
    communicatorThread_.reset();
  }

  void SetUp() override {
    VLOG(0) << "creating pool";
    pool_ = facebook::velox::memory::memoryManager()->addLeafPool(
        "CudfTestMemoryPool");
  }

  exec::Split remoteSplit(const std::string& taskId, int partitionId) {
    std::string remoteUrl =
        "http://127.0.0.1:" + std::to_string(kCommunicatorPort - 3) +
        "/v1/task/" + taskId + "/results/" + std::to_string(partitionId);
    return exec::Split(
        std::make_shared<facebook::velox::exec::RemoteConnectorSplit>(
            remoteUrl));
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

INSTANTIATE_TEST_SUITE_P(
    CudfExchangeTest,
    CudfExchangeTest,
    ::testing::Values(1, 2, 10));

TEST_P(CudfExchangeTest, basicTest) {
  VLOG(3) << "+ CudfExchangeTest::basicTest";
  size_t numPartitions = 1;
  int numDrivers = GetParam();
  uint32_t numChunks = 10;
  size_t numRowsPerChunk = 1000;

  // Basic test implementation
  const std::string srcTaskId = "sourceTask";
  auto srcTask = createSourceTask(srcTaskId, pool_, CudfTestData::kTestRowType);

  // tell the queue manager that a new source task with one driver
  // and one partition exists.
  queueManager_->initializeTask(srcTask, numPartitions, numDrivers);

  // Mock the CudfPartitionedOutput operator, it will produce numChunks of data
  // each containing numRowsPerChunk of empty data

  auto sourceMock = std::make_shared<CudfPartitionedOutputMock>(
      srcTaskId, numDrivers, numPartitions, numChunks, numRowsPerChunk);

  const std::string sinkTaskId = "sinkTask";
  int partitionId = 0;
  core::PlanNodeId exchangeNodeId;
  auto sinkTask = createExchangeTask(
      sinkTaskId, CudfTestData::kTestRowType, partitionId, exchangeNodeId);

  int driverId = 0;
  SinkDriverMock sinkDriver(sinkTask, driverId);

  // create a remote split and add it to the sink driver mock.
  std::vector<facebook::velox::exec::Split> splits;
  splits.emplace_back(remoteSplit(srcTaskId, partitionId));
  sinkDriver.addSplits(splits);

  // Start the mocks.
  VLOG(3) << "Starting source task";
  sourceMock->run();
  VLOG(3) << "Starting sink task";
  std::thread sinkThread(&SinkDriverMock::run, &sinkDriver);

  sourceMock->joinThreads();
  VLOG(3) << "Source task done.";
  sinkThread.join();
  VLOG(3) << "Sink task done.";

  size_t expectedRows = numChunks * numRowsPerChunk * numDrivers;
  size_t effectiveRows = sinkDriver.numRows();

  GTEST_ASSERT_EQ(expectedRows, effectiveRows);

  // Remove the srcTask from the queue manager, so queue get freed
  queueManager_->removeTask(srcTaskId);

  VLOG(3) << "- CudfExchangeTest::basicTest";
}

TEST_P(CudfExchangeTest, dataTest) {
  VLOG(3) << "+ CudfExchangeTest::dataTest";

 
  int numDrivers = GetParam();
  size_t numRowsPerChunk = 1000 * 1000 * 100;
  uint32_t numChunks = 1;
  int strLength = 5;

  std::shared_ptr<CudfTestData> data = std::make_shared<CudfTestData>();
  data->initialize(numRowsPerChunk, strLength, strLength * 2);
  auto strings = data->getStrings();
  auto integers = data->getIntegers();
  auto doubles = data->getDoubles();

  for (int i = 0; i < numRowsPerChunk; i++) {
    VLOG(3) << "String: " << strings->at(i) << " Integer: " << integers->at(i)
            << " Double: " << doubles->at(i);
  }

  // Basic test implementation
  const std::string srcTaskId = "sourceTask";
  auto srcTask = createSourceTask(srcTaskId, pool_, CudfTestData::kTestRowType);

  // tell the queue manager that a new source task with one driver
  // and one partition exists.
  queueManager_->initializeTask(srcTask, numPartitions, numDrivers);

  // Mock the CudfPartitionedOutput operator, it will produce numChunks of data
  // each containing numRowsPerChunk of data copied from the CudfTestData object
  // data

  auto sourceMock = std::make_shared<CudfPartitionedOutputMock>(
      srcTaskId, numDrivers, numPartitions, numChunks, numRowsPerChunk, data);

  const std::string sinkTaskId = "sinkTask";
  int partitionId = 0;
  core::PlanNodeId exchangeNodeId;
  auto sinkTask = createExchangeTask(
      sinkTaskId, CudfTestData::kTestRowType, partitionId, exchangeNodeId);

  int driverId = 0;
  SinkDriverMock sinkDriver(sinkTask, driverId, nullptr, 1, data);

  // create a remote split and add it to the sink driver mock.
  std::vector<facebook::velox::exec::Split> splits;
  splits.emplace_back(remoteSplit(srcTaskId, partitionId));
  sinkDriver.addSplits(splits);

  // Start the mocks.
  VLOG(3) << "Starting source task";
  sourceMock->run();
  VLOG(3) << "Starting sink task";
  std::thread sinkThread(&SinkDriverMock::run, &sinkDriver);

  sourceMock->joinThreads();
  VLOG(3) << "Source task done.";
  sinkThread.join();
  VLOG(3) << "Sink task done.";

  // Remove the srcTask from the queue manager, so queue get freed
  queueManager_->removeTask(srcTaskId);

  VLOG(3) << "- CudfExchangeTest::basicTest";
  GTEST_ASSERT_EQ(sinkDriver.dataIsValid(), true);
}

TEST_P(CudfExchangeTest, multiUpstreamTasksTest) {
  VLOG(3) << "+ CudfExchangeTest::multiUpstreamTasksTest";
  size_t numPartitions = 1;
  int numDrivers = GetParam();
  int numUpstreamTasks = 10;
  uint32_t numChunks = 10;
  size_t numRowsPerChunk = 1000;

  std::vector<std::shared_ptr<CudfPartitionedOutputMock>> sourceMocks;

  // Create n upstream tasks.
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    auto srcTask =
        createSourceTask(srcTaskId, pool_, CudfTestData::kTestRowType);

    // tell the queue manager that a new source task with one driver
    // and one partition exists.
    queueManager_->initializeTask(srcTask, numPartitions, numDrivers);

    sourceMocks.emplace_back(std::make_shared<CudfPartitionedOutputMock>(
        srcTaskId, numDrivers, numPartitions, numChunks, numRowsPerChunk));
  }

  const std::string sinkTaskId = "sinkTask";
  int partitionId = 0;
  core::PlanNodeId exchangeNodeId;
  auto sinkTask = createExchangeTask(
      sinkTaskId, CudfTestData::kTestRowType, partitionId, exchangeNodeId);

  int driverId = 0;
  SinkDriverMock sinkDriver(sinkTask, driverId);

  // create n remote splits and add it to the sink driver mock.
  std::vector<facebook::velox::exec::Split> splits;
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    splits.emplace_back(remoteSplit(srcTaskId, partitionId));
  }
  sinkDriver.addSplits(splits);

  // Start the mocks.
  VLOG(3) << "Starting source tasks";
  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->run();
  }
  VLOG(3) << "Starting sink task";
  std::thread sinkThread(&SinkDriverMock::run, &sinkDriver);

  for (int i = 0; i < numUpstreamTasks; i++) {
    sourceMocks[i]->joinThreads();
  }
  VLOG(3) << "Source tasks done.";
  sinkThread.join();
  VLOG(3) << "Sink task done.";

  size_t expectedRows =
      numChunks * numRowsPerChunk * numUpstreamTasks * numDrivers;
  size_t effectiveRows = sinkDriver.numRows();

  GTEST_ASSERT_EQ(expectedRows, effectiveRows);

  // Remove the srcTasks from the queue manager, so queue get freed
  for (int i = 0; i < numUpstreamTasks; i++) {
    const std::string srcTaskId = "sourceTask" + std::to_string(i);
    queueManager_->removeTask(srcTaskId);
  }

  VLOG(3) << "- CudfExchangeTest::multiUpstreamTasksTest";
}

std::shared_ptr<CudfOutputQueueManager> CudfExchangeTest::queueManager_;
std::shared_ptr<std::thread> CudfExchangeTest::communicatorThread_;
std::shared_ptr<Communicator> CudfExchangeTest::communicator_;

} // namespace facebook::velox::cudf_exchange
