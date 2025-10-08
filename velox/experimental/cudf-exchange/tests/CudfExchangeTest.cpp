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
#include "velox/experimental/cudf-exchange/tests/CudfTestHelpers.h"
#include "velox/experimental/cudf-exchange/tests/SinkDriverMock.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace facebook::velox::cudf_exchange {

class CudfExchangeTest : public testing::Test {
 protected:
  static constexpr uint16_t kCommunicatorPort = 21345;
  static constexpr auto kUnusedCoordinatorUrl =
      std::string_view("http://localhost:12345/bla");

  static void SetUpTestCase() {
    VLOG(0) << "setup test case.";
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    VLOG(0) << "setup, creating pool, communicator, etc..";
    queueManager_ = CudfOutputQueueManager::getInstanceRef();
    pool_ = facebook::velox::memory::memoryManager()->addLeafPool();
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

  void TearDown() override {
    VLOG(0) << "Teardown, destroying everything.";
    communicator_->stop();
    communicator_.reset();
    communicatorThread_->join();
    communicatorThread_.reset();
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

  std::shared_ptr<CudfOutputQueueManager> queueManager_;
  std::shared_ptr<std::thread> communicatorThread_;
  std::shared_ptr<Communicator> communicator_;
};

TEST_F(CudfExchangeTest, basicTest) {
  VLOG(3) << "Basic test.";
  size_t numPartitons = 1;
  int numDrivers = 1;
  // Basic test implementation
  const std::string srcTaskId = "sourceTask";
  auto srcTask = createSourceTask(srcTaskId, pool_, kTestRowType);

  // tell the queue manager that a new source task with one driver
  // and one partition exists.
  queueManager_->initializeTask(srcTask, numPartitons, numDrivers);

  uint32_t numChunks = 10;
  size_t numRowsPerChunk = 1000;
  auto sourceMock = std::make_shared<CudfPartitionedOutputMock>(
      srcTaskId, numDrivers, numChunks, numRowsPerChunk);

  const std::string sinkTaskId = "sinkTask";
  int partitionId = 0;
  core::PlanNodeId exchangeNodeId;
  auto sinkTask =
      createExchangeTask(sinkTaskId, kTestRowType, partitionId, exchangeNodeId);

  int driverId = 0;
  SinkDriverMock sinkDriver(sinkTask, driverId);

  // create a remote split and add it to the sink driver mock.
  std::vector<facebook::velox::exec::Split> splits;
  splits.emplace_back(remoteSplit(srcTaskId, partitionId));
  sinkDriver.addSplits(splits);

  // Start the mocks.
  VLOG(3) << "Starting source task";
  std::thread sourceThread(&CudfPartitionedOutputMock::run, sourceMock.get());
  VLOG(3) << "Starting sink task";
  std::thread sinkThread(&SinkDriverMock::run, &sinkDriver);

  sourceThread.join();
  VLOG(3) << "Source task done.";
  sinkThread.join();
  VLOG(3) << "Sink task done.";

  size_t expectedRows = numChunks * numRowsPerChunk;
  size_t effectiveRows = sinkDriver.numRows();

  GTEST_ASSERT_EQ(expectedRows, effectiveRows);
}

// Add more test cases as needed
// TEST_F(CudfExchangeTest, yourTestName) {
//   // Your test implementation
// }

} // namespace facebook::velox::cudf_exchange
