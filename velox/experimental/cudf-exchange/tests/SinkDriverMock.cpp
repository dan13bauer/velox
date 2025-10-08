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
#include "velox/experimental/cudf-exchange/tests/SinkDriverMock.h"

namespace facebook::velox::cudf_exchange {

constexpr int kPipelineId = 0;
constexpr uint32_t kPartitionId = 0; // not used in tests.

SinkDriverMock::SinkDriverMock(
    std::shared_ptr<facebook::velox::exec::Task> task,
    int driverId,
    std::shared_ptr<ExchangeClientFacade> exchangeClient,
    int32_t numberOfConsumers)
    : task_{std::move(task)},
      driverCtx_{task_, driverId, kPipelineId, kUngroupedGroupId, kPartitionId},
      numRows_{0} {
  if (!exchangeClient) {
    VELOX_CHECK(
        driverId == 0,
        "Only driverId 0 is allowed to create a sink driver without an exchange client");
    // create a new exchange client facade. Since this test doesn't use
    // HTTP exchange, the facade will only use a cudf exchange client.
    // create new cudfExchangeClient
    auto cudfClient = std::make_shared<CudfExchangeClient>(
        task_->taskId(), task_->destination(), numberOfConsumers);
    exchangeClient_ = std::make_shared<ExchangeClientFacade>(
        std::move(cudfClient), nullptr); // no HTTP client.
  } else {
    exchangeClient_ = exchangeClient;
  }
  uint32_t operatorId = 0;
  auto planNode = task_->planFragment().planNode;
  hybridExchange_ = std::make_unique<HybridExchange>(
      operatorId, &driverCtx_, planNode, exchangeClient_);
}

void SinkDriverMock::run() {
  while (true) {
    ContinueFuture future;
    auto blocked = hybridExchange_->isBlocked(&future);
    if (blocked != BlockingReason::kNotBlocked) {
      future.wait();
    } else {
      // not blocked.
      RowVectorPtr res = hybridExchange_->getOutput();
      if (res) {
        facebook::velox::cudf_velox::CudfVectorPtr cudfRes =
            std::dynamic_pointer_cast<facebook::velox::cudf_velox::CudfVector>(
                res);
        numRows_ += cudfRes->getTableView().num_rows();
      }
    }
    if (hybridExchange_->isFinished()) {
      break;
    }
  }
  hybridExchange_->close();
}

void SinkDriverMock::addSplits(
    std::vector<facebook::velox::exec::Split>& splits) {
  auto planNode = task_->planFragment().planNode;
  for (auto& split : splits) {
    VLOG(3) << "Adding split to planNode: " << planNode->id()
            << " to sink driver for task " << task_->taskId();
    task_->addSplit(planNode->id(), std::move(split));
  }
  task_->noMoreSplits(planNode->id());
}

} // namespace facebook::velox::cudf_exchange
