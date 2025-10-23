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


#include <cudf/strings/strings_column_view.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/unary.hpp>
#include <cudf/binaryop.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>


namespace facebook::velox::cudf_exchange {


constexpr int kPipelineId = 0;
constexpr uint32_t kPartitionId = 0; // not used in tests.

SinkDriverMock::SinkDriverMock(
    std::shared_ptr<facebook::velox::exec::Task> task,
    int driverId,
    std::shared_ptr<ExchangeClientFacade> exchangeClient,
    int32_t numberOfConsumers,
    std::shared_ptr<CudfTestData> dataToSend)
    : task_{std::move(task)},
      driverCtx_{task_, driverId, kPipelineId, kUngroupedGroupId, kPartitionId},
      numRows_{0},
      dataToSend_(dataToSend){
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

void SinkDriverMock::updateDataValidity(const cudf::table_view& tab) {
  // Should make the test more dynamic, row is assumed to be [integer, double,
  // string]

  int num_columns = tab.num_columns();
  int num_rows= dataToSend_->getNumRows();

  VELOX_CHECK_EQ(num_columns, CudfTestData::kTestColumnTypes.size());

  for (int i = 0; i < num_columns; i++) {
    VLOG(4) << "Type of column " << i << " "
            << cudf::type_to_name(tab.column(i).type());
  }

  // Get the Reference data
  std::shared_ptr<std::vector<uint32_t>> integers = dataToSend_->getIntegers();
  std::shared_ptr<std::vector<float>> doubles = dataToSend_->getDoubles();
  const std::shared_ptr<std::vector<std::string>>& strings = dataToSend_->getStrings();

  // Get the Received data: assume in a fixed order
  cudf::column_view iCol = tab.column(0);
  cudf::column_view dCol = tab.column(1);
  cudf::strings_column_view sCol{tab.column(2)};
  rmm::cuda_stream_pool stream_pool(32);
  const std::vector<std::string>& col_strings = getStringCol(sCol, num_rows,stream_pool.get_stream());
  std::vector<uint32_t>  col_integers = getColVector<uint32_t>(iCol,num_rows,stream_pool.get_stream());
  std::vector<float>  col_doubles = getColVector<float>(dCol,num_rows,stream_pool.get_stream());
  // Com pare Reference with Received
  for (int i = 0; i < num_rows; i++) {
    if (integers->at(i) != col_integers.at(i)) {
      VLOG(0) << "Error " << integers->at(i) << " != " << col_integers.at(i);
      dataValidFlag_ = false;
    }
    if (doubles->at(i) != col_doubles.at(i)) {
      VLOG(0) << "Error " << doubles->at(i) << " != " << col_doubles.at(i);
      dataValidFlag_ = false;
    }

  if (strings->at(i) != col_strings.at(i)) {
      VLOG(0) << "Error " << strings->at(i) << " != " << col_strings.at(i);
      dataValidFlag_ = false;
    }
  }
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
        // If we have Reference data check the received data is the same
        if (dataToSend_) updateDataValidity(cudfRes->getTableView());
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
 