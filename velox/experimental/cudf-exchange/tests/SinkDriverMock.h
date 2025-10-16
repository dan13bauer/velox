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

#include "velox/exec/Driver.h"
#include "velox/experimental/cudf-exchange/HybridExchange.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestHelpers.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestData.h"

using namespace facebook::velox::exec;

namespace facebook::velox::cudf_exchange {

/// @class SinkTaskMock
/// @brief A mock class that receives data through an exchange from one or more
/// upstream remote tasks. This class mocks the setup and execution of a plan
/// fragment with a single HybridExchange operator.

class SinkDriverMock {
 public:
  /// @brief Mocks the driver to execute a pipeline with a single exchange
  /// operator. One or more of these drivers may be used to driver multiple
  /// exchange operator instances in the same task. The driver will create the
  /// exchange operator instance and drive it until is is finished or an error
  /// occurs.
  /// @param task A pointer to the sink task.
  /// @param driverId The driverId, must be 0 for the first driver, 1 for the
  /// 2nd driver of this task etc. When driverId > 0, then the exchange client
  /// must be provided.
  /// @param exchangeClient The exchange client to use. If this is a nullptr,
  /// then an exchange client for the partition indicated in the task and the
  /// number of upstream consumers given by numberOfConsumers will be
  /// constructed.
  /// @param numberOfConsumers The number of upstream tasks that provide data
  /// for this given task/partition.
  SinkDriverMock(
      std::shared_ptr<facebook::velox::exec::Task> task,
      int driverId,
      std::shared_ptr<ExchangeClientFacade> exchangeClient = nullptr,
      int32_t numberOfConsumers = 1,
     std::shared_ptr<CudfTestData> dataToSend = nullptr);

  /// @brief Executes the exchange operator until it finishes receiving all data
  /// from the upstream.
  void run();

  /// @brief Adds the set of splits to the task, from where they will be picked
  /// up by the exchange operator to initiate connections to the upstream task.
  /// @param splits A vector containing remote splits.
  void addSplits(std::vector<facebook::velox::exec::Split>& splits);

  /// @brief Returns the number of rows read by "run".
  uint32_t numRows() const {
    return numRows_;
  }

  bool dataIsValid() { return dataValidFlag_;}

 private:

  /// @brief checks if the received table corresponds to that sent, sets dataValidFlag_=false if not
  /// @param tab 
  void updateDataValidity(const cudf::table_view& tab);

  bool dataValidFlag_ = true;
  std::shared_ptr<facebook::velox::exec::Task> task_;
  int driverId_;
  DriverCtx driverCtx_;
  std::shared_ptr<ExchangeClientFacade> exchangeClient_;
  std::unique_ptr<HybridExchange> hybridExchange_;
  uint64_t numRows_;
  const std::shared_ptr<CudfTestData> dataToSend_;
};

} // namespace facebook::velox::cudf_exchange
