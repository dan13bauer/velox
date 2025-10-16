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

#include "velox/experimental/cudf-exchange/CudfOutputQueueManager.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestData.h"


namespace facebook::velox::cudf_exchange {

/// @brief Mocks the cudf partitioned output operator by
/// pushing data into a set of partitions.
class CudfPartitionedOutputMock {
 public:
  CudfPartitionedOutputMock(
      const std::string& taskId,
      const size_t numPartitions,
      const uint32_t numDataChunks,
      const size_t numRowsPerChunk,
      std::shared_ptr<CudfTestData> dataToSend = nullptr);

  /// @brief creates numPartitions_ x numDataChunks_ data chunks and
  /// pushes it into the destination queues identified by the taskId.
  void run();
 
 private:
  const std::shared_ptr<CudfOutputQueueManager> queueManager_;
  const std::string taskId_;
  const size_t numPartitions_;
  const uint32_t numDataChunks_;
  const size_t numRowsPerChunk_;
  const std::shared_ptr<CudfTestData> dataToSend_;
};

} // namespace facebook::velox::cudf_exchange
