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
#include "velox/experimental/cudf-exchange/tests/CudfTestHelpers.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace facebook::velox::cudf_exchange {

std::shared_ptr<Task> createSourceTask(
    const std::string& taskId,
    std::shared_ptr<memory::MemoryPool> pool,
    RowTypePtr rowType) {
  VLOG(3) << "Testing SourceTask";
  const size_t vectorSize = 10;

  auto typeParams = rowType->parameters();
  std::vector<VectorPtr> vecPtrs;
  for (auto& typeParam : typeParams) {
    vecPtrs.emplace_back(
        BaseVector::create(typeParam.type, vectorSize, pool.get()));
  }

  // Wrap the vector (column) in a RowVector.
  auto rowVector = std::make_shared<RowVector>(
      pool.get(), // pool where allocations will be made.
      rowType, // input row type.
      BufferPtr(nullptr), // no nulls on this example.
      vectorSize, // length of the vectors.
      vecPtrs); // the input vector data.

  auto planFragment =
      exec::test::PlanBuilder().values({rowVector}).planFragment();

  std::unordered_map<std::string, std::string> configSettings;
  auto queryCtx = core::QueryCtx::create(
      nullptr, core::QueryConfig(std::move(configSettings)));

  auto task = Task::create(
      taskId,
      std::move(planFragment),
      0, // partition number, irrelevant here; will be set by the test.
      std::move(queryCtx),
      Task::ExecutionMode::kParallel);

  return task;
}

std::shared_ptr<facebook::velox::exec::Task> createExchangeTask(
    const std::string& taskId,
    facebook::velox::RowTypePtr rowType,
    int partitionId,
    core::PlanNodeId& exchangeNodeId) {
  auto planFragment = exec::test::PlanBuilder()
                          .exchange(rowType, VectorSerde::Kind::kCompactRow)
                          .capturePlanNodeId(exchangeNodeId)
                          .planFragment();

  std::unordered_map<std::string, std::string> configSettings;
  auto queryCtx = core::QueryCtx::create(
      nullptr, core::QueryConfig(std::move(configSettings)));

  auto task = Task::create(
      taskId,
      std::move(planFragment),
      partitionId,
      std::move(queryCtx),
      Task::ExecutionMode::kParallel);
  return task;
}

std::unique_ptr<cudf::packed_columns> makePackedColumns(
    std::size_t numRows,
    rmm::cuda_stream_view stream) {
  // Create two numeric columns using cudf::make_numeric_column
  auto col1 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      numRows,
      cudf::mask_state::UNALLOCATED);
  auto col2 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::FLOAT64},
      numRows,
      cudf::mask_state::UNALLOCATED);

  // Table will contain arbitrary values.

  // Build cudf::table
  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(col1));
  columns.push_back(std::move(col2));
  auto table = std::make_unique<cudf::table>(std::move(columns));

  cudf::packed_columns packed = cudf::pack(table->view(), stream);

  return std::unique_ptr<cudf::packed_columns>(new cudf::packed_columns(
      std::move(packed.metadata), std::move(packed.gpu_data)));
}

} // namespace facebook::velox::cudf_exchange
