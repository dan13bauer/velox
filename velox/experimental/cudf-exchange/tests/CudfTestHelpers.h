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

#include <cudf/column/column_factories.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <folly/Executor.h>
#include <memory>
#include <vector>
#include "velox/common/memory/MemoryPool.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestData.h"

namespace facebook::velox::cudf_exchange {

/*const std::vector<std::string> kTestColumnNames = {"c0", "c1","c2"};
const std::vector<TypePtr> kTestColumnTypes = {INTEGER(), DOUBLE(),VARCHAR()};
const facebook::velox::RowTypePtr kTestRowType =
    ROW(kTestColumnNames, kTestColumnTypes);*/

/// @brief Helper function to create a source Task for testing purposes.
/// Creates a simple task associated with a plan fragment that consists of a
/// single value node. The output type is the given row type. Memory pool is
/// only needed to initialize a value node in the plan fragment.
///
/// @param taskId The unique identifier for the task
/// @param pool Shared pointer to the memory pool to use by the value source
/// node.
/// @param rowType The row type to use for the task
/// @return Shared pointer to the created Task
std::shared_ptr<facebook::velox::exec::Task> createSourceTask(
    const std::string& taskId,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    facebook::velox::RowTypePtr rowType);

/// @brief Helper function to create a sink task for testing.
/// Creates a simple task associated with a plan fragment that consists fo a
/// single exchange node. The input type is the given row type.
/// @param taskId The unique identifier for the task
/// @param rowType The row type to use for the task
/// @param partitionId The partition that this task is retrieving from upstream
/// task(s).
/// @param exchangeNodeId Reference parameter that returns the id of the
/// exchange node in this task's plan fragment.
/// @return Shared pointer to the created Task
std::shared_ptr<facebook::velox::exec::Task> createExchangeTask(
    const std::string& taskId,
    facebook::velox::RowTypePtr rowType,
    int partitionId,
    core::PlanNodeId& exchangeNodeId);

/// Helper function to create cudf::packed_columns for testing.
/// Creates a packed table with two columns: INT32 and FLOAT64.
///
/// @param numRows Number of rows to create in the packed columns
/// @return Unique pointer to the created packed columns
///
std::unique_ptr<cudf::packed_columns> makePackedColumns(
    std::size_t numRows,
    facebook::velox::RowTypePtr rowType,
    rmm::cuda_stream_view stream);

std::unique_ptr<cudf::packed_columns> makeFilledPackedColumns(
    std::size_t numRows,
    std::shared_ptr<CudfTestData> dataToSend,
    rmm::cuda_stream_view stream);


} // namespace facebook::velox::cudf_exchange
