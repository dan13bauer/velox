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
#include "velox/experimental/cudf-exchange/CudfPartitionedOutput.h"
#include <fmt/format.h>
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include <cudf/contiguous_split.hpp>
#include <cudf/copying.hpp>
#include <cudf/partitioning.hpp>

using namespace facebook::velox::cudf_velox;
using facebook::velox::exec::Task;
namespace facebook::velox::cudf_exchange {

CudfPartitionedOutput::CudfPartitionedOutput(
    int32_t operatorId,
    exec::DriverCtx* ctx,
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
    bool eagerFlush)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "cudfPartitionedOutput"),
      NvtxHelper(
          nvtx3::rgb{255, 215, 0}, // Gold
          operatorId,
          fmt::format("[{}]", planNode->id())),
      queueManager_(CudfOutputQueueManager::getInstanceRef()),
      numPartitions_(planNode->numPartitions()) {
  this->initPartitionKeys(planNode);
}

void CudfPartitionedOutput::addInput(RowVectorPtr input) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  auto cudfVector = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK(cudfVector, "Input must be a CudfVector");
  auto stream = cudfVector->stream();

  if (numPartitions_ > 1) {
    if (partitionKeyIndices_.size() > 0 || spec_ == "gather") {
      hashPartition(cudfVector);
    } else {
      equalPartition(cudfVector);
    }
  } else {
    // Single partition case. No need to hash, assume queue zero
    auto packedCols = cudf::pack(
        cudfVector->getTableView(), stream
        // mr = cudf::get_current_device_resource_ref() //?
    );
    std::unique_ptr<cudf::packed_columns> packedColsPtr =
        std::make_unique<cudf::packed_columns>(
            std::move(packedCols.metadata), std::move(packedCols.gpu_data));
    bool res = sharedQueueManager()->enqueue(
        this->taskId(),
        0,
        std::move(packedColsPtr),
        cudfVector->getTableView().num_rows(),
        nullptr);
    // FIXME Will return true if queue is full: ignore for now
    VLOG(3) << "enqueued cudf vector with "
            << cudfVector->getTableView().num_rows() << " rows";
  }
}

exec::BlockingReason CudfPartitionedOutput::isBlocked(ContinueFuture* future) {
  // As we currently are ignoring running out of memory we are never blocked
  return exec::BlockingReason::kNotBlocked;
}

RowVectorPtr CudfPartitionedOutput::getOutput() {
  VLOG(2) << "CudfPartitionedOutput::getOutput()";
  if (finished_) {
    return nullptr;
  }
  if (noMoreInput_) {
    // Tell the queue manager there is nothing more to come
    sharedQueueManager()->noMoreData(this->taskId());
    finished_ = true;
  }
  return nullptr;
}

bool CudfPartitionedOutput::isFinished() {
  VLOG(2) << "CudfPartitionedOutput::isFinished(): " << finished_;
  return finished_;
}

std::shared_ptr<facebook::velox::cudf_exchange::CudfOutputQueueManager>
CudfPartitionedOutput::sharedQueueManager() {
  auto shared_queueManager = queueManager_.lock();
  VELOX_CHECK_NOT_NULL(
      shared_queueManager, "OutputQueueManager was already destructed");
  return shared_queueManager;
}

void CudfPartitionedOutput::initPartitionKeys(
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode) {
  // Following Logic copied direcly from CudLocalPartition (!)

  // Following is IMO a hacky way to get the partition key indices. It is to
  // workaround the fact that the partition spec constructs the hash function
  // directly and has no public methods to get the partition key indices.

  // When the operator is of type kRepartition, the partition spec is a string
  // in the format "HASH(key1, key2, ...)"
  // We're going to extract the keys between HASH( and ) and find their indices
  // in the output row type.

  // When operator is of type kGather, we don't need to store any partition key
  // indices because we're going to merge all the incoming streams together.

  // Get partition function specification string
  spec_ = planNode->partitionFunctionSpec().toString();

  // Only parse keys if it's a hash function
  if (spec_.find("HASH(") != std::string::npos) {
    // Extract keys between HASH( and )
    size_t start = spec_.find("HASH(") + 5;
    size_t end = spec_.find(")", start);
    if (start != std::string::npos && end != std::string::npos) {
      std::string keysStr = spec_.substr(start, end - start);

      // Split by comma to get individual keys.
      std::vector<std::string> keys;
      size_t pos = 0;
      while ((pos = keysStr.find(",")) != std::string::npos) {
        std::string key = keysStr.substr(0, pos);
        keys.push_back(key);
        keysStr.erase(0, pos + 1);
      }
      keys.push_back(keysStr); // Add the last key.

      // Find field indices for each key.
      const auto& rowType = planNode->outputType();
      for (const auto& key : keys) {
        auto trimmedKey = key;
        // Trim whitespace
        trimmedKey.erase(0, trimmedKey.find_first_not_of(" "));
        trimmedKey.erase(trimmedKey.find_last_not_of(" ") + 1);

        auto fieldIndex = rowType->getChildIdx(trimmedKey);
        partitionKeyIndices_.push_back(fieldIndex);
      }
    }
  }
}

void CudfPartitionedOutput::hashPartition(CudfVectorPtr cudfVector) {
  VLOG(3) << "Hashing and partitioning into " << numPartitions_ << " chunks";

  // Use cudf hash partitioning
  auto tableView = cudfVector->getTableView();
  std::vector<cudf::size_type> partitionKeyIndices;
  for (const auto& idx : partitionKeyIndices_) {
    partitionKeyIndices.push_back(static_cast<cudf::size_type>(idx));
  }

  auto [partitionedTable, partitionOffsets] = cudf::hash_partition(
      tableView,
      partitionKeyIndices,
      numPartitions_,
      cudf::hash_id::HASH_MURMUR3,
      cudf::DEFAULT_HASH_SEED,
      cudfVector->stream());

  VELOX_CHECK(partitionOffsets.size() == numPartitions_);
  VELOX_CHECK(partitionOffsets[0] == 0);

  // Erase first element since it's always 0 and we don't need it.
  partitionOffsets.erase(partitionOffsets.begin());

  splitAndEnqueue(
      partitionedTable->view(), partitionOffsets, cudfVector->stream());
}

void CudfPartitionedOutput::equalPartition(CudfVectorPtr cudfVector) {
  VLOG(3) << "Splitting into " << numPartitions_ << " chunks";
  auto tableView = cudfVector->getTableView();
  std::vector<cudf::size_type> offsets;
  cudf::size_type size = tableView.num_rows();
  for (int i = 1; i < numPartitions_; ++i) {
    cudf::size_type idx = size / (numPartitions_ / (double)i);
    offsets.push_back(idx);
  }
  splitAndEnqueue(tableView, offsets, cudfVector->stream());
}

void CudfPartitionedOutput::splitAndEnqueue(
    cudf::table_view tableView,
    std::vector<cudf::size_type> offsets,
    rmm::cuda_stream_view stream) {
  auto contiguousTables = cudf::contiguous_split(tableView, offsets, stream);

  VELOX_CHECK_EQ(
      offsets.size() + 1, numPartitions_, "mismatch in numPartitions_");
  for (int i = 0; i < numPartitions_; ++i) {
    auto const& partitionTable = contiguousTables[i];
    auto packedColsPtr = std::make_unique<cudf::packed_columns>(
        std::move(contiguousTables[i].data.metadata),
        std::move(contiguousTables[i].data.gpu_data));

    if (partitionTable.table.num_rows() == 0) {
      // Skip empty partitions.
      continue;
    }

    // enqueue partition data on Cudf Output Buffer
    VLOG(3) << "Enqueued " << partitionTable.table.num_rows()
            << " rows into partition " << i;
    bool res = sharedQueueManager()->enqueue(
        this->taskId(),
        i,
        std::move(packedColsPtr),
        partitionTable.table.num_rows(),
        nullptr);

    // FIXME Will return false if queue is full: ignore for now
  }
}

} // namespace facebook::velox::cudf_exchange
