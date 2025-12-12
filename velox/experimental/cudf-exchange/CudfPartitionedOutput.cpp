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
#include <sstream>
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#include "velox/experimental/cudf-exchange/CudfExchangeProtocol.h"
#include "velox/experimental/cudf-exchange/CudfQueues.h"

#include <cuda_runtime.h>
#include <cudf/contiguous_split.hpp>
#include <cudf/copying.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/strings/strings_column_view.hpp>

using namespace facebook::velox::cudf_velox;
using facebook::velox::exec::Task;
namespace facebook::velox::cudf_exchange {

// Computes a mapping from names in n2 to names in n1
// and returns that mapping in remap.
// Names in n2 must occurs in n1.
static void getRemapping(
    const std::vector<std::string>& n1,
    const std::vector<std::string>& n2,
    std::vector<uint32_t>& remap) {
  std::unordered_map<std::string, uint32_t> lookup;
  for (uint32_t i = 0; i < n1.size(); ++i) {
    lookup[n1[i]] = i;
  }

  remap.clear();
  remap.reserve(n2.size());
  for (const auto& key : n2) {
    remap.push_back(lookup.at(key));
  }
}

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
      numPartitions_(planNode->numPartitions()),
      pipelineId_(ctx->pipelineId),
      driverId_(ctx->driverId) {
  this->initPartitionKeys(planNode);
  auto sources = planNode->sources();
  std::vector<std::string> inNames, outNames;
  inNames.reserve(planNode->inputType()->size());
  for (int i = 0; i < planNode->inputType()->size(); ++i) {
    inNames.push_back(planNode->inputType()->nameOf(i));
  }
  outNames.reserve(planNode->outputType()->size());
  for (int i = 0; i < planNode->outputType()->size(); ++i) {
    outNames.push_back(planNode->outputType()->nameOf(i));
  }
  if (inNames != outNames) {
    getRemapping(inNames, outNames, remap_);
  }

  // Log planNode input and output types
  VLOG(3) << "@" << taskId() << " CudfPartitionedOutput inputType: "
          << planNode->inputType()->toString();
  VLOG(3) << "@" << taskId() << " CudfPartitionedOutput outputType: "
          << planNode->outputType()->toString();
  if (!remap_.empty()) {
    std::stringstream ss;
    ss << "@" << taskId() << " CudfPartitionedOutput remap: [";
    for (size_t i = 0; i < remap_.size(); ++i) {
      if (i > 0) ss << ", ";
      ss << remap_[i];
    }
    ss << "]";
    VLOG(3) << ss.str();
  }
}

void CudfPartitionedOutput::addInput(RowVectorPtr input) {
  VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_ << " addInput";
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  auto cudfVector = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK(cudfVector, "Input must be a CudfVector");
  VELOX_CHECK(
      !future_.valid() || future_.hasValue(),
      "addInput with outstanding future!");
  try {
    auto stream = cudfVector->stream();

    cudf::table_view tableView;
    bool blocked;
    if (remap_.empty()) {
      // input and output column order is the same.
      tableView = cudfVector->getTableView();
    } else {
      // input and output column order needs re-mapping.
      tableView =
          cudfVector->getTableView().select(remap_.begin(), remap_.end());
    }

    auto queueManager = sharedQueueManager();
    if (numPartitions_ > 1) {
      if (partitionKeyIndices_.size() > 0 || spec_ == "gather") {
        hashPartition(tableView, stream);
      } else {
        // For equal partition, we need to release the table from CudfVector
        // since cudf::split returns views that reference the source table.
        stream.synchronize();
        auto sourceTable =
            std::shared_ptr<cudf::table>(cudfVector->release());
        // Pass the remapped tableView for correct column order
        equalPartition(tableView, std::move(sourceTable), stream);
      }
    } else {
      // Single partition case. No need to hash, assume queue zero.
      // Use zero-copy transfer: release the table and generate metadata only.
      auto numRows = tableView.num_rows();

      // Sync the stream since UCXX/UCX is not stream oriented and without
      // syncing, data could get lost. Syncing here is easy but not the most
      // efficient. A better approach is to create an event and pass it along
      // the data through the queue and synchronize on the event before calling
      // into UCXX.
      // TODO: change stream sync and move to event sync
      // Thanks to Lawrence Mitchel for pointing this out!
      stream.synchronize();

      // Release the table from CudfVector - this avoids data copy
      auto sourceTable =
          std::shared_ptr<cudf::table>(cudfVector->release());

      // Generate custom metadata for the table using the REMAPPED tableView
      // (not sourceTable->view()) to ensure correct column order
      auto metadata = TableMetadata::buildFromTable(tableView);

      // Create unpacked table data with remapped view + shared ownership
      auto unpackedData = std::make_unique<TableWithMetadata>();
      unpackedData->metadata = std::move(metadata);
      unpackedData->tableView = tableView;  // Use remapped view
      unpackedData->sourceTable = std::move(sourceTable);
      unpackedData->numRows = numRows;
      unpackedData->stream = stream;  // Propagate stream for proper sync

      // Set chunk sequence, partition, and driver ID for tracing
      unpackedData->chunkSeq = chunkSeq_;
      unpackedData->partition = 0;
      unpackedData->driverId = driverId_;

      // Log ENQUEUE checksums for single partition (partition 0)
      logEnqueueChecksums(tableView, chunkSeq_, 0);

      queueManager->enqueue(this->taskId(), 0, std::move(unpackedData));
    }
    // Increment chunk sequence after processing this input batch
    ++chunkSeq_;

    // Check once after all enqueues if we're blocked
    blocked = queueManager->checkBlocked(this->taskId(), &future_);
    // record the statistics.
    {
      auto lockedStats = stats_.wlock();
      lockedStats->addOutputVector(input->estimateFlatSize(), input->size());
    }
    if (blocked) {
      VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_ << " is blocked, can no longer write to output!";
    }
    blockingReason_ = blocked ? exec::BlockingReason::kWaitForConsumer
                              : exec::BlockingReason::kNotBlocked;

  } catch (const rmm::bad_alloc& e) {
    VLOG(1) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_
        << " caught memory alloc error, removing all memory in output queues";
    for (int i = 0; i < numPartitions_; i++) {
      sharedQueueManager()->deleteResults(this->taskId(), i);
    }
    throw; // Let the driver know we have failed
  }
}

exec::BlockingReason CudfPartitionedOutput::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != exec::BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    blockingReason_ = exec::BlockingReason::kNotBlocked;
    return exec::BlockingReason::kWaitForConsumer;
  }
  return exec::BlockingReason::kNotBlocked;
}

RowVectorPtr CudfPartitionedOutput::getOutput() {
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

void CudfPartitionedOutput::hashPartition(
    cudf::table_view tableView,
    rmm::cuda_stream_view stream) {
  VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_ << " Hashing and partitioning into " << numPartitions_ << " chunks";

  // Use cudf hash partitioning
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
      stream);

  // hash_partition is async, sync before splitting to ensure data is ready
  stream.synchronize();

  VELOX_CHECK(partitionOffsets.size() == numPartitions_);
  VELOX_CHECK(partitionOffsets[0] == 0);

  // Erase first element since it's always 0 and we don't need it.
  partitionOffsets.erase(partitionOffsets.begin());

  // Convert to shared_ptr for shared ownership across partitions
  auto sourceTable =
      std::shared_ptr<cudf::table>(std::move(partitionedTable));

  // hash_partition returns a new table with the same column order as the input
  // tableView (which may have been remapped). Use sourceTable->view() since
  // it points to the actual partitioned data.
  splitAndEnqueue(std::move(sourceTable), partitionOffsets, stream);
}

void CudfPartitionedOutput::equalPartition(
    cudf::table_view tableView,
    std::shared_ptr<cudf::table> sourceTable,
    rmm::cuda_stream_view stream) {
  VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_ << " Splitting into " << numPartitions_ << " chunks";
  std::vector<cudf::size_type> offsets;
  cudf::size_type size = tableView.num_rows();
  for (int i = 1; i < numPartitions_; ++i) {
    cudf::size_type idx = size / (numPartitions_ / (double)i);
    offsets.push_back(idx);
  }
  // Pass the remapped tableView for correct column order
  splitAndEnqueue(tableView, std::move(sourceTable), offsets, stream);
}

void CudfPartitionedOutput::splitAndEnqueue(
    std::shared_ptr<cudf::table> sourceTable,
    std::vector<cudf::size_type> offsets,
    rmm::cuda_stream_view stream) {
  // This overload uses sourceTable->view() directly (for hashPartition where
  // the table is already in the correct column order after hash_partition)
  auto tableView = sourceTable->view();
  splitAndEnqueue(tableView, std::move(sourceTable), offsets, stream);
}

void CudfPartitionedOutput::splitAndEnqueue(
    cudf::table_view tableView,
    std::shared_ptr<cudf::table> sourceTable,
    std::vector<cudf::size_type> offsets,
    rmm::cuda_stream_view stream) {
  // Use cudf::split for zero-copy splitting. Returns vector of table_views
  // that reference the source table's data (via tableView which may be remapped).
  auto tableViews = cudf::split(tableView, offsets, stream);

  VELOX_CHECK_EQ(
      offsets.size() + 1, numPartitions_, "mismatch in numPartitions_");

  auto queueManager = sharedQueueManager();
  for (int i = 0; i < numPartitions_; ++i) {
    auto const& partitionView = tableViews[i];
    if (partitionView.num_rows() == 0) {
      // Skip empty partitions.
      continue;
    }

    // Generate custom metadata for the partition view
    auto metadata = TableMetadata::buildFromTable(partitionView);

    // Create unpacked table data with view + shared ownership of source
    auto unpackedData = std::make_unique<TableWithMetadata>();
    unpackedData->metadata = std::move(metadata);
    unpackedData->tableView = partitionView;
    unpackedData->sourceTable = sourceTable; // All partitions share ownership

    // DEBUG: Log column types being stored in TableWithMetadata
    {
      std::stringstream ss;
      ss << "@" << taskId() << " drv=" << driverId_
         << " chunk=" << chunkSeq_ << " part=" << i
         << " STORING tableView with column types: [";
      for (cudf::size_type c = 0; c < partitionView.num_columns(); ++c) {
        if (c > 0) ss << ", ";
        ss << static_cast<int>(partitionView.column(c).type().id());
      }
      ss << "]";
      VLOG(1) << ss.str();
    }
    unpackedData->numRows = partitionView.num_rows();
    unpackedData->stream = stream;  // Propagate stream for proper sync
    unpackedData->chunkSeq = chunkSeq_;  // Set chunk sequence for tracing
    unpackedData->partition = i;  // Set partition index for tracing
    unpackedData->driverId = driverId_;  // Set driver ID for tracing

    // Log ENQUEUE checksums for this partition (after split, before enqueue)
    logEnqueueChecksums(partitionView, chunkSeq_, i);

    // enqueue partition data on Cudf Output Buffer
    queueManager->enqueue(this->taskId(), i, std::move(unpackedData));
  }
}

void CudfPartitionedOutput::logEnqueueChecksums(
    cudf::table_view tableView,
    uint64_t chunkSeq,
    int partitionIndex) {
  // Compute checksums for each column in depth-first order (matches metadata)
  size_t colIdx = 0;
  std::function<void(cudf::column_view)> logColumnChecksum =
      [&, chunkSeq](cudf::column_view colView) {
        auto dataType = colView.type();
        auto size = colView.size();
        size_t currentColIdx = colIdx++;

        if (dataType.id() == cudf::type_id::STRING && size > 0 &&
            colView.num_children() > 0) {
          // STRING column - compute checksum of chars
          // For split STRING columns, we compute partial chars range
          // The parent column's offset() tells us where this slice starts
          // in the offsets array. We read offsets[parent.offset()] and
          // offsets[parent.offset() + parent.size()] to get the byte range.
          cudf::strings_column_view scv(colView);
          auto offsetsView = scv.offsets();
          auto parentOffset = colView.offset();  // Index into offsets array

          int32_t firstOffset = 0;
          int32_t lastOffset = 0;
          // Read offsets at positions parent.offset() and parent.offset() + size
          // Note: offsetsView.head() points to the raw offsets buffer,
          // and offsetsView.offset() may also be set, so we use both.
          const int32_t* offsetsPtr =
              static_cast<const int32_t*>(offsetsView.head()) +
              offsetsView.offset();
          cudaMemcpy(
              &firstOffset,
              offsetsPtr + parentOffset,
              sizeof(int32_t),
              cudaMemcpyDeviceToHost);
          cudaMemcpy(
              &lastOffset,
              offsetsPtr + parentOffset + size,
              sizeof(int32_t),
              cudaMemcpyDeviceToHost);

          auto actualCharsSize = lastOffset - firstOffset;
          uint64_t checksum = 0;
          if (actualCharsSize > 0) {
            const char* charsBegin = scv.chars_begin(rmm::cuda_stream_default);
            const void* partialCharsPtr = charsBegin + firstOffset;
            if (actualCharsSize >= 16) {
              uint64_t first = 0, last = 0;
              cudaMemcpy(&first, partialCharsPtr, std::min<int32_t>(8, actualCharsSize), cudaMemcpyDeviceToHost);
              cudaMemcpy(&last, static_cast<const uint8_t*>(partialCharsPtr) + actualCharsSize - 8, 8, cudaMemcpyDeviceToHost);
              checksum = first ^ last ^ actualCharsSize;
            } else {
              cudaMemcpy(&checksum, partialCharsPtr, std::min<int32_t>(8, actualCharsSize), cudaMemcpyDeviceToHost);
              checksum ^= actualCharsSize;
            }
          }
          VLOG(1) << "[ENQUEUE-STR] @" << taskId()
                  << " drv=" << driverId_
                  << " chunk=" << chunkSeq
                  << " part=" << partitionIndex
                  << " col=" << currentColIdx
                  << " STRING: numStrings=" << size
                  << " offset=" << colView.offset()
                  << " charsSize=" << actualCharsSize
                  << " (range " << firstOffset << "-" << lastOffset << ")"
                  << " needsRebasing=" << (firstOffset > 0 ? "yes" : "no")
                  << " checksum=" << std::hex << checksum << std::dec;
        } else if (size > 0 && colView.head() != nullptr &&
                   cudf::is_fixed_width(dataType)) {
          // Fixed-width column
          size_t dataSize = size * cudf::size_of(dataType);
          const void* dataPtr = static_cast<const void*>(
              static_cast<const uint8_t*>(colView.head()) +
              colView.offset() * cudf::size_of(dataType));
          uint64_t checksum = 0;
          if (dataSize >= 16) {
            uint64_t first = 0, last = 0;
            cudaMemcpy(&first, dataPtr, std::min<size_t>(8, dataSize), cudaMemcpyDeviceToHost);
            cudaMemcpy(&last, static_cast<const uint8_t*>(dataPtr) + dataSize - 8, 8, cudaMemcpyDeviceToHost);
            checksum = first ^ last ^ dataSize;
          } else if (dataSize > 0) {
            cudaMemcpy(&checksum, dataPtr, std::min<size_t>(8, dataSize), cudaMemcpyDeviceToHost);
            checksum ^= dataSize;
          }
          VLOG(1) << "[ENQUEUE-DATA] @" << taskId()
                  << " drv=" << driverId_
                  << " chunk=" << chunkSeq
                  << " part=" << partitionIndex
                  << " col=" << currentColIdx
                  << " type=" << static_cast<int>(dataType.id())
                  << " size=" << size
                  << " offset=" << colView.offset()
                  << " dataSize=" << dataSize
                  << " checksum=" << std::hex << checksum << std::dec;
        }

        // Recursively process children
        // Note: STRING columns are fully handled above and should NOT recurse
        // to children, matching the behavior in CudfExchangeServer::sendColumnView
        // which handles STRING chars, null_mask, and offsets specially and returns early.
        // However, we must increment colIdx for the synthetic offsets entry that
        // metadata includes for STRING columns.
        if (dataType.id() == cudf::type_id::STRING) {
          if (colView.num_children() > 0 && size > 0) {
            // Metadata includes a synthetic offsets entry after STRING, increment counter
            ++colIdx;
          }
        } else {
          for (int i = 0; i < colView.num_children(); ++i) {
            logColumnChecksum(colView.child(i));
          }
        }
      };

  for (cudf::size_type i = 0; i < tableView.num_columns(); ++i) {
    logColumnChecksum(tableView.column(i));
  }
}

} // namespace facebook::velox::cudf_exchange
