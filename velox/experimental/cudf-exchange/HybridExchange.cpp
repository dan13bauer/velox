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
#include "velox/experimental/cudf-exchange/HybridExchange.h"
#include <cuda_runtime.h>
#include <cudf/strings/strings_column_view.hpp>
#include "velox/experimental/cudf-exchange/Communicator.h"
#include "velox/experimental/cudf-exchange/ExchangeClientFacade.h"
#include "velox/experimental/cudf-exchange/NetUtil.h"
#include "velox/experimental/cudf/exec/Utilities.h"

// Uncomment to enable checkpoint/checksum logging for debugging data corruption
// #define CHECKSUM_LOGGING 1

using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace facebook::velox::cudf_exchange {

namespace {
std::unique_ptr<VectorSerde::Options> getVectorSerdeOptions(
    const core::QueryConfig& queryConfig,
    VectorSerde::Kind kind) {
  std::unique_ptr<VectorSerde::Options> options =
      kind == VectorSerde::Kind::kPresto
      ? std::make_unique<serializer::presto::PrestoVectorSerde::PrestoOptions>()
      : std::make_unique<VectorSerde::Options>();
  options->compressionKind =
      common::stringToCompressionKind(queryConfig.shuffleCompressionKind());
  return options;
}
} // namespace

// --- Implementation of the HybridExchange operator.

HybridExchange::HybridExchange(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::PlanNode>& planNode,
    std::shared_ptr<ExchangeClientFacade> exchangeClientFacade,
    const std::string& operatorType)
    : SourceOperator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          operatorType),
      preferredOutputBatchBytes_{
          driverCtx->queryConfig().preferredOutputBatchBytes()},
      processSplits_{driverCtx->driverId == 0},
      pipelineId_{driverCtx->pipelineId},
      driverId_{driverCtx->driverId} {
  // Log planNode outputType for debugging column order issues
  VLOG(3) << "@" << taskId() << " HybridExchange expected outputType: "
          << planNode->outputType()->toString();
  if (exchangeClientFacade) {
    // cudfExchangeClient is provided externally when this is a "plain"
    // CudfExchange.
    exchangeClient_ = exchangeClientFacade;
    std::shared_ptr<const core::ExchangeNode> exchangeNode =
        std::static_pointer_cast<const core::ExchangeNode>(planNode);
    VELOX_CHECK_NOT_NULL(exchangeNode, "Plan node must be an Exchange node!");
    serdeKind_ = exchangeNode->serdeKind();
    serdeOptions_ = getVectorSerdeOptions(
        operatorCtx_->driverCtx()->queryConfig(), serdeKind_);
  } else {
    // cudfExchangeClient is nullptr when this CudfExchange is used to implement
    // a MergeExchange. Create a new cudf exchange client.
    auto task = operatorCtx_->task();
    auto client = std::make_shared<CudfExchangeClient>(
        task->taskId(),
        task->destination(),
        1 // number of consumers, is always 1.
    );
    exchangeClient_ =
        std::make_shared<ExchangeClientFacade>(taskId(), pipelineId_, std::move(client), nullptr);
    exchangeClient_->activateCudfExchangeClient();
  }
}

HybridExchange::~HybridExchange() {
  close();
}

void HybridExchange::addRemoteTaskIds(std::vector<std::string>& remoteTaskIds) {
  std::shuffle(std::begin(remoteTaskIds), std::end(remoteTaskIds), rng_);
  for (const std::string& remoteTaskId : remoteTaskIds) {
    exchangeClient_->addRemoteTaskId(remoteTaskId);
  }
  stats_.wlock()->numSplits += remoteTaskIds.size();
}

void HybridExchange::getSplits(ContinueFuture* future) {
  VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_ << " getSplits called for task: " << taskId();
  if (!processSplits_) {
    VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_ << " getSplits: Not allowed to process splits for task: " << taskId();
    return;
  }
  if (noMoreSplits_) {
    VLOG(3) << "@" << taskId() << "#" << pipelineId_ << "/" << driverId_ << " getSplits: No more splits for task: " << taskId();
    return;
  }
  std::vector<std::string> remoteTaskIds;
  // loop until we get an end marker.
  for (;;) {
    exec::Split split;
    auto reason = operatorCtx_->task()->getSplitOrFuture(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId(), split, *future);
    if (reason != BlockingReason::kNotBlocked) {
      // we are blocked. Add the splits collected so far (if any) and return.
      addRemoteTaskIds(remoteTaskIds);
      return;
    }

    if (split.hasConnectorSplit()) {
      auto remoteSplit = std::dynamic_pointer_cast<RemoteConnectorSplit>(
          split.connectorSplit);
      VELOX_CHECK_NOT_NULL(remoteSplit, "Wrong type of split");
      remoteTaskIds.push_back(remoteSplit->taskId);
      // check for more splits.
      continue;
    }

    // not blocked but also no connector split.
    // we've reached the end.
    addRemoteTaskIds(remoteTaskIds);
    exchangeClient_->noMoreRemoteTasks();
    noMoreSplits_ = true;
    if (atEnd_) {
      operatorCtx_->task()->multipleSplitsFinished(
          false, stats_.rlock()->numSplits, 0);
      recordExchangeClientStats();
    }
    return;
  }
}

bool HybridExchange::resultIsEmpty() {
  auto checkEmpty = [](auto&& res) -> bool {
    // std::decay_t makes sure that references, const, const references etc.
    // are "decayed" into the base type to allow for the type comparison.
    if constexpr (std::is_same_v<std::decay_t<decltype(res)>, SerPageVector>) {
      return res.empty();
    } else {
      // TableWithStream
      return res.table == nullptr;
    }
  };
  return std::visit(checkEmpty, currentData_);
}

void HybridExchange::emptyResult() {
  auto empty = [](auto&& res) -> void {
    // std::decay_t makes sure that references, const, const references etc.
    // are "decayed" into the base type to allow for the type comparison.
    if constexpr (std::is_same_v<std::decay_t<decltype(res)>, SerPageVector>) {
      res.clear();
    } else {
      // TableWithStream
      res.table.reset();
    }
  };
  return std::visit(empty, currentData_);
}

const SerPageVector* HybridExchange::getResultPages() {
  return std::get_if<SerPageVector>(&currentData_);
}

const TableWithStream* HybridExchange::getResultTable() {
  return std::get_if<TableWithStream>(&currentData_);
}

BlockingReason HybridExchange::isBlocked(ContinueFuture* future) {
  if (!resultIsEmpty() || atEnd_) {
    return BlockingReason::kNotBlocked;
  }

  // Get splits from the task if no splits are outstanding.
  if (!splitFuture_.valid()) {
    getSplits(&splitFuture_);
  }

  ContinueFuture dataFuture = ContinueFuture::makeEmpty();
  currentData_ = exchangeClient_->next(
      driverId_, preferredOutputBatchBytes_, &atEnd_, &dataFuture);
  if (!resultIsEmpty() || atEnd_) {
    // got some data or reached the end.
    if (atEnd_ && noMoreSplits_) {
      const auto numSplits = stats_.rlock()->numSplits;
      operatorCtx_->task()->multipleSplitsFinished(false, numSplits, 0);
    }
    recordExchangeClientStats();
    return BlockingReason::kNotBlocked;
  }

  if (splitFuture_.valid()) {
    // Combine the futures and block until data becomes available or more splits
    // arrive.
    std::vector<ContinueFuture> futures;
    futures.push_back(std::move(splitFuture_));
    futures.push_back(std::move(dataFuture));
    *future = folly::collectAny(futures).unit();
    return BlockingReason::kWaitForSplit;
  }

  // Block until data becomes available.
  *future = std::move(dataFuture);
  return BlockingReason::kWaitForProducer;
}

bool HybridExchange::isFinished() {
  return atEnd_ && resultIsEmpty();
}

// local helper functions for converting the exchange specific format into
// a row vector.
namespace {
std::unique_ptr<folly::IOBuf> mergePages(
    const std::vector<std::unique_ptr<SerializedPage>>& pages) {
  VELOX_CHECK(!pages.empty());
  std::unique_ptr<folly::IOBuf> mergedBufs;
  for (const auto& page : pages) {
    if (mergedBufs == nullptr) {
      mergedBufs = page->getIOBuf();
    } else {
      mergedBufs->appendToChain(page->getIOBuf());
    }
  }
  return mergedBufs;
}
} // namespace

RowVectorPtr HybridExchange::getOutputFromPages(const SerPageVector* pages) {
  VELOX_CHECK_NOT_NULL(pages, "Pages shouldn't be null here.");
  auto* serde = getNamedVectorSerde(serdeKind_);
  RowVectorPtr result = nullptr;
  if (serde->supportsAppendInDeserialize()) {
    uint64_t rawInputBytes{0};
    if (pages->empty()) {
      return nullptr;
    }
    vector_size_t resultOffset = 0;
    for (const auto& page : *pages) {
      rawInputBytes += page->size();

      auto inputStream = page->prepareStreamForDeserialize();
      while (!inputStream->atEnd()) {
        serde->deserialize(
            inputStream.get(),
            pool(),
            outputType_,
            &result,
            resultOffset,
            serdeOptions_.get());
        resultOffset = result->size();
      }
    }
    emptyResult(); // release the memory in the pages vector.
    recordInputStats(rawInputBytes, result);
  } else if (serde->kind() == VectorSerde::Kind::kCompactRow) {
    result = getOutputFromCompactRows(serde, pages);
  } else if (serde->kind() == VectorSerde::Kind::kUnsafeRow) {
    result = getOutputFromUnsafeRows(serde, pages);
  } else {
    VELOX_UNREACHABLE(
        "Unsupported serde kind: {}", VectorSerde::kindName(serde->kind()));
  }
  if (!result) {
    return result;
  }
  // Convert the Velox row vector into a cudf vector.
  auto cudfFromVelox =
      std::make_shared<facebook::velox::cudf_velox::CudfFromVelox>(
          operatorId(), outputType_, operatorCtx_->driverCtx(), planNodeId());
  cudfFromVelox->addInput(result);
  cudfFromVelox->noMoreInput();

  return cudfFromVelox->getOutput();
}

RowVectorPtr HybridExchange::getOutputFromCompactRows(
    VectorSerde* serde,
    const SerPageVector* pages) {
  uint64_t rawInputBytes{0};
  if (pages->empty()) {
    VELOX_CHECK_NULL(compactRowInputStream_);
    VELOX_CHECK_NULL(compactRowIterator_);
    return nullptr;
  }

  if (compactRowInputStream_ == nullptr) {
    std::unique_ptr<folly::IOBuf> mergedBufs = mergePages(*pages);
    rawInputBytes += mergedBufs->computeChainDataLength();
    compactRowPages_ = std::make_unique<SerializedPage>(std::move(mergedBufs));
    compactRowInputStream_ = compactRowPages_->prepareStreamForDeserialize();
  }

  auto numRows = kInitialOutputCompactRows;
  if (estimatedCompactRowSize_.has_value()) {
    numRows = std::max(
        (preferredOutputBatchBytes_ / estimatedCompactRowSize_.value()),
        kInitialOutputCompactRows);
  }
  RowVectorPtr result = nullptr;
  serde->deserialize(
      compactRowInputStream_.get(),
      compactRowIterator_,
      numRows,
      outputType_,
      &result,
      pool(),
      serdeOptions_.get());
  const auto numOutputRows = result_->size();
  VELOX_CHECK_GT(numOutputRows, 0);

  estimatedCompactRowSize_ = std::max(
      result->estimateFlatSize() / numOutputRows,
      estimatedCompactRowSize_.value_or(1L));

  if (compactRowInputStream_->atEnd() && compactRowIterator_ == nullptr) {
    // only clear the input stream if we have reached the end of the row
    // iterator because row iterator may depend on input stream if serialized
    // rows are not compressed.
    compactRowInputStream_ = nullptr;
    compactRowPages_ = nullptr;
    emptyResult(); // empty current page vector.
  }

  recordInputStats(rawInputBytes, result);
  return result;
}

RowVectorPtr HybridExchange::getOutputFromUnsafeRows(
    VectorSerde* serde,
    const SerPageVector* pages) {
  uint64_t rawInputBytes{0};
  if (pages->empty()) {
    return nullptr;
  }
  RowVectorPtr result = nullptr;
  std::unique_ptr<folly::IOBuf> mergedBufs = mergePages(*pages);
  rawInputBytes += mergedBufs->computeChainDataLength();
  auto mergedPages = std::make_unique<SerializedPage>(std::move(mergedBufs));
  auto source = mergedPages->prepareStreamForDeserialize();
  serde->deserialize(
      source.get(), pool(), outputType_, &result, serdeOptions_.get());
  emptyResult(); // empty current page vector.
  recordInputStats(rawInputBytes, result);
  return result;
}

RowVectorPtr HybridExchange::getOutputFromTable(
    const TableWithStream* tableWithStream) {
  if (tableWithStream->table == nullptr) {
    return nullptr;
  }
  // The table is already unpacked - directly construct a CudfVector from it.
  // This avoids the expensive cudf::unpack() operation.
  // Use the stream from the queue (same stream used to allocate buffers).
  tableWithStream->stream.synchronize();

  auto numRows = tableWithStream->table->num_rows();

#if CHECKSUM_LOGGING
  // DEBUG: Compute checksums for all columns to detect corruption
  auto numCols = tableWithStream->table->num_columns();
  VLOG(3) << "@" << taskId() << " Dequeued table with "
          << numCols << " columns, " << numRows << " rows";

  // Compute checksums for each column in depth-first order
  size_t colIdx = 0;
  std::function<void(cudf::column_view)> logColumnChecksum =
      [&](cudf::column_view colView) {
        auto dataType = colView.type();
        auto size = colView.size();
        size_t currentColIdx = colIdx++;

        if (dataType.id() == cudf::type_id::STRING && size > 0 &&
            colView.num_children() > 0) {
          // STRING column - compute checksum of chars
          cudf::strings_column_view scv(colView);
          auto charsSize = scv.chars_size(tableWithStream->stream);
          uint64_t checksum = 0;
          if (charsSize > 0) {
            const void* charsPtr = scv.chars_begin(tableWithStream->stream);
            if (charsSize >= 16) {
              uint64_t first = 0, last = 0;
              cudaMemcpy(&first, charsPtr, std::min<size_t>(8, charsSize), cudaMemcpyDeviceToHost);
              cudaMemcpy(&last, static_cast<const uint8_t*>(charsPtr) + charsSize - 8, 8, cudaMemcpyDeviceToHost);
              checksum = first ^ last ^ charsSize;
            } else {
              cudaMemcpy(&checksum, charsPtr, std::min<size_t>(8, charsSize), cudaMemcpyDeviceToHost);
              checksum ^= charsSize;
            }
          }
          VLOG(1) << "[DEQUEUE-STR] @" << taskId()
                  << " chunk=" << tableWithStream->chunkSeq
                  << " part=" << tableWithStream->partition
                  << " col=" << currentColIdx
                  << " STRING: numStrings=" << size
                  << " charsSize=" << charsSize
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
          VLOG(1) << "[DEQUEUE-DATA] @" << taskId()
                  << " chunk=" << tableWithStream->chunkSeq
                  << " part=" << tableWithStream->partition
                  << " col=" << currentColIdx
                  << " type=" << static_cast<int>(dataType.id())
                  << " size=" << size
                  << " dataSize=" << dataSize
                  << " checksum=" << std::hex << checksum << std::dec;
        }

        // Recursively process children
        for (int i = 0; i < colView.num_children(); ++i) {
          logColumnChecksum(colView.child(i));
        }
      };

  for (cudf::size_type i = 0; i < numCols; ++i) {
    logColumnChecksum(tableWithStream->table->view().column(i));
  }
#endif

  // We need to move the table out of the variant, which requires a const_cast
  // since the variant gives us a const pointer. This is safe because we're
  // about to empty the result anyway.
  auto& mutableTableWithStream = const_cast<TableWithStream&>(*tableWithStream);
  auto tableSize = mutableTableWithStream.table->alloc_size();

  // outputType_ is declared in the Operator base class.
  auto result = std::make_shared<cudf_velox::CudfVector>(
      pool(),
      outputType_,
      numRows,
      std::move(mutableTableWithStream.table),
      mutableTableWithStream.stream);

  recordInputStats(tableSize, result);
  // free the memory and set it to nullptr;
  emptyResult();

  return result;
}

RowVectorPtr HybridExchange::getOutput() {
  const TableWithStream* tableWithStream = getResultTable();
  if (tableWithStream) {
    return getOutputFromTable(tableWithStream);
  }
  return getOutputFromPages(getResultPages());
}

void HybridExchange::recordInputStats(
    uint64_t rawInputBytes,
    RowVectorPtr result) {
  auto lockedStats = stats_.wlock();
  lockedStats->rawInputBytes += rawInputBytes;
  // size(): number of rows in result_ vector
  lockedStats->rawInputPositions += result->size();
  lockedStats->addInputVector(result->estimateFlatSize(), result->size());
}

void HybridExchange::close() {
  SourceOperator::close();
  emptyResult();
  bool usesHttp = false;
  if (exchangeClient_) {
    usesHttp = exchangeClient_->usesHttp_;
    recordExchangeClientStats();
    exchangeClient_->close();
  }
  exchangeClient_ = nullptr;
  if (usesHttp) {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        Operator::kShuffleSerdeKind,
        RuntimeCounter(static_cast<int64_t>(serdeKind_)));
    lockedStats->addRuntimeStat(
        Operator::kShuffleCompressionKind,
        RuntimeCounter(static_cast<int64_t>(serdeOptions_->compressionKind)));
  }
}

void HybridExchange::recordExchangeClientStats() {
  if (!processSplits_) {
    return;
  }

  auto lockedStats = stats_.wlock();
  const auto exchangeClientStats = exchangeClient_->stats();
  for (const auto& [name, value] : exchangeClientStats) {
    lockedStats->runtimeStats.erase(name);
    lockedStats->runtimeStats.insert({name, value});
  }

  auto backgroundCpuTimeMs =
      exchangeClientStats.find(ExchangeClient::kBackgroundCpuTimeMs);
  if (backgroundCpuTimeMs != exchangeClientStats.end()) {
    const CpuWallTiming backgroundTiming{
        static_cast<uint64_t>(backgroundCpuTimeMs->second.count),
        0,
        static_cast<uint64_t>(backgroundCpuTimeMs->second.sum) *
            Timestamp::kNanosecondsInMillisecond};
    lockedStats->backgroundTiming.clear();
    lockedStats->backgroundTiming.add(backgroundTiming);
  }
}

} // namespace facebook::velox::cudf_exchange
