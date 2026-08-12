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
#include "velox/experimental/ucx-exchange/UcxMergeSource.h"

#include "velox/exec/Merge.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

namespace facebook::velox::ucx_exchange {

UcxMergeSource::UcxMergeSource(
    exec::MergeExchange* mergeExchange,
    const std::string& taskId,
    std::shared_ptr<UcxExchangeClient> client)
    : mergeExchange_(mergeExchange), client_(std::move(client)) {
  VELOX_CHECK_NOT_NULL(mergeExchange_);
  VELOX_CHECK_NOT_NULL(client_);
  client_->addRemoteTaskId(taskId);
  client_->noMoreRemoteTasks();
}

UcxMergeSource::~UcxMergeSource() {
  close();
}

exec::BlockingReason UcxMergeSource::started(ContinueFuture* /*future*/) {
  VELOX_NYI();
}

exec::BlockingReason UcxMergeSource::next(
    RowVectorPtr& data,
    ContinueFuture* future,
    bool& drained) {
  drained = false;
  data.reset();
  if (atEnd_) {
    return exec::BlockingReason::kNotBlocked;
  }

  auto packedTable = client_->next(kConsumerId, &atEnd_, future);
  if (packedTable == nullptr) {
    // Either the source is done, or 'future' completes when the next table
    // arrives.
    return atEnd_ ? exec::BlockingReason::kNotBlocked
                  : exec::BlockingReason::kWaitForProducer;
  }

  const auto gpuDataSize = packedTable->gpuDataSize();
  const auto numRows = packedTable->packedTable->table.num_rows();
  // Same conversion as UcxExchange::getOutputFromPackedTable(): the stream
  // UcxExchangeSource allocated plus the packed_table constructor of CudfVector
  // wrap the received data without copying it off the device. Only the stats
  // bookkeeping differs -- this source is not that operator, so it reports
  // through the merge operator's stats below.
  data = std::make_shared<cudf_velox::CudfVector>(
      mergeExchange_->pool(),
      mergeExchange_->outputType(),
      numRows,
      std::move(packedTable->packedTable),
      packedTable->stream);

  auto lockedStats = mergeExchange_->stats().wlock();
  lockedStats->rawInputBytes += gpuDataSize;
  lockedStats->rawInputPositions += data->size();
  lockedStats->addInputVector(data->estimateFlatSize(), data->size());
  return exec::BlockingReason::kNotBlocked;
}

exec::BlockingReason UcxMergeSource::enqueue(
    RowVectorPtr /*input*/,
    ContinueFuture* /*future*/,
    bool /*drained*/) {
  VELOX_FAIL("UcxMergeSource does not accept enqueued data");
}

void UcxMergeSource::close() {
  if (client_) {
    client_->close();
    client_ = nullptr;
  }
}

} // namespace facebook::velox::ucx_exchange
