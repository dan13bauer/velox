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

#include "velox/exec/MergeSource.h"
#include "velox/experimental/ucx-exchange/UcxExchangeClient.h"

namespace facebook::velox::exec {
class MergeExchange;
} // namespace facebook::velox::exec

namespace facebook::velox::ucx_exchange {

/// Reads the output of one remote task over UCX on behalf of a MergeExchange
/// operator, the UCX counterpart of the in-memory merge exchange source. The
/// UCX receive path delivers whole cudf packed tables, so each next() call
/// hands the merge operator one table wrapped as a CudfVector rather than
/// deserializing a page incrementally.
class UcxMergeSource : public exec::MergeSource {
 public:
  /// Takes over 'client' and registers 'taskId' as its only remote task. The
  /// merge path creates one client per remote source, so this source owns that
  /// client's task list, like the in-memory merge exchange source does.
  UcxMergeSource(
      exec::MergeExchange* mergeExchange,
      const std::string& taskId,
      std::shared_ptr<UcxExchangeClient> client);

  ~UcxMergeSource() override;

  /// No-op: a UCX producer pushes data without waiting for a start signal.
  void start() override {}

  /// Never called on an exchange source; the start signal only applies to
  /// local merge sources.
  exec::BlockingReason started(ContinueFuture* future) override;

  /// Returns the next packed table as a CudfVector, or leaves 'data' null and
  /// returns kWaitForProducer with 'future' set when more data is expected.
  /// Returns kNotBlocked with 'data' null once the source is at end. 'drained'
  /// is always false: barrier processing does not apply to UCX exchange.
  exec::BlockingReason
  next(RowVectorPtr& data, ContinueFuture* future, bool& drained) override;

  /// Always fails: data reaches this source through the UCX queue, not through
  /// a producer driver.
  exec::BlockingReason
  enqueue(RowVectorPtr input, ContinueFuture* future, bool drained) override;

  /// Closes the client. Idempotent, and also called by the destructor.
  void close() override;

 private:
  // Consumer id passed to every client_->next() call. The merge path builds
  // its clients with a single consumer.
  static constexpr int kConsumerId{0};

  // The operator this source feeds. Owns the output type, memory pool and
  // stats this source reports through, and outlives the source.
  exec::MergeExchange* const mergeExchange_;

  // Null once close() has run.
  std::shared_ptr<UcxExchangeClient> client_;

  // True once the client reported that no more data is expected.
  bool atEnd_{false};
};

} // namespace facebook::velox::ucx_exchange
