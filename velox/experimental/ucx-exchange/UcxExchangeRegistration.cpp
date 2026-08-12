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

#include "velox/experimental/ucx-exchange/UcxExchangeRegistration.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/exec/OutputTransportRegistry.h"
#include "velox/experimental/ucx-exchange/UcxExchange.h"
#include "velox/experimental/ucx-exchange/UcxExchangeClient.h"
#include "velox/experimental/ucx-exchange/UcxOutputQueueManager.h"
#include "velox/experimental/ucx-exchange/UcxPartitionedOutput.h"

namespace facebook::velox::ucx_exchange {

void registerUcxExchange() {
  auto entry = std::make_shared<exec::ExchangeTransportEntry>();
  entry->makeClient = [](const exec::ExchangeClientContext& context)
      -> std::shared_ptr<exec::ExchangeClient> {
    return std::make_shared<UcxExchangeClient>(
        context.taskId, context.destination, context.numberOfConsumers);
  };
  entry->makeOperator =
      [](int32_t operatorId,
         exec::DriverCtx* ctx,
         const std::shared_ptr<const core::ExchangeNode>& node,
         std::shared_ptr<exec::ExchangeClient> client)
      -> std::unique_ptr<exec::Operator> {
    auto ucxClient =
        std::dynamic_pointer_cast<UcxExchangeClient>(std::move(client));
    VELOX_CHECK_NOT_NULL(
        ucxClient,
        "UCX exchange requires a UcxExchangeClient for transport: {}",
        core::TransportKind::kUcx);
    return std::make_unique<UcxExchange>(
        operatorId, ctx, node, std::move(ucxClient));
  };
  // 'overwrite' makes this idempotent: a second call (e.g. cuDF
  // re-initializing) replaces the existing entry instead of throwing on the
  // duplicate key.
  exec::ExchangeTransportRegistry::global().insert(
      std::string{core::TransportKind::kUcx},
      std::move(entry),
      /*overwrite=*/true);

  auto outputEntry = exec::OutputTransportEntry::make<UcxOutputQueueManager>(
      UcxOutputQueueManager::getInstanceRef(),
      [](int32_t operatorId,
         exec::DriverCtx* ctx,
         const std::shared_ptr<const core::PartitionedOutputNode>& node,
         bool eagerFlush,
         const std::shared_ptr<UcxOutputQueueManager>& /*manager*/)
          -> std::unique_ptr<exec::Operator> {
        // UcxPartitionedOutput fetches the manager itself via
        // UcxOutputQueueManager::getInstanceRef() (a process-wide singleton),
        // so the locked 'manager' handed in by OutputTransportEntry::make is
        // provably the same object and does not need to be threaded through.
        return std::make_unique<UcxPartitionedOutput>(
            operatorId, ctx, node, eagerFlush);
      });
  exec::OutputTransportRegistry::global().insert(
      std::string{core::TransportKind::kUcx},
      std::move(outputEntry),
      /*overwrite=*/true);
}

} // namespace facebook::velox::ucx_exchange
