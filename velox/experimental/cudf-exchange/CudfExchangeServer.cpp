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
#include "velox/experimental/cudf-exchange/CudfExchangeServer.h"
#include <cudf/contiguous_split.hpp>
#include <glog/logging.h>
#include "cuda_runtime.h"
#include "velox/experimental/cudf-exchange/Communicator.h"
#include "velox/experimental/cudf-exchange/CudfExchangeProtocol.h"
#include "velox/experimental/cudf/exec/Utilities.h"

namespace facebook::velox::cudf_exchange {

// This constructor is private
CudfExchangeServer::CudfExchangeServer(
    const std::shared_ptr<Communicator> communicator,
    std::shared_ptr<EndpointRef> endpointRef,
    const PartitionKey& key)
    : CommElement(communicator, endpointRef),
      partitionKey_(key),
      partitionKeyHash_(fnv1a_32(partitionKey_.toString())),
      queueMgr_(CudfOutputQueueManager::getInstanceRef()) {
  setState(ServerState::Created);
}

// static
std::shared_ptr<CudfExchangeServer> CudfExchangeServer::create(
    const std::shared_ptr<Communicator> communicator,
    std::shared_ptr<EndpointRef> endpointRef,
    const PartitionKey& key) {
  auto ptr = std::shared_ptr<CudfExchangeServer>(
      new CudfExchangeServer(communicator, endpointRef, key));
  return ptr;
}

void CudfExchangeServer::process() {
  // Check if close() was called - avoid processing if we're shutting down
  if (closed_.load(std::memory_order_acquire)) {
    return;
  }
  switch (state_) {
    case ServerState::Created:
      setState(ServerState::ReadyToTransfer);
      communicator_->addToWorkQueue(getSelfPtr());
      break;
    case ServerState::ReadyToTransfer: {
      // Fetch the data from CudfQueueManager and store it in the dataPtr_;
      setState(ServerState::WaitingForDataFromQueue);
      // Register the callback with the destination queue to get data.
      // If the queue doesn't exist yet, getData will create an empty
      // queue and the callback will be triggered once the corresponding
      // source task has initialized the queue and added data to it.
      // Use weak_ptr to prevent use-after-free if close() is called during callback
      std::weak_ptr<CudfExchangeServer> weakQueue = weak_from_this();
      queueMgr_->getData(
          partitionKey_.taskId,
          partitionKey_.destination,
          [weakQueue](
              std::unique_ptr<TableWithMetadata> data,
              std::vector<int64_t> remainingBytes) {
            auto self = weakQueue.lock();
            if (!self) {
              return; // Object was destroyed, safe to ignore
            }
            // Check if close() was called - avoid processing if we're shutting down
            if (self->closed_.load(std::memory_order_acquire)) {
              VLOG(3) << "@" << self->partitionKey_.taskId
                      << " getData callback called after close, ignoring";
              return;
            }
            // This upcall may be called from another thread than the
            // communicator thread. It is called
            // when data on the queue becomes available.
            VLOG(3) << "@" << self->partitionKey_.taskId
                    << " Found data for client: "
                    << self->partitionKey_.toString();
            std::lock_guard<std::recursive_mutex> lock(self->dataMutex_);
            VELOX_CHECK(
                self->dataPtr_ == nullptr,
                "Data pointer exists: Illegal state!");
            self->dataPtr_ = std::move(data);
            self->setState(ServerState::DataReady);
            self->communicator_->addToWorkQueue(self);
          });
      this->communicator_->addToWorkQueue(getSelfPtr());
    } break;
    case ServerState::WaitingForDataFromQueue:
      // Waiting for data is handled by an upcall from the data queue. Nothing
      // to do
      break;
    case ServerState::DataReady:
      sendData();
      break;
    case ServerState::WaitingForSendComplete:
      // Waiting for send complete is handled by an upcall from UCXX. Nothing to
      // do
      break;
    case ServerState::Done:
      close();
      if (endpointRef_) {
        endpointRef_->removeCommElem(getSelfPtr());
        endpointRef_ = nullptr;
      }
      break;
  };
}

void CudfExchangeServer::close() {
  // Use memory_order_acq_rel to ensure proper synchronization with callbacks
  // that check closed_ with memory_order_acquire.
  bool expected = false;
  bool desired = true;
  if (!closed_.compare_exchange_strong(
          expected, desired, std::memory_order_acq_rel)) {
    return; // already closed.
  }
  VLOG(3) << "@" << partitionKey_.taskId
          << " Close CudfExchangeServer to remote " << partitionKey_.toString();

  // Cancel any outstanding requests. With weak_ptr callbacks, the callbacks
  // will safely no-op if we're destroyed before they complete.
  if (metaRequest_ && !metaRequest_->isCompleted()) {
    metaRequest_->cancel();
  }
  for (auto& req : dataRequests_) {
    if (req && !req->isCompleted()) {
      req->cancel();
    }
  }

  communicator_->unregister(getSelfPtr());
}

std::string CudfExchangeServer::toString() {
  std::stringstream out;
  out << "[ExSrv " << partitionKey_.toString() << " - " << sequenceNumber_
      << "]";
  return out.str();
}

// ------ private methods ---------

std::shared_ptr<CudfExchangeServer> CudfExchangeServer::getSelfPtr() {
  return shared_from_this();
}

void CudfExchangeServer::sendData() {
  // Create the MetaDataRecord.
  std::shared_ptr<MetadataMsg> metadataMsg = std::make_shared<MetadataMsg>();

  {
    std::lock_guard<std::recursive_mutex> lock(dataMutex_);
    if (dataPtr_) {
      // Build per-column metadata from the table view for the receiver.
      // This uses the new TableMetadata format that contains ColumnMetadata
      // entries in depth-first order, which the receiver can use to
      // reconstruct the table from per-column buffer transfers.
      metadataMsg->cudfMetadata =
          TableMetadata::buildFromTable(dataPtr_->table->view());
      metadataMsg->dataSizeBytes = dataPtr_->gpuDataSize();
      metadataMsg->remainingBytes = {};
      metadataMsg->atEnd = false;
      metadataMsg->numColumns = dataPtr_->table->num_columns();
    } else {
      VLOG(3) << "@" << partitionKey_.taskId << " Final exchange for "
              << partitionKey_.toString();
      metadataMsg->cudfMetadata = nullptr;
      metadataMsg->dataSizeBytes = 0;
      metadataMsg->remainingBytes = {};
      metadataMsg->atEnd = true;
      metadataMsg->numColumns = 0;
    }
  }

  auto [serializedMetadata, serMetaSize] = metadataMsg->serialize();

  // send metadata.
  uint64_t metadataTag =
      getMetadataTag(this->partitionKeyHash_, this->sequenceNumber_);
  // Use weak_ptr to prevent use-after-free if close() is called during callback
  std::weak_ptr<CudfExchangeServer> weakMeta = weak_from_this();
  metaRequest_ = endpointRef_->endpoint_->tagSend(
      serializedMetadata.get(),
      serMetaSize,
      ucxx::Tag{metadataTag},
      false,
      [tid = partitionKey_.toString(), metadataTag, weakMeta](
          ucs_status_t status, std::shared_ptr<void> arg) {
        auto self = weakMeta.lock();
        if (!self) {
          return; // Object was destroyed, safe to ignore
        }
        // Check if close() was called - avoid processing if we're shutting down
        if (self->closed_.load(std::memory_order_acquire)) {
          VLOG(3) << "@" << self->partitionKey_.taskId
                  << " metadata send callback called after close, ignoring";
          return;
        }
        if (status == UCS_OK) {
          VLOG(3) << "@" << self->partitionKey_.taskId
                  << " metadata successfully sent to " << tid
                  << " with tag: " << std::hex << metadataTag;
        } else {
          VLOG(0) << "@" << self->partitionKey_.taskId
                  << " Error in sendData, send metadata "
                  << ucs_status_string(status) << " failed for task: " << tid;
          self->setState(ServerState::Done);
          self->communicator_->addToWorkQueue(self);
        }
      },
      serializedMetadata);

  // Send data per-column without packing
  {
    std::lock_guard<std::recursive_mutex> lock(dataMutex_);
    if (dataPtr_ && dataPtr_->table) {
      sendStart_ = std::chrono::high_resolution_clock::now();

      auto numColumns = dataPtr_->table->num_columns();
      VLOG(3) << "@" << partitionKey_.taskId << " Sending table with "
              << numColumns << " columns to task " << partitionKey_.toString()
              << ":" << this->sequenceNumber_ << " using per-column transfer";

      setState(ServerState::WaitingForSendComplete);

      // Base tag for data - we'll add offsets for each column/buffer
      uint64_t baseDataTag =
          getDataTag(this->partitionKeyHash_, this->sequenceNumber_);

      dataRequests_.clear();
      bytes_ = 0;

      // Release ownership of all columns and send their buffers
      auto columns = dataPtr_->table->release();

      // Store released column contents to keep buffers alive until send completes
      auto releasedContents =
          std::make_shared<std::vector<cudf::column::contents>>();
      releasedContents->reserve(numColumns);

      // We send buffers in depth-first order to match metadata order.
      // Tag encoding: baseDataTag + (columnIndex * 2) for data,
      //               baseDataTag + (columnIndex * 2) + 1 for null_mask
      uint32_t bufferIndex = 0;

      std::function<void(std::unique_ptr<cudf::column>)> sendColumn =
          [&](std::unique_ptr<cudf::column> col) {
            if (!col) {
              return;
            }

            // Get column info before release
            auto dataType = col->type();
            auto size = col->size();
            auto nullCount = col->null_count();
            auto numChildren = col->num_children();

            // Release the column to get its raw buffers
            auto contents = col->release();

            // Send data buffer if present
            if (contents.data && contents.data->size() > 0) {
              uint64_t dataTag = baseDataTag + (bufferIndex * 2);
              auto dataSize = contents.data->size();
              bytes_ += dataSize;

              VLOG(3) << "@" << partitionKey_.taskId << " Sending data buffer "
                      << bufferIndex << " size " << dataSize << " tag "
                      << std::hex << dataTag << std::dec;

              // Keep buffer alive - move to shared container
              auto dataBuffer = std::shared_ptr<rmm::device_buffer>(
                  contents.data.release());

              std::weak_ptr<CudfExchangeServer> weakData = weak_from_this();
              auto req = endpointRef_->endpoint_->tagSend(
                  dataBuffer->data(),
                  dataBuffer->size(),
                  ucxx::Tag{dataTag},
                  false,
                  [weakData, dataBuffer](
                      ucs_status_t status, std::shared_ptr<void> arg) {
                    if (auto self = weakData.lock()) {
                      size_t remaining =
                          self->pendingDataRequests_.fetch_sub(1) - 1;
                      if (remaining == 0) {
                        self->sendComplete(status, arg);
                      }
                    }
                  });
              dataRequests_.push_back(req);
              pendingDataRequests_.fetch_add(1);
            }

            // Send null mask buffer if present
            if (contents.null_mask && contents.null_mask->size() > 0) {
              uint64_t nullTag = baseDataTag + (bufferIndex * 2) + 1;
              auto nullSize = contents.null_mask->size();
              bytes_ += nullSize;

              VLOG(3) << "@" << partitionKey_.taskId
                      << " Sending null_mask buffer " << bufferIndex << " size "
                      << nullSize << " tag " << std::hex << nullTag << std::dec;

              // Keep buffer alive
              auto nullBuffer = std::shared_ptr<rmm::device_buffer>(
                  contents.null_mask.release());

              std::weak_ptr<CudfExchangeServer> weakData = weak_from_this();
              auto req = endpointRef_->endpoint_->tagSend(
                  nullBuffer->data(),
                  nullBuffer->size(),
                  ucxx::Tag{nullTag},
                  false,
                  [weakData, nullBuffer](
                      ucs_status_t status, std::shared_ptr<void> arg) {
                    if (auto self = weakData.lock()) {
                      size_t remaining =
                          self->pendingDataRequests_.fetch_sub(1) - 1;
                      if (remaining == 0) {
                        self->sendComplete(status, arg);
                      }
                    }
                  });
              dataRequests_.push_back(req);
              pendingDataRequests_.fetch_add(1);
            }

            bufferIndex++;

            // Recursively process children (depth-first order)
            for (auto& child : contents.children) {
              sendColumn(std::move(child));
            }
          };

      // Send all columns
      for (auto& col : columns) {
        sendColumn(std::move(col));
      }

      VLOG(3) << "@" << partitionKey_.taskId << " Initiated "
              << pendingDataRequests_.load() << " buffer sends, total "
              << bytes_ << " bytes";

      // If no buffers to send (e.g., empty table), complete immediately
      if (pendingDataRequests_.load() == 0) {
        sendComplete(UCS_OK, nullptr);
      }
    } else {
      // Data pointer is null, so no more data will be coming.
      VLOG(3) << "@" << partitionKey_.taskId
              << " Finished transferring partition for task "
              << partitionKey_.toString();
      queueMgr_->deleteResults(partitionKey_.taskId, partitionKey_.destination);
      setState(ServerState::Done);
      communicator_->addToWorkQueue(getSelfPtr());
    }
  }
}

void CudfExchangeServer::sendComplete(
    ucs_status_t status,
    std::shared_ptr<void> arg) {
  // Check if close() was called - avoid processing if we're shutting down
  if (closed_.load(std::memory_order_acquire)) {
    VLOG(3) << "@" << partitionKey_.taskId
            << " sendComplete called after close, ignoring";
    return;
  }
  if (status == UCS_OK) {
    std::lock_guard<std::recursive_mutex> lock(dataMutex_);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = end - sendStart_;
    auto micros =
        std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    auto throughput = micros > 0 ? bytes_ / micros : 0;

    VLOG(3) << "@" << partitionKey_.taskId << " duration: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(duration)
                   .count()
            << " ms ";
    VLOG(3) << "@" << partitionKey_.taskId << " throughput: " << throughput
            << " MByte/s";

    this->sequenceNumber_++;
    dataPtr_.reset(); // release memory (table columns already released)
    dataRequests_.clear(); // clear request pointers
    VLOG(3) << "@" << partitionKey_.taskId
            << " Releasing dataPtr_ in sendComplete.";
    setState(ServerState::ReadyToTransfer);
  } else {
    VLOG(3) << "@" << partitionKey_.taskId
            << " Error in sendComplete, send complete "
            << ucs_status_string(status);
    setState(ServerState::Done);
  }
  communicator_->addToWorkQueue(getSelfPtr());
}

} // namespace facebook::velox::cudf_exchange
