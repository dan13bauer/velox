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
#include <cudf/null_mask.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/structs/structs_column_view.hpp>
#include <glog/logging.h>
#include <sstream>
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
    if (dataPtr_ && dataPtr_->sourceTable) {
      // Build per-column metadata from the table view for the receiver.
      // This uses the new TableMetadata format that contains ColumnMetadata
      // entries in depth-first order, which the receiver can use to
      // reconstruct the table from per-column buffer transfers.
      metadataMsg->cudfMetadata =
          TableMetadata::buildFromTable(dataPtr_->tableView);
      metadataMsg->dataSizeBytes = dataPtr_->gpuDataSize();
      metadataMsg->remainingBytes = {};
      metadataMsg->atEnd = false;
      metadataMsg->numColumns = dataPtr_->tableView.num_columns();
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
    if (dataPtr_ && dataPtr_->sourceTable) {
      sendStart_ = std::chrono::high_resolution_clock::now();

      auto numColumns = dataPtr_->tableView.num_columns();
      VLOG(3) << "@" << partitionKey_.taskId << " Sending table with "
              << numColumns << " columns and " << dataPtr_->tableView.num_rows()
              << " rows to task " << partitionKey_.toString()
              << ":" << this->sequenceNumber_ << " using per-column transfer";

      // Log column types being sent for debugging column order issues
      {
        std::stringstream ss;
        ss << "@" << partitionKey_.taskId << " CudfExchangeServer sending column types: [";
        for (cudf::size_type i = 0; i < numColumns; ++i) {
          if (i > 0) ss << ", ";
          ss << static_cast<int>(dataPtr_->tableView.column(i).type().id());
        }
        ss << "]";
        VLOG(3) << ss.str();
      }

      setState(ServerState::WaitingForSendComplete);

      // Each buffer gets a unique tag by incrementing the sequence number.
      // This ensures that UCXX matches send/receive pairs correctly.
      // bufferSeq starts at sequenceNumber_ and increments for each buffer.
      uint64_t bufferSeq = this->sequenceNumber_;

      dataRequests_.clear();
      bytes_ = 0;

      // Capture sourceTable to keep data alive during transfers.
      // All column_views reference data in sourceTable.
      auto sourceTable = dataPtr_->sourceTable;

      // First pass: count the number of buffers to send.
      // This is needed to pre-increment pendingDataRequests_ before any sends,
      // preventing race conditions where callbacks fire synchronously.
      size_t bufferCount = 0;
      std::function<void(cudf::column_view)> countBuffers =
          [&](cudf::column_view colView) {
            auto dataType = colView.type();
            auto size = colView.size();
            auto numChildren = colView.num_children();

            // Count STRING chars buffer
            if (dataType.id() == cudf::type_id::STRING && size > 0 &&
                numChildren > 0) {
              cudf::strings_column_view scv(colView);
              auto charsSize = scv.chars_size(rmm::cuda_stream_default);
              if (charsSize > 0) {
                ++bufferCount;
              }
            } else if (size > 0 && colView.head() != nullptr &&
                       cudf::is_fixed_width(dataType)) {
              ++bufferCount;
            }

            // Count null mask buffer
            // Only count if the column has nulls AND has a valid null mask pointer
            // Note: nullable() can be true but null_mask() nullptr if no nulls exist
            if (size > 0 && colView.null_mask() != nullptr) {
              ++bufferCount;
            }

            // Recursively count children
            // For STRING columns, skip the chars child (child 1) since chars are
            // sent via strings_column_view. Only process offsets child (child 0).
            // For STRUCT columns, use structs_column_view::get_sliced_child() to
            // get children with parent's offset/size applied (needed after split).
            if (dataType.id() == cudf::type_id::STRING) {
              if (numChildren > 0) {
                countBuffers(colView.child(0));  // offsets only
              }
            } else if (dataType.id() == cudf::type_id::STRUCT) {
              cudf::structs_column_view scv(colView);
              for (cudf::size_type i = 0; i < numChildren; ++i) {
                countBuffers(scv.get_sliced_child(i));
              }
            } else {
              for (cudf::size_type i = 0; i < numChildren; ++i) {
                countBuffers(colView.child(i));
              }
            }
          };

      for (cudf::size_type i = 0; i < numColumns; ++i) {
        countBuffers(dataPtr_->tableView.column(i));
      }

      // Pre-increment pendingDataRequests_ BEFORE any sends to prevent
      // race conditions where callbacks fire synchronously during tagSend.
      // This ensures the counter never hits 0 until all sends are initiated.
      pendingDataRequests_.store(bufferCount);
      numBuffersSent_ = bufferCount;

      VLOG(3) << "@" << partitionKey_.taskId << " Will send " << bufferCount
              << " buffers for table with " << numColumns << " columns";

      // If no buffers to send (e.g., empty table), complete immediately
      if (bufferCount == 0) {
        sendComplete(UCS_OK, nullptr);
      } else {
        // Second pass: send all buffers.
        // Send columns from the tableView. The sourceTable is captured in
        // callbacks to ensure the underlying data stays alive until all
        // transfers complete.
        // bufferSeq is captured by reference and incremented for each buffer.
        std::function<void(cudf::column_view)> sendColumnView =
            [&](cudf::column_view colView) {
              auto dataType = colView.type();
              auto size = colView.size();
              auto numChildren = colView.num_children();

              // Handle STRING columns specially - chars are accessed via
              // strings_column_view, not head()
              if (dataType.id() == cudf::type_id::STRING && size > 0 &&
                  numChildren > 0) {
                cudf::strings_column_view scv(colView);
                auto charsSize = scv.chars_size(rmm::cuda_stream_default);
                if (charsSize > 0) {
                  const void* charsPtr = scv.chars_begin(rmm::cuda_stream_default);
                  bytes_ += charsSize;

                  uint64_t tag = getDataTag(partitionKeyHash_, bufferSeq++);
                  VLOG(3) << "@" << partitionKey_.taskId
                          << " Sending STRING chars buffer "
                          << " size " << charsSize << " tag " << std::hex << tag
                          << std::dec;

                  std::weak_ptr<CudfExchangeServer> weakData = weak_from_this();
                  auto req = endpointRef_->endpoint_->tagSend(
                      const_cast<void*>(charsPtr),
                      charsSize,
                      ucxx::Tag{tag},
                      false,
                      [weakData, sourceTable](
                          ucs_status_t status, std::shared_ptr<void> arg) {
                        // sourceTable captured to keep data alive
                        if (auto self = weakData.lock()) {
                          size_t remaining =
                              self->pendingDataRequests_.fetch_sub(1) - 1;
                          if (remaining == 0) {
                            self->sendComplete(status, arg);
                          }
                        }
                      },
                      sourceTable);
                  dataRequests_.push_back(req);
                }
              } else if (size > 0 && colView.head() != nullptr &&
                         cudf::is_fixed_width(dataType)) {
                // Send data buffer for fixed-width types
                size_t dataSize = size * cudf::size_of(dataType);
                const void* dataPtr = static_cast<const void*>(
                    static_cast<const uint8_t*>(colView.head()) +
                    colView.offset() * cudf::size_of(dataType));
                bytes_ += dataSize;

                uint64_t tag = getDataTag(partitionKeyHash_, bufferSeq++);
                VLOG(3) << "@" << partitionKey_.taskId << " Sending data buffer "
                        << " size " << dataSize << " tag " << std::hex << tag
                        << std::dec;

                std::weak_ptr<CudfExchangeServer> weakData = weak_from_this();
                auto req = endpointRef_->endpoint_->tagSend(
                    const_cast<void*>(dataPtr),
                    dataSize,
                    ucxx::Tag{tag},
                    false,
                    [weakData, sourceTable](
                        ucs_status_t status, std::shared_ptr<void> arg) {
                      // sourceTable captured to keep data alive
                      if (auto self = weakData.lock()) {
                        size_t remaining =
                            self->pendingDataRequests_.fetch_sub(1) - 1;
                        if (remaining == 0) {
                          self->sendComplete(status, arg);
                        }
                      }
                    },
                    sourceTable); // sourceTable keeps data alive
                dataRequests_.push_back(req);
              }

              // Send null mask buffer if present
              // Only send if the column has a valid null mask pointer
              // Note: nullable() can be true but null_mask() nullptr if no nulls exist
              if (size > 0 && colView.null_mask() != nullptr) {
                // For split views, the null_mask pointer points to the original
                // buffer and offset() indicates where this slice's bits start.
                // We use copy_bitmask to extract just the bits for this slice
                // into a new contiguous buffer aligned at bit 0.
                auto nullMaskCopy = cudf::copy_bitmask(colView);
                size_t nullSize = nullMaskCopy.size();

                // Only send if the buffer is non-empty
                if (nullSize > 0) {
                  // Store the buffer to keep it alive during transfer
                  auto nullMaskPtr = std::make_shared<rmm::device_buffer>(
                      std::move(nullMaskCopy));
                  bytes_ += nullSize;

                  uint64_t tag = getDataTag(partitionKeyHash_, bufferSeq++);
                  VLOG(3) << "@" << partitionKey_.taskId
                          << " Sending null_mask buffer "
                          << " size " << nullSize << " tag " << std::hex << tag
                          << std::dec;

                  std::weak_ptr<CudfExchangeServer> weakData = weak_from_this();
                  auto req = endpointRef_->endpoint_->tagSend(
                      nullMaskPtr->data(),
                      nullSize,
                      ucxx::Tag{tag},
                      false,
                      [weakData, sourceTable, nullMaskPtr](
                          ucs_status_t status, std::shared_ptr<void> arg) {
                        // sourceTable and nullMaskPtr captured to keep data alive
                        if (auto self = weakData.lock()) {
                          size_t remaining =
                              self->pendingDataRequests_.fetch_sub(1) - 1;
                          if (remaining == 0) {
                            self->sendComplete(status, arg);
                          }
                        }
                      },
                      sourceTable); // sourceTable keeps data alive
                  dataRequests_.push_back(req);
                }
              }

              // Recursively process children (depth-first order)
              // For STRING columns, skip the chars child (child 1) since chars are
              // sent via strings_column_view. Only process offsets child (child 0).
              // For STRUCT columns, use structs_column_view::get_sliced_child() to
              // get children with parent's offset/size applied (needed after split).
              if (dataType.id() == cudf::type_id::STRING) {
                if (numChildren > 0) {
                  sendColumnView(colView.child(0));  // offsets only
                }
              } else if (dataType.id() == cudf::type_id::STRUCT) {
                cudf::structs_column_view scv(colView);
                for (cudf::size_type i = 0; i < numChildren; ++i) {
                  sendColumnView(scv.get_sliced_child(i));
                }
              } else {
                for (cudf::size_type i = 0; i < numChildren; ++i) {
                  sendColumnView(colView.child(i));
                }
              }
            };

        // Send all columns from the tableView
        for (cudf::size_type i = 0; i < numColumns; ++i) {
          sendColumnView(dataPtr_->tableView.column(i));
        }

        VLOG(3) << "@" << partitionKey_.taskId << " Initiated "
                << numBuffersSent_ << " buffer sends, total "
                << bytes_ << " bytes";
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

    // Increment sequence number by the number of buffers sent (each buffer used one sequence)
    // For empty tables, increment by at least 1 to advance the sequence
    this->sequenceNumber_ += std::max(numBuffersSent_, static_cast<size_t>(1));
    dataPtr_.reset(); // release TableWithMetadata (sourceTable ref count decrements)
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
