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

#include <thread>

#include <cudf/column/column.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/structs/structs_column_view.hpp>
#include <cudf/table/table.hpp>
#include <folly/String.h>
#include <folly/Uri.h>
#include "velox/experimental/cudf-exchange/CudfExchangeSource.h"
#include "velox/experimental/cudf/exec/Utilities.h"

using namespace facebook::velox::exec;
namespace facebook::velox::cudf_exchange {

// This constructor is private.
CudfExchangeSource::CudfExchangeSource(
    const std::shared_ptr<Communicator> communicator,
    const std::string& taskId,
    const std::string& host,
    uint16_t port,
    const PartitionKey& partitionKey,
    const std::shared_ptr<CudfExchangeQueue> queue)
    : CommElement(communicator),
      host_(host),
      port_(port),
      taskId_(taskId),
      partitionKey_(partitionKey),
      partitionKeyHash_(fnv1a_32(partitionKey_.toString())),
      queue_(std::move(queue)) {
  setState(ReceiverState::Created);
}

/*static*/
std::shared_ptr<CudfExchangeSource> CudfExchangeSource::create(
    const std::string& taskId,
    const std::string& url,
    const std::shared_ptr<CudfExchangeQueue>& queue) {
  folly::Uri uri(url);
  // Note that there is no distinct schema for the UCXX exchange.
  // The approach is to ignore the schema and not check for HTTP or HTTPS.
  // FIXME: Can't use the HTTP port as this conflicts with Prestissimo!
  // For the time being, there's an ugly hack that just increases the port by 3.
  const std::string host = uri.host();
  int port = uri.port() + 3;
  std::shared_ptr<Communicator> communicator = Communicator::getInstance();
  auto key = extractTaskAndDestinationId(uri.path());
  auto source = std::shared_ptr<CudfExchangeSource>(
      new CudfExchangeSource(communicator, taskId, host, port, key, queue));
  // register the exchange source with the communicator. This makes sure that
  // "progress" is called.
  communicator->registerCommElement(source);
  VLOG(3) << source->toString() << " creating CudfExchangeSource for url: " << url;
  return source;
}

void CudfExchangeSource::process() {
  if (closed_) {
    // Driver thread called closed
    cleanUp();
    return;
  }

  switch (state_) {
    case ReceiverState::Created: {
      // Get the endpoint.
      HostPort hp{host_, port_};
      std::shared_ptr<CudfExchangeSource> selfPtr = getSelfPtr();
      auto epRef = communicator_->assocEndpointRef(selfPtr, hp);
      if (epRef) {
        setEndpoint(epRef);
        setStateIf(
            ReceiverState::Created, ReceiverState::WaitingForHandshakeComplete);
        sendHandshake();
      } else {
        // connection failed.
        VLOG(0) << toString() << " Failed to connect to " << host_ << ":"
                << std::to_string(port_);
        setState(ReceiverState::Done);
      }
      communicator_->addToWorkQueue(getSelfPtr());
    } break;
    case ReceiverState::WaitingForHandshakeComplete:
      // Waiting for metadata is handled by an upcall from UCXX. Nothing to do
      break;
    case ReceiverState::ReadyToReceive:
      // change state before calling getMetadata since immediate upcalls in
      // getMetadata will also change state.
      setStateIf(
          ReceiverState::ReadyToReceive, ReceiverState::WaitingForMetadata);
      getMetadata();
      break;
    case ReceiverState::WaitingForMetadata:
      // Waiting for metadata is handled by an upcall from UCXX. Nothing to do
      break;
    case ReceiverState::WaitingForData:
      // Waiting for data is handled by an upcall from UCXX. Nothing to do.
      break;
    case ReceiverState::Done:
      // We need to call clean-up in this thread to remove any state
      cleanUp();
      break;
  }
}

void CudfExchangeSource::cleanUp() {
  uint32_t value = static_cast<uint32_t>(getState());
  if (value != static_cast<uint32_t>(ReceiverState::Done)) {
    // Unexpected cleanup
    VLOG(3) << toString() << " In CudfExchangeSource::cleanUp state == "
            << value;
  }

  // Cancel any outstanding request. With weak_ptr callbacks, the callback
  // will safely no-op if we're destroyed before it completes.
  if (request_ && !request_->isCompleted()) {
    // The Task has failed and we may need to cancel outstanding requests
    request_->cancel();
  }

  if (endpointRef_) {
    endpointRef_->removeCommElem(getSelfPtr());
    endpointRef_ = nullptr;
  }
  if (communicator_) {
    communicator_->unregister(getSelfPtr());
  }
}

void CudfExchangeSource::close() {
  // This is called by the driver thread so we need to be careful to
  // indicate to the process thread that we are closing and
  // let it do the actual cleaning up.

  // Use memory_order_acq_rel to ensure proper synchronization with callbacks
  // that check closed_ with memory_order_acquire.
  bool expected = false;
  bool desired = true;
  if (!closed_.compare_exchange_strong(
          expected, desired, std::memory_order_acq_rel)) {
    return; // already closed.
  }

  VLOG(1) << toString() << " CudfExchangeSource::close called.";

  // Let the Communicator progress thread do the actual clean-up
  setState(ReceiverState::Done);
  communicator_->addToWorkQueue(getSelfPtr());
}

folly::F14FastMap<std::string, int64_t> CudfExchangeSource::stats() const {
  VELOX_UNREACHABLE();
}

folly::F14FastMap<std::string, RuntimeMetric> CudfExchangeSource::metrics()
    const {
  folly::F14FastMap<std::string, RuntimeMetric> map;

  // these metrics will be aggregated over all exchange sources of the same
  // exchange client.
  map["cudfExchangeSource.numPackedColumns"] = metrics_.numPackedColumns_;
  map["cudfExchangeSource.totalBytes"] = metrics_.totalBytes_;
  map["cudfExchangeSource.rttPerRequest"] = metrics_.rttPerRequest_;
  return map;
}

// private methods ---
PartitionKey CudfExchangeSource::extractTaskAndDestinationId(
    const std::string& path) {
  // The URL path has the form: /v1/task/<taskId>/results/<destinationId>"
  std::vector<folly::StringPiece> components;
  folly::split('/', path, components, true);

  VELOX_CHECK_EQ(components[0], "v1");
  VELOX_CHECK_EQ(components[1], "task");
  VELOX_CHECK_EQ(components[3], "results");

  uint32_t destinationId;
  try {
    destinationId = static_cast<uint32_t>(std::stoul(components[4].str()));
  } catch (const std::exception& e) {
    std::string msg = "Illegal destination in task URL: " + path;
    VELOX_UNSUPPORTED(msg);
  }

  return PartitionKey{components[2].str(), destinationId};
}

std::shared_ptr<CudfExchangeSource> CudfExchangeSource::getSelfPtr() {
  std::shared_ptr<CudfExchangeSource> ptr;
  try {
    ptr = shared_from_this();
  } catch (std::bad_weak_ptr& exp) {
    ptr = nullptr;
  }
  return ptr;
}

void CudfExchangeSource::enqueue(
    std::unique_ptr<cudf::table> table,
    MetadataMsg& metadata) {
  std::vector<velox::ContinuePromise> queuePromises;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());

    queue_->enqueueLocked(std::move(table), queuePromises);
  }
  // wake up consumers of the CudfExchangeQueue
  for (auto& promise : queuePromises) {
    promise.setValue();
  }
}

void CudfExchangeSource::setEndpoint(std::shared_ptr<EndpointRef> endpointRef) {
  endpointRef_ = std::move(endpointRef);
}

void CudfExchangeSource::sendHandshake() {
  std::shared_ptr<HandshakeMsg> handshakeReq = std::make_shared<HandshakeMsg>();
  handshakeReq->destination = partitionKey_.destination;
  strncpy(
      handshakeReq->taskId,
      partitionKey_.taskId.c_str(),
      sizeof(handshakeReq->taskId));

  VLOG(3) << toString() << " Sending handshake with initial value: "
          << partitionKey_.toString() << " to server";

  // Create the handshake which will register client's existence with the server
  ucxx::AmReceiverCallbackInfo info(
      communicator_->kAmCallbackOwner, communicator_->kAmCallbackId);
  // Use weak_ptr to prevent use-after-free if close() is called during callback
  std::weak_ptr<CudfExchangeSource> weak = weak_from_this();
  request_ = endpointRef_->endpoint_->amSend(
      handshakeReq.get(),
      sizeof(HandshakeMsg),
      UCS_MEMORY_TYPE_HOST,
      info,
      false,
      [weak](ucs_status_t status, std::shared_ptr<void> arg) {
        if (auto self = weak.lock()) {
          self->onHandshake(status, arg);
        }
      },
      handshakeReq);
}

void CudfExchangeSource::onHandshake(
    ucs_status_t status,
    std::shared_ptr<void> arg) {
  // Check if close() was called - avoid processing if we're shutting down
  if (closed_.load(std::memory_order_acquire)) {
    VLOG(3) << toString() << " onHandshake called after close, ignoring";
    return;
  }
  if (status != UCS_OK) {
    std::string errorMsg = fmt::format(
        "Failed to send handshake to host {}:{}, task {}: {}",
        host_,
        port_,
        partitionKey_.toString(),
        ucs_status_string(status));
    VLOG(0) << errorMsg;
    setState(ReceiverState::Done);
    queue_->setError(errorMsg); // Let the operator know via the queue

  } else {
    VLOG(3) << toString() << "+ onHandshake " << ucs_status_string(status);
    setStateIf(
        ReceiverState::WaitingForHandshakeComplete,
        ReceiverState::ReadyToReceive);
  }
  // more work to do
  communicator_->addToWorkQueue(getSelfPtr());
}

void CudfExchangeSource::getMetadata() {
  uint32_t sizeMetadata = 4096; // shouldn't be a fixed size.
  auto metadataReq = std::make_shared<std::vector<uint8_t>>(sizeMetadata);
  uint64_t metadataTag = getMetadataTag(partitionKeyHash_, sequenceNumber_);

  VLOG(3) << toString()
          << " waiting for metadata for chunk: " << sequenceNumber_
          << " using tag: " << std::hex << metadataTag << std::dec;

  // Use weak_ptr to prevent use-after-free if close() is called during callback
  std::weak_ptr<CudfExchangeSource> weak = weak_from_this();
  request_ = endpointRef_->endpoint_->tagRecv(
      reinterpret_cast<void*>(metadataReq->data()),
      sizeMetadata,
      ucxx::Tag{metadataTag},
      ucxx::TagMaskFull,
      false,
      [weak](ucs_status_t status, std::shared_ptr<void> arg) {
        if (auto self = weak.lock()) {
          self->onMetadata(status, arg);
        }
      },
      metadataReq);
}

void CudfExchangeSource::onMetadata(
    ucs_status_t status,
    std::shared_ptr<void> arg) {
  // Check if close() was called - avoid processing if we're shutting down
  if (closed_.load(std::memory_order_acquire)) {
    VLOG(3) << toString() << " onMetadata called after close, ignoring";
    return;
  }
  VLOG(3) << toString() << " + onMetadata " << ucs_status_string(status);

  if (status != UCS_OK) {
    std::string errorMsg = fmt::format(
        "Failed to receive metadata from host {}:{}, task {}: {}",
        host_,
        port_,
        partitionKey_.toString(),
        ucs_status_string(status));
    VLOG(0) << errorMsg;
    setState(ReceiverState::Done);
    queue_->setError(errorMsg); // Let the operator know via the queue
    communicator_->addToWorkQueue(getSelfPtr());
  } else {
    VELOX_CHECK(arg != nullptr, "Didn't get metadata");

    // arg contains the actual serialized metadata, deserialize the metadata
    std::shared_ptr<std::vector<uint8_t>> metadataMsg =
        std::static_pointer_cast<std::vector<uint8_t>>(arg);

    auto ptr = std::make_shared<DataAndMetadata>();

    ptr->metadata =
        std::move(MetadataMsg::deserializeMetadataMsg(metadataMsg->data()));

    VLOG(3) << toString() << " Datasize bytes == "
            << ptr->metadata.dataSizeBytes;

    if (ptr->metadata.atEnd) {
      // It seems that all data has been transferred
      atEnd_ = true;
      // enqueue a nullpointer to mark the end for this source.
      VLOG(3) << "There is no more data to transfer for " << toString();
      setStateIf(ReceiverState::WaitingForMetadata, ReceiverState::Done);
      communicator_->addToWorkQueue(getSelfPtr());
      enqueue(nullptr, ptr->metadata); // nullptr table marks end
      // jump out of this function.
      return;
    }

    VLOG(3) << toString() << " Expecting " << ptr->metadata.numColumns
            << " columns using per-column transfer";

    // Parse the per-column metadata from cudfMetadata
    std::vector<ColumnMetadata> columnMetadata;
    if (ptr->metadata.cudfMetadata && !ptr->metadata.cudfMetadata->empty()) {
      columnMetadata = TableMetadata::deserialize(*ptr->metadata.cudfMetadata);
    }

    if (columnMetadata.empty()) {
      VLOG(0) << toString() << " No column metadata, cannot receive data";
      queue_->setError("No column metadata in message");
      setState(ReceiverState::Done);
      communicator_->addToWorkQueue(getSelfPtr());
      return;
    }

    // Create a structure to hold all the per-column buffers
    struct PerColumnData {
      MetadataMsg metadata;
      std::vector<ColumnMetadata> columnMeta;
      std::vector<std::unique_ptr<rmm::device_buffer>> dataBuffers;
      std::vector<std::unique_ptr<rmm::device_buffer>> nullMaskBuffers;
      std::atomic<size_t> pendingReceives{0};
      std::atomic<bool> hasError{false};
    };

    auto perColData = std::make_shared<PerColumnData>();
    perColData->metadata = std::move(ptr->metadata);
    perColData->columnMeta = std::move(columnMetadata);
    perColData->dataBuffers.resize(perColData->columnMeta.size());
    perColData->nullMaskBuffers.resize(perColData->columnMeta.size());

    // Get a stream from the global stream pool
    auto stream =
        facebook::velox::cudf_velox::cudfGlobalStreamPool().get_stream();

    // Allocate buffers for each column based on metadata
    VLOG(3) << toString() << " Allocating buffers for " << perColData->columnMeta.size() << " columns";
    try {
      for (size_t i = 0; i < perColData->columnMeta.size(); ++i) {
        const auto& colMeta = perColData->columnMeta[i];

        VLOG(3) << toString() << " Column " << i << " type=" << static_cast<int>(colMeta.typeId)
                << " size=" << colMeta.size << " dataSize=" << colMeta.dataSize
                << " nullMaskSize=" << colMeta.nullMaskSize << " numChildren=" << colMeta.numChildren;

        // Allocate data buffer if needed
        if (colMeta.dataSize > 0) {
          perColData->dataBuffers[i] = std::make_unique<rmm::device_buffer>(
              static_cast<size_t>(colMeta.dataSize), stream);
          VLOG(3) << toString() << " Allocated data buffer " << i << " size " << colMeta.dataSize
                  << " ptr=" << perColData->dataBuffers[i]->data();
        }

        // Allocate null mask buffer if needed
        if (colMeta.nullMaskSize > 0) {
          perColData->nullMaskBuffers[i] = std::make_unique<rmm::device_buffer>(
              static_cast<size_t>(colMeta.nullMaskSize), stream);
        }
      }
    } catch (const rmm::bad_alloc& e) {
      VLOG(0) << toString() << " *** RMM failed to allocate: " << e.what();
      queue_->setError("Failed to alloc GPU memory");
      setState(ReceiverState::Done);
      communicator_->addToWorkQueue(getSelfPtr());
      return;
    }

    // sync after allocating.
    stream.synchronize();

    if (!setStateIf(
            ReceiverState::WaitingForMetadata, ReceiverState::WaitingForData)) {
      VLOG(1) << toString() << " onMetadata Invalid previous state ";
      return;
    }

    // Base tag for data - matches the sender's tag encoding
    uint64_t baseDataTag = getDataTag(partitionKeyHash_, sequenceNumber_);

    // Initiate receives for each column's buffers
    // Tag encoding: baseDataTag + (columnIndex * 2) for data,
    //               baseDataTag + (columnIndex * 2) + 1 for null_mask
    std::weak_ptr<CudfExchangeSource> weak = weak_from_this();

    // Verify endpoint is still valid
    if (!endpointRef_ || !endpointRef_->endpoint_) {
      VLOG(0) << toString() << " Endpoint is invalid, cannot receive data";
      queue_->setError("Endpoint is invalid");
      setState(ReceiverState::Done);
      communicator_->addToWorkQueue(getSelfPtr());
      return;
    }

    // Store all requests to keep them alive until completion
    auto requests = std::make_shared<std::vector<std::shared_ptr<ucxx::Request>>>();
    requests->reserve(perColData->columnMeta.size() * 2);  // data + null_mask per column

    // Pre-calculate total pending receives BEFORE starting any tagRecv calls.
    // This is critical because tagRecv callbacks can fire synchronously during
    // the call, and we need pendingReceives to be fully initialized before any
    // callback can decrement it.
    size_t totalPendingReceives = 0;
    for (size_t i = 0; i < perColData->columnMeta.size(); ++i) {
      const auto& colMeta = perColData->columnMeta[i];
      if (colMeta.dataSize > 0 && perColData->dataBuffers[i]) {
        ++totalPendingReceives;
      }
      if (colMeta.nullMaskSize > 0 && perColData->nullMaskBuffers[i]) {
        ++totalPendingReceives;
      }
    }

    // If no buffers to receive (e.g., empty table), complete immediately
    if (totalPendingReceives == 0) {
      onPerColumnDataComplete(perColData);
      return;
    }

    // Initialize the counter with the total count before any tagRecv calls
    perColData->pendingReceives.store(totalPendingReceives);

    for (size_t i = 0; i < perColData->columnMeta.size(); ++i) {
      const auto& colMeta = perColData->columnMeta[i];

      // Receive data buffer if expected
      if (colMeta.dataSize > 0 && perColData->dataBuffers[i]) {
        void* bufferPtr = perColData->dataBuffers[i]->data();

        uint64_t dataTag = baseDataTag + (i * 2);

        VLOG(3) << toString() << " Receiving data buffer " << i << " size "
                << colMeta.dataSize << " tag " << std::hex << dataTag
                << std::dec << " bufferPtr=" << bufferPtr;

        auto req = endpointRef_->endpoint_->tagRecv(
            bufferPtr,
            static_cast<size_t>(colMeta.dataSize),
            ucxx::Tag{dataTag},
            ucxx::TagMaskFull,
            false,
            [weak, perColData, requests, i](
                ucs_status_t status, std::shared_ptr<void> arg) {
              if (status != UCS_OK) {
                perColData->hasError.store(true);
              }
              size_t remaining = perColData->pendingReceives.fetch_sub(1) - 1;
              if (remaining == 0) {
                if (auto self = weak.lock()) {
                  self->onPerColumnDataComplete(perColData);
                }
              }
            });
        requests->push_back(req);
      }

      // Receive null mask buffer if expected
      if (colMeta.nullMaskSize > 0 && perColData->nullMaskBuffers[i]) {
        void* nullBufPtr = perColData->nullMaskBuffers[i]->data();

        uint64_t nullTag = baseDataTag + (i * 2) + 1;

        VLOG(3) << toString() << " Receiving null_mask buffer " << i << " size "
                << colMeta.nullMaskSize << " tag " << std::hex << nullTag
                << std::dec << " bufferPtr=" << nullBufPtr;

        auto req = endpointRef_->endpoint_->tagRecv(
            nullBufPtr,
            static_cast<size_t>(colMeta.nullMaskSize),
            ucxx::Tag{nullTag},
            ucxx::TagMaskFull,
            false,
            [weak, perColData, requests, i](
                ucs_status_t status, std::shared_ptr<void> arg) {
              if (status != UCS_OK) {
                perColData->hasError.store(true);
              }
              size_t remaining = perColData->pendingReceives.fetch_sub(1) - 1;
              if (remaining == 0) {
                if (auto self = weak.lock()) {
                  self->onPerColumnDataComplete(perColData);
                }
              }
            });
        requests->push_back(req);
      }
    }
  }
}

void CudfExchangeSource::onPerColumnDataComplete(std::shared_ptr<void> arg) {

  // Check if close() was called - avoid processing if we're shutting down
  if (closed_.load(std::memory_order_acquire)) {
    VLOG(3) << toString()
            << " onPerColumnDataComplete called after close, ignoring";
    return;
  }

  // Structure definition must match what was created in onMetadata
  struct PerColumnData {
    MetadataMsg metadata;
    std::vector<ColumnMetadata> columnMeta;
    std::vector<std::unique_ptr<rmm::device_buffer>> dataBuffers;
    std::vector<std::unique_ptr<rmm::device_buffer>> nullMaskBuffers;
    std::atomic<size_t> pendingReceives{0};
    std::atomic<bool> hasError{false};
  };

  auto perColData = std::static_pointer_cast<PerColumnData>(arg);

  if (perColData->hasError.load()) {
    std::string errorMsg = fmt::format(
        "Failed to receive column data from host {}:{}, task {}",
        host_,
        port_,
        partitionKey_.toString());
    VLOG(0) << toString() << errorMsg;
    setState(ReceiverState::Done);
    queue_->setError(errorMsg);
    communicator_->addToWorkQueue(getSelfPtr());
    return;
  }

  VLOG(3) << toString() << " + onPerColumnDataComplete got chunk: "
          << sequenceNumber_;

  this->sequenceNumber_++;

  // Calculate total bytes received for metrics
  int64_t totalBytes = 0;
  for (const auto& colMeta : perColData->columnMeta) {
    if (colMeta.dataSize > 0) {
      totalBytes += colMeta.dataSize;
    }
    if (colMeta.nullMaskSize > 0) {
      totalBytes += colMeta.nullMaskSize;
    }
  }

  metrics_.numPackedColumns_.addValue(1);
  metrics_.totalBytes_.addValue(totalBytes);

  // Reconstruct the table from per-column buffers.
  // The columns are stored in depth-first order in the metadata.
  // We need to rebuild the column hierarchy.

  int32_t numRootColumns = perColData->metadata.numColumns;
  const auto& columnMeta = perColData->columnMeta;

  // Helper function to recursively build a column from metadata and buffers
  size_t bufferIndex = 0;
  std::function<std::unique_ptr<cudf::column>(size_t&)> buildColumn =
      [&](size_t& idx) -> std::unique_ptr<cudf::column> {
    if (idx >= columnMeta.size()) {
      return nullptr;
    }

    const auto& meta = columnMeta[idx];
    auto& dataBuf = perColData->dataBuffers[idx];
    auto& nullBuf = perColData->nullMaskBuffers[idx];

    // Get data type
    cudf::data_type dtype(meta.typeId, meta.typeScale);

    // Log buffer status before moving
    VLOG(3) << "buildColumn " << idx << " type=" << static_cast<int>(meta.typeId)
            << " size=" << meta.size << " metaDataSize=" << meta.dataSize
            << " dataBuf=" << (dataBuf ? "exists" : "null")
            << " dataBufSize=" << (dataBuf ? dataBuf->size() : 0);

    // Move buffers
    rmm::device_buffer data;
    if (dataBuf && dataBuf->size() > 0) {
      data = std::move(*dataBuf);
    }

    rmm::device_buffer null_mask;
    if (nullBuf && nullBuf->size() > 0) {
      null_mask = std::move(*nullBuf);
    }

    // Build children recursively
    std::vector<std::unique_ptr<cudf::column>> children;
    size_t childIdx = idx + 1;
    for (int32_t c = 0; c < meta.numChildren; ++c) {
      auto child = buildColumn(childIdx);
      if (child) {
        children.push_back(std::move(child));
      }
    }
    idx = childIdx;

    VLOG(3) << "Building column " << idx << " type=" << static_cast<int>(meta.typeId)
            << " size=" << meta.size << " nullCount=" << meta.nullCount
            << " dataSize=" << data.size() << " nullMaskSize=" << null_mask.size()
            << " numChildren=" << children.size();

    // For types that don't have data (like STRUCT), we need to create them differently
    // Also for columns with size=0, no data buffer is needed
    std::unique_ptr<cudf::column> column;
    if (meta.size == 0) {
      // Empty column
      column = cudf::make_empty_column(dtype);
    } else if (meta.typeId == cudf::type_id::STRUCT) {
      // STRUCT columns don't have data buffers, only children and null mask
      column = cudf::make_structs_column(
          meta.size,
          std::move(children),
          meta.nullCount,
          std::move(null_mask));
    } else if (meta.typeId == cudf::type_id::STRING) {
      // STRING columns: data buffer contains chars, child(0) contains offsets
      // data.size() can be 0 for columns of empty strings (valid case!)
      // We need to create the column with the offsets child and chars data
      column = std::make_unique<cudf::column>(
          dtype,
          meta.size,
          std::move(data),  // chars (can be empty)
          std::move(null_mask),
          meta.nullCount,
          std::move(children));  // offsets child
    } else if (data.size() == 0 && meta.size > 0) {
      // This shouldn't happen - we have a non-empty column but no data
      // Log error and create an empty column as fallback
      VLOG(0) << "ERROR: Column " << idx << " has size " << meta.size
              << " but no data buffer!";
      column = cudf::make_empty_column(dtype);
    } else {
      // Normal column with data
      column = std::make_unique<cudf::column>(
          dtype,
          meta.size,
          std::move(data),
          std::move(null_mask),
          meta.nullCount,
          std::move(children));
    }

    return column;
  };

  // Build all root columns
  std::vector<std::unique_ptr<cudf::column>> rootColumns;
  rootColumns.reserve(numRootColumns);

  size_t idx = 0;
  for (int32_t i = 0; i < numRootColumns; ++i) {
    auto col = buildColumn(idx);
    if (col) {
      rootColumns.push_back(std::move(col));
    }
  }

  // Create the table from the columns
  auto table = std::make_unique<cudf::table>(std::move(rootColumns));

  VLOG(3) << toString() << " Reconstructed table with "
          << table->num_columns() << " columns, " << table->num_rows()
          << " rows from per-column data";

  enqueue(std::move(table), perColData->metadata);
  setStateIf(ReceiverState::WaitingForData, ReceiverState::ReadyToReceive);
  communicator_->addToWorkQueue(getSelfPtr());
}

bool CudfExchangeSource::setStateIf(
    CudfExchangeSource::ReceiverState expected,
    CudfExchangeSource::ReceiverState desired) {
  ReceiverState exp = expected;
  // since spurious failures can happen even if state_ == expected, we need
  // to do this in a loop.
  while (!state_.compare_exchange_strong(
      exp, desired, std::memory_order_acq_rel, std::memory_order_relaxed)) {
    if (exp != expected) {
      // no spurious failure, state isn't what we've expected.
      return false;
    }
    // spurious failure.
    exp = expected; // reset for the next try
  }
  return true;
}

} // namespace facebook::velox::cudf_exchange
