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

#include "cuda.h"
#include <cuda_runtime.h>

#include <cudf/null_mask.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/structs/structs_column_view.hpp>
#include <rmm/cuda_stream_view.hpp>
#include "velox/experimental/cudf-exchange/CudfExchangeProtocol.h"

namespace facebook::velox::cudf_exchange {

// ColumnMetadata implementation

void ColumnMetadata::serialize(uint8_t* buffer) const {
  uint8_t* ptr = buffer;

  int32_t typeIdInt = static_cast<int32_t>(typeId);
  std::memcpy(ptr, &typeIdInt, sizeof(int32_t));
  ptr += sizeof(int32_t);

  std::memcpy(ptr, &typeScale, sizeof(int32_t));
  ptr += sizeof(int32_t);

  std::memcpy(ptr, &size, sizeof(int32_t));
  ptr += sizeof(int32_t);

  std::memcpy(ptr, &nullCount, sizeof(int32_t));
  ptr += sizeof(int32_t);

  std::memcpy(ptr, &dataSize, sizeof(int64_t));
  ptr += sizeof(int64_t);

  std::memcpy(ptr, &nullMaskSize, sizeof(int64_t));
  ptr += sizeof(int64_t);

  std::memcpy(ptr, &numChildren, sizeof(int32_t));
}

ColumnMetadata ColumnMetadata::deserialize(const uint8_t* buffer) {
  ColumnMetadata meta;
  const uint8_t* ptr = buffer;

  int32_t typeIdInt;
  std::memcpy(&typeIdInt, ptr, sizeof(int32_t));
  meta.typeId = static_cast<cudf::type_id>(typeIdInt);
  ptr += sizeof(int32_t);

  std::memcpy(&meta.typeScale, ptr, sizeof(int32_t));
  ptr += sizeof(int32_t);

  std::memcpy(&meta.size, ptr, sizeof(int32_t));
  ptr += sizeof(int32_t);

  std::memcpy(&meta.nullCount, ptr, sizeof(int32_t));
  ptr += sizeof(int32_t);

  std::memcpy(&meta.dataSize, ptr, sizeof(int64_t));
  ptr += sizeof(int64_t);

  std::memcpy(&meta.nullMaskSize, ptr, sizeof(int64_t));
  ptr += sizeof(int64_t);

  std::memcpy(&meta.numChildren, ptr, sizeof(int32_t));

  return meta;
}

// TableMetadata implementation

void TableMetadata::buildColumnMetadata(
    const cudf::column_view& col,
    std::vector<ColumnMetadata>& metadata) {
  ColumnMetadata meta;

  // Type information
  meta.typeId = col.type().id();
  meta.typeScale = col.type().scale();

  // Size information
  meta.size = col.size();
  meta.nullCount = col.null_count();

  // Data buffer size
  // We need to compute the actual buffer size that column::release() will return.
  // The sender sends data when contents.data && contents.data->size() > 0.
  // We check if the column has a valid data pointer using head().
  //
  // Note: For nested types (STRING, LIST, STRUCT), the data buffer semantics differ:
  // - STRING: chars array in parent's data buffer, offsets column as child(0)
  // - LIST: offsets array (size+1 int32 elements) in parent, elements in child
  // - STRUCT: no data buffer (only children)
  //
  // We mark dataSize = -1 if there's no data to send.
  if (col.size() == 0) {
    // Empty column has no data
    meta.dataSize = -1;
  } else {
    switch (meta.typeId) {
      case cudf::type_id::STRUCT:
        // STRUCT has no data buffer, only children
        meta.dataSize = -1;
        break;
      case cudf::type_id::STRING: {
        // STRING columns store chars in the parent's data buffer.
        // The offsets are in child(0).
        // For split STRING columns, we compute the actual chars range needed
        // by reading the first and last offset values. This allows sending
        // only the partial chars buffer instead of the full buffer.
        // The sender will rebase offsets so offsets[0] = 0.
        // The parent column's offset() tells us where this slice starts
        // in the offsets array.
        if (col.num_children() > 0) {
          cudf::strings_column_view scv(col);
          auto offsetsView = scv.offsets();
          auto parentOffset = col.offset();  // Index into offsets array
          auto size = col.size();  // Number of strings in this slice

          if (size > 0) {
            // Read offsets at positions parent.offset() and parent.offset() + size
            int32_t firstOffset = 0;
            int32_t lastOffset = 0;
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

            // Store actual chars size needed (partial buffer for split views)
            meta.dataSize = lastOffset - firstOffset;
          } else {
            meta.dataSize = -1;
          }
        } else {
          meta.dataSize = -1;
        }
        break;
      }
      case cudf::type_id::LIST:
        // LIST has offsets buffer if it has data
        // Check if offsets are present
        if (col.head<int32_t>() != nullptr) {
          // Offsets array has (size + 1) elements
          meta.dataSize =
              static_cast<int64_t>(col.size() + 1) * sizeof(int32_t);
        } else {
          meta.dataSize = -1;
        }
        break;
      default:
        // Fixed-width type: check if data pointer is valid
        if (col.head<uint8_t>() != nullptr) {
          meta.dataSize =
              static_cast<int64_t>(col.size()) * cudf::size_of(col.type());
        } else {
          meta.dataSize = -1;
        }
        break;
    }
  }

  // Null mask size
  // Only set nullMaskSize if the column has a valid null mask pointer
  // Note: nullable() can be true but null_mask() nullptr if no nulls exist
  if (col.size() > 0 && col.null_mask() != nullptr) {
    // Null mask size is determined by number of elements
    // cudf uses 1 bit per element, rounded up to 64-byte alignment
    meta.nullMaskSize =
        static_cast<int64_t>(cudf::bitmask_allocation_size_bytes(col.size()));
  } else {
    meta.nullMaskSize = -1;
  }

  // Number of children
  // For STRING columns, we only transfer the offsets child (child 0).
  // The chars child (child 1) is handled specially via strings_column_view,
  // so we report only 1 child in metadata.
  if (col.type().id() == cudf::type_id::STRING) {
    meta.numChildren = col.num_children() > 0 ? 1 : 0;
  } else {
    meta.numChildren = col.num_children();
  }

  // DEBUG: Log metadata for STRING columns for corruption detection
  // metadata.size() is the index of this column (before push_back)
  size_t colIdx = metadata.size();
  if (meta.typeId == cudf::type_id::STRING) {
    if (col.num_children() > 0) {
      cudf::strings_column_view scv(col);
      auto offsetsView = scv.offsets();
      auto parentOffset = col.offset();
      auto size = col.size();
      int32_t firstOff = 0, lastOff = 0;
      if (size > 0) {
        const int32_t* offsetsPtr =
            static_cast<const int32_t*>(offsetsView.head()) +
            offsetsView.offset();
        cudaMemcpy(&firstOff, offsetsPtr + parentOffset, sizeof(int32_t), cudaMemcpyDeviceToHost);
        cudaMemcpy(&lastOff, offsetsPtr + parentOffset + size, sizeof(int32_t), cudaMemcpyDeviceToHost);
      }
      VLOG(1) << "[META-STR] col=" << colIdx << " STRING: size=" << meta.size
              << " dataSize=" << meta.dataSize << " (chars range " << firstOff
              << "-" << lastOff << ")"
              << " nullMaskSize=" << meta.nullMaskSize
              << " numChildren=" << meta.numChildren
              << " col.offset=" << col.offset()
              << " needsRebasing=" << (firstOff > 0 ? "yes" : "no");
    } else {
      VLOG(1) << "[META-STR] col=" << colIdx << " STRING: size=" << meta.size
              << " dataSize=" << meta.dataSize
              << " nullMaskSize=" << meta.nullMaskSize
              << " numChildren=" << meta.numChildren
              << " col.offset=" << col.offset() << " (no children)";
    }
  } else {
    VLOG(2) << "[META] col=" << colIdx
            << " type=" << static_cast<int>(meta.typeId)
            << " size=" << meta.size
            << " dataSize=" << meta.dataSize
            << " nullMaskSize=" << meta.nullMaskSize
            << " numChildren=" << meta.numChildren
            << " col.offset=" << col.offset();
  }

  // Add this column's metadata
  metadata.push_back(meta);

  // Recursively process children (depth-first)
  // For STRING columns, we create a synthetic offsets metadata entry that reflects
  // what we actually send: (parent.size() + 1) INT32 offsets. We can't use
  // col.child(0) directly because for split views, child(0) returns the original
  // unsliced offsets column with the wrong size.
  // For STRUCT columns, use structs_column_view::get_sliced_child() to get
  // children with the parent's offset/size applied (needed after cudf::split).
  if (col.type().id() == cudf::type_id::STRING) {
    if (col.num_children() > 0 && col.size() > 0) {
      // Create synthetic offsets metadata with correct size for this slice
      ColumnMetadata offsetsMeta;
      offsetsMeta.typeId = cudf::type_id::INT32;
      offsetsMeta.typeScale = 0;
      // Offsets array has (string_count + 1) elements
      offsetsMeta.size = col.size() + 1;
      offsetsMeta.nullCount = 0;  // Offsets never have nulls
      offsetsMeta.dataSize = static_cast<int64_t>(col.size() + 1) * sizeof(int32_t);
      offsetsMeta.nullMaskSize = -1;  // No null mask
      offsetsMeta.numChildren = 0;

      size_t colIdx = metadata.size();
      VLOG(2) << "[META-OFFSETS] col=" << colIdx
              << " INT32 offsets: size=" << offsetsMeta.size
              << " dataSize=" << offsetsMeta.dataSize
              << " for parent STRING with size=" << col.size();

      metadata.push_back(offsetsMeta);
    }
  } else if (col.type().id() == cudf::type_id::STRUCT) {
    // Use structs_column_view::get_sliced_child() for proper offset handling
    // after cudf::split. The child() method returns children with original
    // sizes, but get_sliced_child() returns children adjusted to parent's slice.
    cudf::structs_column_view scv(col);
    for (cudf::size_type i = 0; i < col.num_children(); ++i) {
      buildColumnMetadata(scv.get_sliced_child(i), metadata);
    }
  } else {
    for (cudf::size_type i = 0; i < col.num_children(); ++i) {
      buildColumnMetadata(col.child(i), metadata);
    }
  }
}

std::unique_ptr<std::vector<uint8_t>> TableMetadata::buildFromTable(
    const cudf::table_view& table) {
  std::vector<ColumnMetadata> columnMetadata;

  // Reserve space for typical table sizes
  columnMetadata.reserve(table.num_columns() * 2); // Account for some nesting

  // Process each root column
  for (cudf::size_type i = 0; i < table.num_columns(); ++i) {
    buildColumnMetadata(table.column(i), columnMetadata);
  }

  // Calculate total size: header + column metadata
  // Header: numRootColumns (int32) + totalColumns (int32)
  size_t headerSize = 2 * sizeof(int32_t);
  size_t totalSize =
      headerSize + columnMetadata.size() * ColumnMetadata::serializedSize();

  auto buffer = std::make_unique<std::vector<uint8_t>>(totalSize);
  uint8_t* ptr = buffer->data();

  // Write header
  int32_t numRootColumns = table.num_columns();
  std::memcpy(ptr, &numRootColumns, sizeof(int32_t));
  ptr += sizeof(int32_t);

  int32_t totalColumns = static_cast<int32_t>(columnMetadata.size());
  std::memcpy(ptr, &totalColumns, sizeof(int32_t));
  ptr += sizeof(int32_t);

  // Write each column's metadata
  for (const auto& meta : columnMetadata) {
    meta.serialize(ptr);
    ptr += ColumnMetadata::serializedSize();
  }

  return buffer;
}

std::vector<ColumnMetadata> TableMetadata::deserialize(
    const std::vector<uint8_t>& buffer) {
  if (buffer.size() < 2 * sizeof(int32_t)) {
    throw std::runtime_error("TableMetadata buffer too small for header");
  }

  const uint8_t* ptr = buffer.data();

  // Skip numRootColumns
  ptr += sizeof(int32_t);

  // Read total column count
  int32_t totalColumns;
  std::memcpy(&totalColumns, ptr, sizeof(int32_t));
  ptr += sizeof(int32_t);

  // Validate buffer size
  size_t expectedSize = 2 * sizeof(int32_t) +
      static_cast<size_t>(totalColumns) * ColumnMetadata::serializedSize();
  if (buffer.size() < expectedSize) {
    throw std::runtime_error("TableMetadata buffer too small for columns");
  }

  // Deserialize each column
  std::vector<ColumnMetadata> metadata;
  metadata.reserve(totalColumns);

  for (int32_t i = 0; i < totalColumns; ++i) {
    metadata.push_back(ColumnMetadata::deserialize(ptr));
    ptr += ColumnMetadata::serializedSize();
  }

  return metadata;
}

int32_t TableMetadata::getNumRootColumns(const std::vector<uint8_t>& buffer) {
  if (buffer.size() < sizeof(int32_t)) {
    throw std::runtime_error("TableMetadata buffer too small");
  }

  int32_t numRootColumns;
  std::memcpy(&numRootColumns, buffer.data(), sizeof(int32_t));
  return numRootColumns;
}

uint32_t fnv1a_32(const std::string& s) {
  uint32_t hash = 0x811C9DC5u; // FNV offset basis
  for (unsigned char c : s) {
    hash ^= c;
    hash *= 0x01000193u; // FNV prime
  }
  return hash;
}

void cudaCheck(CUresult result) {
  if (result != CUDA_SUCCESS) {
    const char* err_msg;
    cuGetErrorName(result, &err_msg);
    std::cout << "Cuda error: " << err_msg << std::endl;
    exit(-1);
  }
}

} // namespace facebook::velox::cudf_exchange
