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

#include <cuda_runtime.h>
#include <cudf/column/column.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <rmm/device_buffer.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <vector>

namespace facebook::velox::cudf_exchange {

/**
 * Information about a STRING column's chars range needed for transfer.
 * For split STRING columns, firstOffset > 0 indicates rebasing is needed.
 */
struct StringColumnInfo {
  int32_t firstOffset;      // Byte offset where this slice's chars start
  int32_t lastOffset;       // Byte offset where this slice's chars end
  int32_t actualCharsSize;  // Number of chars bytes to transfer (lastOffset - firstOffset)
  cudf::size_type numOffsetsToSend;  // Number of offset values to send (size + 1)
  bool needsRebasing;       // True if firstOffset > 0
};

/**
 * Gets the STRING column info for a potentially split column view.
 * This correctly handles split views where the parent column has a non-zero offset.
 *
 * @param colView The STRING column view (possibly from cudf::split)
 * @return StringColumnInfo with the chars range and rebasing information
 */
inline StringColumnInfo getStringColumnInfo(const cudf::column_view& colView) {
  StringColumnInfo info{};

  if (colView.type().id() != cudf::type_id::STRING ||
      colView.size() == 0 ||
      colView.num_children() == 0) {
    info.firstOffset = 0;
    info.lastOffset = 0;
    info.actualCharsSize = 0;
    info.numOffsetsToSend = 0;
    info.needsRebasing = false;
    return info;
  }

  cudf::strings_column_view scv(colView);
  auto offsetsView = scv.offsets();
  auto parentOffset = colView.offset();  // Index into offsets array for this slice
  auto size = colView.size();            // Number of strings in this slice

  // Read offsets at positions parent.offset() and parent.offset() + size
  // to get the byte range in the chars buffer for this slice.
  const int32_t* offsetsPtr =
      static_cast<const int32_t*>(offsetsView.head()) + offsetsView.offset();

  cudaMemcpy(
      &info.firstOffset,
      offsetsPtr + parentOffset,
      sizeof(int32_t),
      cudaMemcpyDeviceToHost);
  cudaMemcpy(
      &info.lastOffset,
      offsetsPtr + parentOffset + size,
      sizeof(int32_t),
      cudaMemcpyDeviceToHost);

  info.actualCharsSize = info.lastOffset - info.firstOffset;
  info.numOffsetsToSend = size + 1;  // Includes end offset
  info.needsRebasing = (info.firstOffset > 0);

  return info;
}

/**
 * Extracts the partial chars buffer for a split STRING column.
 *
 * @param colView The STRING column view
 * @param info The StringColumnInfo from getStringColumnInfo()
 * @param stream CUDA stream for operations
 * @return Device buffer containing the partial chars data
 */
inline std::unique_ptr<rmm::device_buffer> extractPartialChars(
    const cudf::column_view& colView,
    const StringColumnInfo& info,
    rmm::cuda_stream_view stream) {
  if (info.actualCharsSize <= 0) {
    return std::make_unique<rmm::device_buffer>(0, stream);
  }

  cudf::strings_column_view scv(colView);
  const char* charsBegin = scv.chars_begin(stream);
  const void* partialCharsPtr = charsBegin + info.firstOffset;

  auto buffer = std::make_unique<rmm::device_buffer>(info.actualCharsSize, stream);
  cudaMemcpy(buffer->data(), partialCharsPtr, info.actualCharsSize, cudaMemcpyDeviceToDevice);

  return buffer;
}

/**
 * Extracts and rebases the offsets for a split STRING column.
 * The rebased offsets start at 0 instead of firstOffset.
 *
 * @param colView The STRING column view
 * @param info The StringColumnInfo from getStringColumnInfo()
 * @param stream CUDA stream for operations
 * @return Device buffer containing the rebased offsets
 */
inline std::unique_ptr<rmm::device_buffer> extractRebasedOffsets(
    const cudf::column_view& colView,
    const StringColumnInfo& info,
    rmm::cuda_stream_view stream) {
  if (info.numOffsetsToSend <= 0) {
    return std::make_unique<rmm::device_buffer>(0, stream);
  }

  cudf::strings_column_view scv(colView);
  auto offsetsView = scv.offsets();
  auto parentOffset = colView.offset();

  const int32_t* offsetsPtr =
      static_cast<const int32_t*>(offsetsView.head()) + offsetsView.offset();

  size_t offsetsSize = info.numOffsetsToSend * sizeof(int32_t);

  // Copy original offsets to host
  std::vector<int32_t> hostOffsets(info.numOffsetsToSend);
  cudaMemcpy(
      hostOffsets.data(),
      offsetsPtr + parentOffset,
      offsetsSize,
      cudaMemcpyDeviceToHost);

  // Rebase: subtract firstOffset from all values
  for (size_t i = 0; i < info.numOffsetsToSend; ++i) {
    hostOffsets[i] -= info.firstOffset;
  }

  // Copy rebased offsets to device
  auto buffer = std::make_unique<rmm::device_buffer>(offsetsSize, stream);
  cudaMemcpy(buffer->data(), hostOffsets.data(), offsetsSize, cudaMemcpyHostToDevice);

  return buffer;
}

/**
 * Reconstructs a STRING column from partial chars and rebased offsets.
 * This is the inverse of extract operations - used on the receiver side.
 *
 * @param charsBuffer Device buffer containing the chars data
 * @param offsetsBuffer Device buffer containing the (rebased) offsets
 * @param numStrings Number of strings in the column
 * @param nullMaskBuffer Optional null mask buffer (can be empty)
 * @param nullCount Number of null values
 * @param stream CUDA stream for operations
 * @return Reconstructed STRING column
 */
inline std::unique_ptr<cudf::column> reconstructStringColumn(
    rmm::device_buffer&& charsBuffer,
    rmm::device_buffer&& offsetsBuffer,
    cudf::size_type numStrings,
    rmm::device_buffer&& nullMaskBuffer,
    cudf::size_type nullCount,
    rmm::cuda_stream_view stream) {
  // Create the offsets child column
  auto offsetsColumn = std::make_unique<cudf::column>(
      cudf::data_type{cudf::type_id::INT32},
      numStrings + 1,  // offsets has size + 1 elements
      std::move(offsetsBuffer),
      rmm::device_buffer{},  // no null mask for offsets
      0);                    // no nulls in offsets

  // Build children vector (just offsets for STRING)
  std::vector<std::unique_ptr<cudf::column>> children;
  children.push_back(std::move(offsetsColumn));

  // Create the STRING column
  return std::make_unique<cudf::column>(
      cudf::data_type{cudf::type_id::STRING},
      numStrings,
      std::move(charsBuffer),
      std::move(nullMaskBuffer),
      nullCount,
      std::move(children));
}

/**
 * Verifies that a STRING column can be correctly round-tripped through
 * extract and reconstruct operations. Used for testing.
 *
 * @param original The original STRING column view
 * @param reconstructed The reconstructed STRING column
 * @param stream CUDA stream for operations
 * @return True if the columns match, false otherwise
 */
inline bool verifyStringColumnRoundTrip(
    const cudf::column_view& original,
    const cudf::column& reconstructed,
    rmm::cuda_stream_view stream) {
  if (original.size() != reconstructed.size()) {
    return false;
  }

  if (original.size() == 0) {
    return true;
  }

  // Compare chars
  cudf::strings_column_view origScv(original);
  cudf::strings_column_view reconScv(reconstructed.view());

  auto origInfo = getStringColumnInfo(original);

  // For reconstructed column, chars start at 0
  auto reconCharsSize = reconScv.chars_size(stream);

  if (origInfo.actualCharsSize != reconCharsSize) {
    return false;
  }

  if (origInfo.actualCharsSize > 0) {
    // Compare chars content
    std::vector<char> origChars(origInfo.actualCharsSize);
    std::vector<char> reconChars(reconCharsSize);

    cudaMemcpy(
        origChars.data(),
        origScv.chars_begin(stream) + origInfo.firstOffset,
        origInfo.actualCharsSize,
        cudaMemcpyDeviceToHost);
    cudaMemcpy(
        reconChars.data(),
        reconScv.chars_begin(stream),
        reconCharsSize,
        cudaMemcpyDeviceToHost);

    if (origChars != reconChars) {
      return false;
    }
  }

  // Compare rebased offsets
  std::vector<int32_t> origOffsets(origInfo.numOffsetsToSend);
  std::vector<int32_t> reconOffsets(original.size() + 1);

  auto origOffsetsView = origScv.offsets();
  const int32_t* origOffsetsPtr =
      static_cast<const int32_t*>(origOffsetsView.head()) +
      origOffsetsView.offset() + original.offset();

  cudaMemcpy(
      origOffsets.data(),
      origOffsetsPtr,
      origInfo.numOffsetsToSend * sizeof(int32_t),
      cudaMemcpyDeviceToHost);

  auto reconOffsetsView = reconScv.offsets();
  cudaMemcpy(
      reconOffsets.data(),
      reconOffsetsView.head(),
      (original.size() + 1) * sizeof(int32_t),
      cudaMemcpyDeviceToHost);

  // Rebase original offsets for comparison
  for (auto& off : origOffsets) {
    off -= origInfo.firstOffset;
  }

  return origOffsets == reconOffsets;
}

} // namespace facebook::velox::cudf_exchange
