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

#include <gtest/gtest.h>
#include <cuda_runtime.h>
#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <rmm/cuda_stream.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/per_device_resource.hpp>

#include "velox/experimental/cudf-exchange/StringColumnUtils.h"

namespace facebook::velox::cudf_exchange {
namespace {

class StringColumnSplitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Force CUDA context creation
    cudaFree(0);
    stream_ = rmm::cuda_stream();
  }

  rmm::cuda_stream stream_;

  // Helper to create a STRING column from host strings
  std::unique_ptr<cudf::column> makeStringColumn(
      const std::vector<std::string>& strings) {
    // Calculate total chars size
    size_t totalChars = 0;
    for (const auto& s : strings) {
      totalChars += s.size();
    }

    // Build offsets
    std::vector<int32_t> hostOffsets(strings.size() + 1);
    hostOffsets[0] = 0;
    for (size_t i = 0; i < strings.size(); ++i) {
      hostOffsets[i + 1] = hostOffsets[i] + static_cast<int32_t>(strings[i].size());
    }

    // Build chars
    std::vector<char> hostChars(totalChars);
    size_t offset = 0;
    for (const auto& s : strings) {
      std::memcpy(hostChars.data() + offset, s.data(), s.size());
      offset += s.size();
    }

    // Create device buffers
    rmm::device_buffer charsBuffer(totalChars, stream_.view());
    rmm::device_buffer offsetsBuffer(hostOffsets.size() * sizeof(int32_t), stream_.view());

    cudaMemcpy(charsBuffer.data(), hostChars.data(), totalChars, cudaMemcpyHostToDevice);
    cudaMemcpy(offsetsBuffer.data(), hostOffsets.data(),
               hostOffsets.size() * sizeof(int32_t), cudaMemcpyHostToDevice);

    // Create offsets column
    auto offsetsColumn = std::make_unique<cudf::column>(
        cudf::data_type{cudf::type_id::INT32},
        static_cast<cudf::size_type>(strings.size() + 1),
        std::move(offsetsBuffer),
        rmm::device_buffer{},
        0);

    // Create STRING column
    std::vector<std::unique_ptr<cudf::column>> children;
    children.push_back(std::move(offsetsColumn));

    return std::make_unique<cudf::column>(
        cudf::data_type{cudf::type_id::STRING},
        static_cast<cudf::size_type>(strings.size()),
        std::move(charsBuffer),
        rmm::device_buffer{},
        0,
        std::move(children));
  }

  // Helper to read strings from a STRING column back to host
  std::vector<std::string> readStringColumn(const cudf::column_view& col) {
    std::vector<std::string> result;

    if (col.size() == 0) {
      return result;
    }

    cudf::strings_column_view scv(col);
    auto info = getStringColumnInfo(col);

    // Read chars
    std::vector<char> chars(info.actualCharsSize);
    if (info.actualCharsSize > 0) {
      cudaMemcpy(
          chars.data(),
          scv.chars_begin(stream_.view()) + info.firstOffset,
          info.actualCharsSize,
          cudaMemcpyDeviceToHost);
    }

    // Read offsets
    std::vector<int32_t> offsets(info.numOffsetsToSend);
    auto offsetsView = scv.offsets();
    const int32_t* offsetsPtr =
        static_cast<const int32_t*>(offsetsView.head()) +
        offsetsView.offset() + col.offset();
    cudaMemcpy(
        offsets.data(),
        offsetsPtr,
        info.numOffsetsToSend * sizeof(int32_t),
        cudaMemcpyDeviceToHost);

    // Extract strings
    result.reserve(col.size());
    for (cudf::size_type i = 0; i < col.size(); ++i) {
      int32_t start = offsets[i] - info.firstOffset;
      int32_t end = offsets[i + 1] - info.firstOffset;
      result.emplace_back(chars.data() + start, end - start);
    }

    return result;
  }
};

// Test basic STRING column info extraction
TEST_F(StringColumnSplitTest, BasicStringColumnInfo) {
  auto col = makeStringColumn({"hello", "world", "foo", "bar", "baz"});

  auto info = getStringColumnInfo(col->view());

  EXPECT_EQ(info.firstOffset, 0);
  EXPECT_EQ(info.lastOffset, 19);  // "helloworldfoobarbaz" = 19 chars
  EXPECT_EQ(info.actualCharsSize, 19);
  EXPECT_EQ(info.numOffsetsToSend, 6);  // 5 strings + 1
  EXPECT_FALSE(info.needsRebasing);
}

// Test STRING column split and info extraction
TEST_F(StringColumnSplitTest, SplitStringColumnInfo) {
  auto col = makeStringColumn({"hello", "world", "foo", "bar", "baz"});
  // chars: "helloworldfoobarbaz" (19 bytes)
  // offsets: [0, 5, 10, 13, 16, 19]

  // Split at index 3 (after "foo")
  auto partitions = cudf::split(col->view(), {3}, stream_.view());

  ASSERT_EQ(partitions.size(), 2);

  // Partition 0: ["hello", "world", "foo"]
  auto info0 = getStringColumnInfo(partitions[0]);
  EXPECT_EQ(info0.firstOffset, 0);
  EXPECT_EQ(info0.lastOffset, 13);  // "helloworldfoo" = 13 chars
  EXPECT_EQ(info0.actualCharsSize, 13);
  EXPECT_EQ(info0.numOffsetsToSend, 4);  // 3 strings + 1
  EXPECT_FALSE(info0.needsRebasing);

  // Partition 1: ["bar", "baz"]
  auto info1 = getStringColumnInfo(partitions[1]);
  EXPECT_EQ(info1.firstOffset, 13);
  EXPECT_EQ(info1.lastOffset, 19);  // "barbaz" = 6 chars
  EXPECT_EQ(info1.actualCharsSize, 6);
  EXPECT_EQ(info1.numOffsetsToSend, 3);  // 2 strings + 1
  EXPECT_TRUE(info1.needsRebasing);

  // Verify we can still read the strings correctly
  auto strings0 = readStringColumn(partitions[0]);
  ASSERT_EQ(strings0.size(), 3);
  EXPECT_EQ(strings0[0], "hello");
  EXPECT_EQ(strings0[1], "world");
  EXPECT_EQ(strings0[2], "foo");

  auto strings1 = readStringColumn(partitions[1]);
  ASSERT_EQ(strings1.size(), 2);
  EXPECT_EQ(strings1[0], "bar");
  EXPECT_EQ(strings1[1], "baz");
}

// Test extract and reconstruct round-trip for first partition (no rebasing)
TEST_F(StringColumnSplitTest, RoundTripFirstPartition) {
  auto col = makeStringColumn({"hello", "world", "foo", "bar", "baz"});
  auto partitions = cudf::split(col->view(), {3}, stream_.view());

  auto& partition0 = partitions[0];
  auto info = getStringColumnInfo(partition0);

  // Extract chars and offsets
  auto charsBuffer = extractPartialChars(partition0, info, stream_.view());
  auto offsetsBuffer = extractRebasedOffsets(partition0, info, stream_.view());

  stream_.synchronize();

  // Reconstruct
  auto reconstructed = reconstructStringColumn(
      std::move(*charsBuffer),
      std::move(*offsetsBuffer),
      partition0.size(),
      rmm::device_buffer{},
      0,
      stream_.view());

  stream_.synchronize();

  // Verify
  EXPECT_TRUE(verifyStringColumnRoundTrip(partition0, *reconstructed, stream_.view()));

  auto strings = readStringColumn(reconstructed->view());
  ASSERT_EQ(strings.size(), 3);
  EXPECT_EQ(strings[0], "hello");
  EXPECT_EQ(strings[1], "world");
  EXPECT_EQ(strings[2], "foo");
}

// Test extract and reconstruct round-trip for second partition (with rebasing)
TEST_F(StringColumnSplitTest, RoundTripSecondPartitionWithRebasing) {
  auto col = makeStringColumn({"hello", "world", "foo", "bar", "baz"});
  auto partitions = cudf::split(col->view(), {3}, stream_.view());

  auto& partition1 = partitions[1];
  auto info = getStringColumnInfo(partition1);

  EXPECT_TRUE(info.needsRebasing);
  EXPECT_EQ(info.firstOffset, 13);

  // Extract chars and offsets (rebased)
  auto charsBuffer = extractPartialChars(partition1, info, stream_.view());
  auto offsetsBuffer = extractRebasedOffsets(partition1, info, stream_.view());

  stream_.synchronize();

  // Verify rebased offsets start at 0
  std::vector<int32_t> rebasedOffsets(info.numOffsetsToSend);
  cudaMemcpy(
      rebasedOffsets.data(),
      offsetsBuffer->data(),
      info.numOffsetsToSend * sizeof(int32_t),
      cudaMemcpyDeviceToHost);

  EXPECT_EQ(rebasedOffsets[0], 0);  // Should be rebased to start at 0
  EXPECT_EQ(rebasedOffsets[1], 3);  // "bar" = 3 chars
  EXPECT_EQ(rebasedOffsets[2], 6);  // "barbaz" = 6 chars total

  // Reconstruct
  auto reconstructed = reconstructStringColumn(
      std::move(*charsBuffer),
      std::move(*offsetsBuffer),
      partition1.size(),
      rmm::device_buffer{},
      0,
      stream_.view());

  stream_.synchronize();

  // Verify
  EXPECT_TRUE(verifyStringColumnRoundTrip(partition1, *reconstructed, stream_.view()));

  auto strings = readStringColumn(reconstructed->view());
  ASSERT_EQ(strings.size(), 2);
  EXPECT_EQ(strings[0], "bar");
  EXPECT_EQ(strings[1], "baz");
}

// Test with multiple split points (3 partitions)
TEST_F(StringColumnSplitTest, ThreePartitions) {
  auto col = makeStringColumn({"a", "bb", "ccc", "dddd", "eeeee", "ffffff"});
  // chars: "abbcccddddeeeeeffffff" (21 bytes)
  // offsets: [0, 1, 3, 6, 10, 15, 21]

  // Split at indices 2 and 4
  auto partitions = cudf::split(col->view(), {2, 4}, stream_.view());

  ASSERT_EQ(partitions.size(), 3);

  // Partition 0: ["a", "bb"]
  auto info0 = getStringColumnInfo(partitions[0]);
  EXPECT_EQ(info0.firstOffset, 0);
  EXPECT_EQ(info0.actualCharsSize, 3);  // "abb"
  EXPECT_FALSE(info0.needsRebasing);

  // Partition 1: ["ccc", "dddd"]
  auto info1 = getStringColumnInfo(partitions[1]);
  EXPECT_EQ(info1.firstOffset, 3);
  EXPECT_EQ(info1.actualCharsSize, 7);  // "cccdddd"
  EXPECT_TRUE(info1.needsRebasing);

  // Partition 2: ["eeeee", "ffffff"]
  auto info2 = getStringColumnInfo(partitions[2]);
  EXPECT_EQ(info2.firstOffset, 10);
  EXPECT_EQ(info2.actualCharsSize, 11);  // "eeeeeffffff"
  EXPECT_TRUE(info2.needsRebasing);

  // Round-trip all partitions
  for (size_t i = 0; i < partitions.size(); ++i) {
    auto& partition = partitions[i];
    auto info = getStringColumnInfo(partition);

    auto charsBuffer = extractPartialChars(partition, info, stream_.view());
    auto offsetsBuffer = extractRebasedOffsets(partition, info, stream_.view());

    stream_.synchronize();

    auto reconstructed = reconstructStringColumn(
        std::move(*charsBuffer),
        std::move(*offsetsBuffer),
        partition.size(),
        rmm::device_buffer{},
        0,
        stream_.view());

    stream_.synchronize();

    EXPECT_TRUE(verifyStringColumnRoundTrip(partition, *reconstructed, stream_.view()))
        << "Partition " << i << " failed round-trip verification";
  }
}

// Test edge case: empty strings
TEST_F(StringColumnSplitTest, EmptyStrings) {
  auto col = makeStringColumn({"", "hello", "", "world", ""});
  // chars: "helloworld" (10 bytes)
  // offsets: [0, 0, 5, 5, 10, 10]

  auto partitions = cudf::split(col->view(), {2}, stream_.view());

  // Partition 0: ["", "hello"]
  auto strings0 = readStringColumn(partitions[0]);
  ASSERT_EQ(strings0.size(), 2);
  EXPECT_EQ(strings0[0], "");
  EXPECT_EQ(strings0[1], "hello");

  // Partition 1: ["", "world", ""]
  auto strings1 = readStringColumn(partitions[1]);
  ASSERT_EQ(strings1.size(), 3);
  EXPECT_EQ(strings1[0], "");
  EXPECT_EQ(strings1[1], "world");
  EXPECT_EQ(strings1[2], "");

  // Round-trip partition 1 (needs rebasing)
  auto info = getStringColumnInfo(partitions[1]);
  auto charsBuffer = extractPartialChars(partitions[1], info, stream_.view());
  auto offsetsBuffer = extractRebasedOffsets(partitions[1], info, stream_.view());

  stream_.synchronize();

  auto reconstructed = reconstructStringColumn(
      std::move(*charsBuffer),
      std::move(*offsetsBuffer),
      partitions[1].size(),
      rmm::device_buffer{},
      0,
      stream_.view());

  auto reconStrings = readStringColumn(reconstructed->view());
  ASSERT_EQ(reconStrings.size(), 3);
  EXPECT_EQ(reconStrings[0], "");
  EXPECT_EQ(reconStrings[1], "world");
  EXPECT_EQ(reconStrings[2], "");
}

// Test edge case: single string per partition
TEST_F(StringColumnSplitTest, SingleStringPerPartition) {
  auto col = makeStringColumn({"first", "second", "third"});

  auto partitions = cudf::split(col->view(), {1, 2}, stream_.view());

  ASSERT_EQ(partitions.size(), 3);

  for (size_t i = 0; i < partitions.size(); ++i) {
    auto& partition = partitions[i];
    EXPECT_EQ(partition.size(), 1);

    auto info = getStringColumnInfo(partition);
    auto charsBuffer = extractPartialChars(partition, info, stream_.view());
    auto offsetsBuffer = extractRebasedOffsets(partition, info, stream_.view());

    stream_.synchronize();

    auto reconstructed = reconstructStringColumn(
        std::move(*charsBuffer),
        std::move(*offsetsBuffer),
        partition.size(),
        rmm::device_buffer{},
        0,
        stream_.view());

    EXPECT_TRUE(verifyStringColumnRoundTrip(partition, *reconstructed, stream_.view()))
        << "Single-string partition " << i << " failed";
  }
}

// Test table with STRING column split
TEST_F(StringColumnSplitTest, TableWithStringColumnSplit) {
  // Create a table with INT32 and STRING columns
  auto intCol = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      5,
      cudf::mask_state::UNALLOCATED,
      stream_.view());

  // Fill int column with values 0-4
  std::vector<int32_t> intData = {0, 1, 2, 3, 4};
  cudaMemcpy(
      intCol->mutable_view().data<int32_t>(),
      intData.data(),
      5 * sizeof(int32_t),
      cudaMemcpyHostToDevice);

  auto strCol = makeStringColumn({"zero", "one", "two", "three", "four"});

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(intCol));
  columns.push_back(std::move(strCol));

  auto table = std::make_unique<cudf::table>(std::move(columns));

  // Split the table
  auto tablePartitions = cudf::split(table->view(), {2}, stream_.view());

  ASSERT_EQ(tablePartitions.size(), 2);

  // Check partition 0 STRING column
  auto strView0 = tablePartitions[0].column(1);
  auto info0 = getStringColumnInfo(strView0);
  EXPECT_FALSE(info0.needsRebasing);

  auto strings0 = readStringColumn(strView0);
  ASSERT_EQ(strings0.size(), 2);
  EXPECT_EQ(strings0[0], "zero");
  EXPECT_EQ(strings0[1], "one");

  // Check partition 1 STRING column (needs rebasing)
  auto strView1 = tablePartitions[1].column(1);
  auto info1 = getStringColumnInfo(strView1);
  EXPECT_TRUE(info1.needsRebasing);

  auto strings1 = readStringColumn(strView1);
  ASSERT_EQ(strings1.size(), 3);
  EXPECT_EQ(strings1[0], "two");
  EXPECT_EQ(strings1[1], "three");
  EXPECT_EQ(strings1[2], "four");

  // Round-trip partition 1's STRING column
  auto charsBuffer = extractPartialChars(strView1, info1, stream_.view());
  auto offsetsBuffer = extractRebasedOffsets(strView1, info1, stream_.view());

  stream_.synchronize();

  auto reconstructed = reconstructStringColumn(
      std::move(*charsBuffer),
      std::move(*offsetsBuffer),
      strView1.size(),
      rmm::device_buffer{},
      0,
      stream_.view());

  EXPECT_TRUE(verifyStringColumnRoundTrip(strView1, *reconstructed, stream_.view()));
}

} // namespace
} // namespace facebook::velox::cudf_exchange
