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

#include <cudf/column/column_factories.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <gtest/gtest.h>
#include <rmm/device_buffer.hpp>
#include <rmm/mr/device/per_device_resource.hpp>
#include "velox/experimental/cudf-exchange/CudfExchangeProtocol.h"
#include "velox/experimental/cudf-exchange/tests/CudfTestHelpers.h"

using namespace facebook::velox::cudf_exchange;

class TableMetadataTest : public testing::Test {
 protected:
  TableMetadataTest() : stream_(rmm::cuda_stream_default) {}

  void SetUp() override {}

  rmm::cuda_stream_view stream_;
};

// Test ColumnMetadata serialization round-trip
TEST_F(TableMetadataTest, columnMetadataSerializeDeserialize) {
  ColumnMetadata original;
  original.typeId = cudf::type_id::INT32;
  original.typeScale = 0;
  original.size = 100;
  original.nullCount = 5;
  original.dataSize = 400;
  original.nullMaskSize = 64;
  original.numChildren = 0;

  std::vector<uint8_t> buffer(ColumnMetadata::serializedSize());
  original.serialize(buffer.data());

  ColumnMetadata deserialized = ColumnMetadata::deserialize(buffer.data());

  EXPECT_EQ(deserialized.typeId, original.typeId);
  EXPECT_EQ(deserialized.typeScale, original.typeScale);
  EXPECT_EQ(deserialized.size, original.size);
  EXPECT_EQ(deserialized.nullCount, original.nullCount);
  EXPECT_EQ(deserialized.dataSize, original.dataSize);
  EXPECT_EQ(deserialized.nullMaskSize, original.nullMaskSize);
  EXPECT_EQ(deserialized.numChildren, original.numChildren);
}

// Test ColumnMetadata with fixed_point type (has non-zero scale)
TEST_F(TableMetadataTest, columnMetadataFixedPointType) {
  ColumnMetadata original;
  original.typeId = cudf::type_id::DECIMAL64;
  original.typeScale = -3; // 3 decimal places
  original.size = 50;
  original.nullCount = 0;
  original.dataSize = 400;
  original.nullMaskSize = -1; // No nulls
  original.numChildren = 0;

  std::vector<uint8_t> buffer(ColumnMetadata::serializedSize());
  original.serialize(buffer.data());

  ColumnMetadata deserialized = ColumnMetadata::deserialize(buffer.data());

  EXPECT_EQ(deserialized.typeId, cudf::type_id::DECIMAL64);
  EXPECT_EQ(deserialized.typeScale, -3);
  EXPECT_EQ(deserialized.nullMaskSize, -1);
}

// Test TableMetadata with a single INT32 column
TEST_F(TableMetadataTest, singleInt32Column) {
  const cudf::size_type numRows = 100;

  // Create an INT32 column
  auto col = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      numRows,
      cudf::mask_state::UNALLOCATED,
      stream_);

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(col));
  cudf::table table(std::move(columns));

  auto serialized = TableMetadata::buildFromTable(table.view());
  ASSERT_NE(serialized, nullptr);
  EXPECT_GT(serialized->size(), 0);

  // Check number of root columns
  EXPECT_EQ(TableMetadata::getNumRootColumns(*serialized), 1);

  // Deserialize and verify
  auto metadata = TableMetadata::deserialize(*serialized);
  ASSERT_EQ(metadata.size(), 1);

  EXPECT_EQ(metadata[0].typeId, cudf::type_id::INT32);
  EXPECT_EQ(metadata[0].size, numRows);
  EXPECT_EQ(metadata[0].nullCount, 0);
  EXPECT_EQ(metadata[0].dataSize, numRows * sizeof(int32_t));
  EXPECT_EQ(metadata[0].nullMaskSize, -1); // No null mask
  EXPECT_EQ(metadata[0].numChildren, 0);
}

// Test TableMetadata with multiple fixed-width columns
TEST_F(TableMetadataTest, multipleFixedWidthColumns) {
  const cudf::size_type numRows = 50;

  // Create INT32, FLOAT64, and INT64 columns
  auto col1 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      numRows,
      cudf::mask_state::UNALLOCATED,
      stream_);
  auto col2 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::FLOAT64},
      numRows,
      cudf::mask_state::UNALLOCATED,
      stream_);
  auto col3 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT64},
      numRows,
      cudf::mask_state::UNALLOCATED,
      stream_);

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(col1));
  columns.push_back(std::move(col2));
  columns.push_back(std::move(col3));
  cudf::table table(std::move(columns));

  auto serialized = TableMetadata::buildFromTable(table.view());
  ASSERT_NE(serialized, nullptr);

  EXPECT_EQ(TableMetadata::getNumRootColumns(*serialized), 3);

  auto metadata = TableMetadata::deserialize(*serialized);
  ASSERT_EQ(metadata.size(), 3);

  // Verify INT32 column
  EXPECT_EQ(metadata[0].typeId, cudf::type_id::INT32);
  EXPECT_EQ(metadata[0].size, numRows);
  EXPECT_EQ(metadata[0].dataSize, numRows * sizeof(int32_t));

  // Verify FLOAT64 column
  EXPECT_EQ(metadata[1].typeId, cudf::type_id::FLOAT64);
  EXPECT_EQ(metadata[1].size, numRows);
  EXPECT_EQ(metadata[1].dataSize, numRows * sizeof(double));

  // Verify INT64 column
  EXPECT_EQ(metadata[2].typeId, cudf::type_id::INT64);
  EXPECT_EQ(metadata[2].size, numRows);
  EXPECT_EQ(metadata[2].dataSize, numRows * sizeof(int64_t));
}

// Test TableMetadata with nullable column
TEST_F(TableMetadataTest, nullableColumn) {
  const cudf::size_type numRows = 100;

  // Create a nullable INT32 column (all nulls allocated but set to valid)
  auto col = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      numRows,
      cudf::mask_state::ALL_VALID,
      stream_);

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(col));
  cudf::table table(std::move(columns));

  auto serialized = TableMetadata::buildFromTable(table.view());
  auto metadata = TableMetadata::deserialize(*serialized);
  ASSERT_EQ(metadata.size(), 1);

  EXPECT_EQ(metadata[0].typeId, cudf::type_id::INT32);
  EXPECT_EQ(metadata[0].size, numRows);
  EXPECT_EQ(metadata[0].nullCount, 0);
  // Null mask should be allocated (even if all valid)
  EXPECT_GT(metadata[0].nullMaskSize, 0);
}

// Test TableMetadata with empty table
TEST_F(TableMetadataTest, emptyTable) {
  const cudf::size_type numRows = 0;

  auto col = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      numRows,
      cudf::mask_state::UNALLOCATED,
      stream_);

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(col));
  cudf::table table(std::move(columns));

  auto serialized = TableMetadata::buildFromTable(table.view());
  auto metadata = TableMetadata::deserialize(*serialized);
  ASSERT_EQ(metadata.size(), 1);

  EXPECT_EQ(metadata[0].typeId, cudf::type_id::INT32);
  EXPECT_EQ(metadata[0].size, 0);
  EXPECT_EQ(metadata[0].dataSize, -1); // Empty column has no data
  EXPECT_EQ(metadata[0].nullMaskSize, -1);
}

// Test TableMetadata with string column (has children)
TEST_F(TableMetadataTest, stringColumn) {
  // Create a string column using the test helper
  std::vector<std::string> strings = {"hello", "world", "test"};
  auto str_col = make_strings_column_from_host(strings);

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(str_col));
  cudf::table table(std::move(columns));

  auto serialized = TableMetadata::buildFromTable(table.view());
  ASSERT_NE(serialized, nullptr);

  EXPECT_EQ(TableMetadata::getNumRootColumns(*serialized), 1);

  auto metadata = TableMetadata::deserialize(*serialized);
  // String column has children (implementation-dependent, at least 1)
  ASSERT_GE(metadata.size(), 1);

  EXPECT_EQ(metadata[0].typeId, cudf::type_id::STRING);
  EXPECT_EQ(metadata[0].size, 3);
  // STRING columns store chars in data buffer. "hello" + "world" + "test" = 14 chars
  EXPECT_EQ(metadata[0].dataSize, 14);
  EXPECT_GE(metadata[0].numChildren, 1); // At least 1 child (offsets)
}

// Test error handling for too-small buffer in deserialize
TEST_F(TableMetadataTest, deserializeBufferTooSmall) {
  std::vector<uint8_t> tooSmall(4); // Less than header size
  EXPECT_THROW(TableMetadata::deserialize(tooSmall), std::runtime_error);
}

// Test error handling for getNumRootColumns with too-small buffer
TEST_F(TableMetadataTest, getNumRootColumnsBufferTooSmall) {
  std::vector<uint8_t> tooSmall(2);
  EXPECT_THROW(TableMetadata::getNumRootColumns(tooSmall), std::runtime_error);
}

// Test serialized size is consistent
TEST_F(TableMetadataTest, serializedSizeConsistent) {
  // 4*int32_t (typeId, typeScale, size, nullCount) + 2*int64_t (dataSize,
  // nullMaskSize) + 1*int32_t (numChildren) = 36 bytes
  EXPECT_EQ(ColumnMetadata::serializedSize(), 36);
}

// Test TableMetadata with mixed columns (INT32, STRING, FLOAT64)
TEST_F(TableMetadataTest, mixedColumnsTable) {
  const cudf::size_type numRows = 10;

  // Create INT32 column
  auto col1 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32},
      numRows,
      cudf::mask_state::UNALLOCATED,
      stream_);

  // Create STRING column
  std::vector<std::string> strings(numRows, "test");
  auto col2 = make_strings_column_from_host(strings);

  // Create FLOAT64 column
  auto col3 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::FLOAT64},
      numRows,
      cudf::mask_state::UNALLOCATED,
      stream_);

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(col1));
  columns.push_back(std::move(col2));
  columns.push_back(std::move(col3));
  cudf::table table(std::move(columns));

  auto serialized = TableMetadata::buildFromTable(table.view());
  ASSERT_NE(serialized, nullptr);

  EXPECT_EQ(TableMetadata::getNumRootColumns(*serialized), 3);

  auto metadata = TableMetadata::deserialize(*serialized);
  // 3 root columns + children for STRING column
  ASSERT_GE(metadata.size(), 3);

  // First column: INT32
  EXPECT_EQ(metadata[0].typeId, cudf::type_id::INT32);
  EXPECT_EQ(metadata[0].size, numRows);
  EXPECT_EQ(metadata[0].numChildren, 0);

  // Second column: STRING (index 1 in depth-first order)
  EXPECT_EQ(metadata[1].typeId, cudf::type_id::STRING);
  EXPECT_EQ(metadata[1].size, numRows);
  EXPECT_GE(metadata[1].numChildren, 1); // At least 1 child

  // Skip the STRING children and check FLOAT64
  // Find the last column which should be FLOAT64
  // Due to depth-first traversal, FLOAT64 is after STRING + its children
  size_t floatIdx = 1 + 1 + metadata[1].numChildren; // STRING + its children
  ASSERT_LT(floatIdx, metadata.size());
  EXPECT_EQ(metadata[floatIdx].typeId, cudf::type_id::FLOAT64);
  EXPECT_EQ(metadata[floatIdx].size, numRows);
  EXPECT_EQ(metadata[floatIdx].numChildren, 0);
}

// Test that serialization header contains correct counts
TEST_F(TableMetadataTest, headerContainsCorrectCounts) {
  const cudf::size_type numRows = 5;

  // Create 3 simple columns
  auto col1 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32}, numRows,
      cudf::mask_state::UNALLOCATED, stream_);
  auto col2 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT64}, numRows,
      cudf::mask_state::UNALLOCATED, stream_);
  auto col3 = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::FLOAT32}, numRows,
      cudf::mask_state::UNALLOCATED, stream_);

  std::vector<std::unique_ptr<cudf::column>> columns;
  columns.push_back(std::move(col1));
  columns.push_back(std::move(col2));
  columns.push_back(std::move(col3));
  cudf::table table(std::move(columns));

  auto serialized = TableMetadata::buildFromTable(table.view());

  // Verify header - first 4 bytes are numRootColumns
  int32_t numRootColumns;
  std::memcpy(&numRootColumns, serialized->data(), sizeof(int32_t));
  EXPECT_EQ(numRootColumns, 3);

  // Second 4 bytes are totalColumns
  int32_t totalColumns;
  std::memcpy(&totalColumns, serialized->data() + sizeof(int32_t), sizeof(int32_t));
  EXPECT_EQ(totalColumns, 3); // 3 simple columns with no children

  // Verify deserialization returns correct count
  auto metadata = TableMetadata::deserialize(*serialized);
  EXPECT_EQ(metadata.size(), 3);
}
