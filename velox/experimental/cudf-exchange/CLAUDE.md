- Build velox unit tests in /gpfs/zc2/u/dnb/presto/presto-native-execution/velox using this command: cmake --build _build/release -j --target=cudf_exchange_test
- To release UCXX ports, set environment variable export UCX_TCP_CM_REUSEADDR=y before running any unit test.
- Please set the environment variable CUDA_VISIBLE_DEVICES=7 to pin the test to GPU 7 when running the cudf_exchange_test

## Per-Column Transfer Implementation (Dec 2025)

Implemented per-column transfer with single stream and single UCXX tag per table:

### Key Design Decisions:
- **Single stream per table**: All column buffers within a table are allocated using the same CUDA stream
- **Single UCXX tag per table**: All column buffer transfers within a table use the same tag. Tags change between tables but not within a table. Since columns use a single stream, transfer sequence is maintained.
- **Stream propagation**: The stream used for allocation is stored with the table in the queue and propagated to CudfExchange for synchronization

### Files Modified:

1. **CudfExchangeQueue.h/.cpp**: Added `TableWithStream` struct that holds both the table and the CUDA stream used to allocate its buffers.

2. **CudfExchangeSource.cpp**:
   - Changed from per-column UCXX tags to a single tag (`tagRecv`) for all column transfers within a table
   - Stream is captured in lambda closures and passed through callbacks to `onPerColumnDataComplete()` and `enqueue()`

3. **CudfExchangeServer.cpp**: Changed from per-column UCXX tags to a single tag (`tagSend`) for all column transfers within a table

4. **CudfExchangeClient.h/.cpp**: Updated `next()` to return `TableWithStream` instead of `TablePtr`

5. **CudfExchange.h/.cpp**: Changed to use the stream from the queue when creating `CudfVector` instead of allocating a new stream

6. **ExchangeClientFacade.h**: Updated `ResultVariant` to use `TableWithStream`

7. **HybridExchange.h/.cpp**: Updated to use `TableWithStream` throughout

- Find the cudf includes in this directory: _build/release/_deps/cudf-src/cpp/include/cudf

## Zero-Copy Split Optimization (Dec 2025) - COMPLETED

**Status**: All 34 tests pass (6 bandwidth tests skipped by default)

### Goal:
Optimize CudfPartitionedOutput's multi-partition path using `cudf::split` (zero-copy) instead of `contiguous_split` (copies data). This eliminates one data copy per partition by having all partitions share ownership of the source table.

### Key Design Changes:

1. **TableWithMetadata structure changed** (CudfQueues.h):
   - Old: `std::unique_ptr<cudf::table> table` - owned table
   - New: `cudf::table_view tableView` + `std::shared_ptr<cudf::table> sourceTable`
   - Multiple partitions can share the same sourceTable via shared_ptr
   - tableView is a view into the sourceTable's data

2. **CudfPartitionedOutput changes**:
   - Uses `cudf::split()` instead of `cudf::contiguous_split()` for zero-copy splitting
   - `splitAndEnqueue()` signature changed to accept `std::shared_ptr<cudf::table>`
   - `equalPartition()` signature changed to accept `std::shared_ptr<cudf::table>`
   - Single-partition path also uses tableView + sourceTable pattern for consistency

3. **CudfExchangeServer changes**:
   - Sends from `column_view` data pointers instead of releasing owned columns
   - Uses `const_cast<void*>()` since UCXX tagSend expects non-const pointer
   - Captures `sourceTable` in UCXX callbacks to keep data alive during transfer
   - Handles offset-adjusted data pointers for split views

### Files Modified:

1. **CudfQueues.h**: Changed TableWithMetadata from owned table to view + shared source:
   ```cpp
   struct TableWithMetadata {
     std::unique_ptr<std::vector<uint8_t>> metadata;
     cudf::table_view tableView;
     std::shared_ptr<cudf::table> sourceTable;
     cudf::size_type numRows;
   };
   ```

2. **CudfPartitionedOutput.h**: Changed function signatures for splitAndEnqueue and equalPartition

3. **CudfPartitionedOutput.cpp**:
   - `hashPartition()`: Syncs stream, converts result to shared_ptr, calls splitAndEnqueue
   - `equalPartition()`: Takes shared_ptr<table>, calls splitAndEnqueue
   - `splitAndEnqueue()`: Uses cudf::split() for zero-copy, all partitions share sourceTable
   - Single partition path: Releases table from CudfVector, uses tableView + sourceTable

4. **CudfExchangeServer.cpp**:
   - `sendData()`: Iterates over tableView columns using column_view
   - Sends data from offset-adjusted pointers (col.head() + offset * element_size)
   - Captures sourceTable in all UCXX callbacks
   - Fixed const-correctness with const_cast for tagSend

5. **Test files updated**:
   - tests/CudfOutputQueueManagerTest.cpp: makeTableWithMetadata uses new pattern
   - tests/CudfPartitionedOutputMock.cpp: Uses tableView + sourceTable
   - tests/ExchangeServerMain.cpp: Uses tableView + sourceTable

### Additional Fixes Applied:

1. **gpuDataSize() fix for variable-width types**: Added `cudf::is_fixed_width()` check and recursive traversal to handle STRING columns correctly.

2. **STRING column handling in sendColumnView**: Added special handling for STRING columns using `cudf::strings_column_view` to access chars data via `chars_begin()` and `chars_size()` instead of `head()`.

### Known Issues/Considerations:

1. **Offset handling for split views**: When cudf::split() creates views, non-first partitions have non-zero offsets. The server sends from offset-adjusted pointers. For null masks with non-word-aligned offsets, there may be edge cases.

2. **String columns**: Nested types like strings have child columns. The recursive sendColumnView function handles this by iterating over children.

3. **Memory lifetime**: The sourceTable shared_ptr must stay alive until all UCXX transfers complete. This is ensured by capturing it in all UCXX callbacks.