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