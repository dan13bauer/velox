# State Transition Metrics

## Overview

The `StateTransitionMetrics` class tracks timing information for state transitions in the `CudfExchangeServer`. It measures both individual transition times and provides cumulative statistics for repeated transitions.

## Motivation

The `CudfExchangeServer` is a state machine that manages the transfer of CUDA buffers to remote clients. It progresses through the following states:

1. **Created** - Initial state
2. **ReadyToTransfer** - Ready to fetch data from the queue
3. **WaitingForDataFromQueue** - Waiting for data to become available
4. **DataReady** - Data has arrived and is ready to be sent
5. **WaitingForSendComplete** - Waiting for the send operation to complete
6. **Done** - Transfer complete, cleaning up

Understanding how long each transition takes can help identify performance bottlenecks and optimize the exchange process.

## Architecture

### StateTransitionMetrics Class

The metrics class provides:

- **Individual transition recording**: Record each state transition with its duration
- **Aggregated statistics**: Collect min, max, average, and total duration for each transition type
- **Transition history**: Maintain a chronological record of all transitions
- **Query interface**: Get statistics for specific transitions or all transitions

### Integration with CudfExchangeServer

The `CudfExchangeServer` integrates metrics through:

1. **Automatic tracking**: Every call to `setState()` automatically records the transition time
2. **Public getter**: Access metrics via `getStateTransitionMetrics()`
3. **lastStateChangeTime_**: Tracks when the last state transition occurred

## Usage Examples

### Basic Usage

```cpp
#include "velox/experimental/cudf-exchange/StateTransitionMetrics.h"

StateTransitionMetrics metrics;

// Record a transition that took 100 microseconds
auto start = std::chrono::high_resolution_clock::now();
// ... do some work ...
auto end = std::chrono::high_resolution_clock::now();
metrics.recordTransition("Created", "ReadyToTransfer", start, end);

// Query statistics
auto stats = metrics.getTransitionStats("Created", "ReadyToTransfer");
if (stats) {
  std::cout << "Count: " << stats->count << std::endl;
  std::cout << "Total: " << stats->totalDurationMicros << " µs" << std::endl;
  std::cout << "Average: " << stats->getAverageMicros() << " µs" << std::endl;
  std::cout << "Min: " << stats->minDurationMicros << " µs" << std::endl;
  std::cout << "Max: " << stats->maxDurationMicros << " µs" << std::endl;
}
```

### Using startTransition/endTransition

```cpp
StateTransitionMetrics metrics;

// Record transition with automatic timing
auto startTime = metrics.startTransition("DataReady", "WaitingForSendComplete");
// ... do work ...
metrics.endTransition(startTime);
```

### Accessing Metrics from CudfExchangeServer

```cpp
// Get metrics from a server instance
const auto& metrics = server->getStateTransitionMetrics();

// Print all transition statistics
std::cout << metrics.toString() << std::endl;

// Get specific transition stats
auto stats = metrics.getTransitionStats("ReadyToTransfer", "WaitingForDataFromQueue");
if (stats) {
  double avgTime = stats->getAverageMicros();
  uint64_t count = stats->count;
  // Use the statistics for monitoring/alerting
}

// Get complete transition history
for (const auto& record : metrics.getTransitionHistory()) {
  std::cout << record.fromState << " -> " << record.toState
            << ": " << record.durationMicros << " µs" << std::endl;
}
```

## Output Format

The `toString()` method produces formatted output:

```
StateTransitionMetrics:
  Total transitions recorded: 12

  Aggregated Statistics:
  ReadyToTransfer -> WaitingForDataFromQueue:
    Count: 3
    Total duration: 150000 µs
    Average duration: 50000 µs
    Min duration: 45000 µs
    Max duration: 55000 µs
  WaitingForDataFromQueue -> DataReady:
    Count: 3
    Total duration: 300000 µs
    Average duration: 100000 µs
    Min duration: 95000 µs
    Max duration: 105000 µs
  ...
```

## Performance Considerations

1. **Minimal Overhead**: Metrics collection adds minimal overhead due to efficient use of high-resolution clock
2. **Memory Usage**: Memory usage scales linearly with the number of unique transitions recorded
3. **Thread Safety**: The current implementation is not thread-safe; use external synchronization if needed in multi-threaded contexts
4. **Clear Method**: Call `clear()` to reset metrics and free memory

## Typical Transition Pattern

When transferring multiple buffers, the server cycles through the states:

```
Created
   ↓
ReadyToTransfer ←──┐
   ↓               │
WaitingForDataFromQueue (upcall triggers on data arrival)
   ↓               │
DataReady          │
   ↓               │
WaitingForSendComplete (upcall triggers on send completion)
   ↓               │
ReadyToTransfer ───┘ (cycle repeats for next buffer)
   ...
   ↓
Done
```

For the first buffer transfer:
- Created → ReadyToTransfer
- ReadyToTransfer → WaitingForDataFromQueue
- WaitingForDataFromQueue → DataReady (triggered by data availability)
- DataReady → WaitingForSendComplete
- WaitingForSendComplete → ReadyToTransfer

For subsequent buffers, the cycle repeats from ReadyToTransfer.

## Testing

The `StateTransitionMetricsTest.cpp` file contains comprehensive unit tests covering:

- Single transition recording
- Multiple transitions of the same type
- Multiple different transitions
- Min/max duration tracking
- Average duration calculation
- Transition history
- Clear functionality
- Formatted output
