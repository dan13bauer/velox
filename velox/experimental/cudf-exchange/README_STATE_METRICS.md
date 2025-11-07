# State Transition Metrics for CudfExchangeServer

## Quick Start

The `StateTransitionMetrics` class automatically tracks the time it takes to transition between different states in the `CudfExchangeServer` state machine. No manual instrumentation is needed—all transitions are measured automatically.

### Access Metrics

```cpp
auto server = // ... get server instance
const auto& metrics = server->getStateTransitionMetrics();

// Print all metrics
std::cout << metrics.toString() << std::endl;

// Query specific transition
if (auto stats = metrics.getTransitionStats("ReadyToTransfer", "WaitingForDataFromQueue")) {
  std::cout << "Average wait: " << stats->getAverageMicros() << " µs\n";
  std::cout << "Max wait: " << stats->maxDurationMicros << " µs\n";
}
```

## Files Added

| File | Purpose |
|------|---------|
| `StateTransitionMetrics.h` | Main metrics class header |
| `StateTransitionMetrics.cpp` | Implementation |
| `tests/StateTransitionMetricsTest.cpp` | Comprehensive unit tests (13 tests) |
| `STATE_TRANSITION_METRICS.md` | Detailed documentation |
| `INTEGRATION_EXAMPLES.md` | Real-world usage examples |

## Files Modified

| File | Changes |
|------|---------|
| `CudfExchangeServer.h` | Added metrics member and getter; modified setState() |
| `CudfExchangeServer.cpp` | No changes needed (state transitions tracked automatically) |
| `CMakeLists.txt` (main) | Added StateTransitionMetrics.cpp |
| `CMakeLists.txt` (tests) | Added StateTransitionMetricsTest.cpp |

## Automatically Tracked Transitions

The state machine measures time for all transitions:

```
Created
  ↓ (Automatic)
ReadyToTransfer
  ↓ (Automatic)
WaitingForDataFromQueue ← (Callback triggers transition when data arrives)
  ↓ (Automatic)
DataReady
  ↓ (Automatic)
WaitingForSendComplete ← (Callback triggers when send completes)
  ↓ (Automatic back to ReadyToTransfer for next buffer OR to Done)
Done
```

## Key Metrics

For each transition type, you get:

- **Count**: How many times this transition occurred
- **Total Duration**: Cumulative microseconds
- **Average Duration**: Mean time per transition
- **Min Duration**: Fastest transition
- **Max Duration**: Slowest transition

## Example Output

```
StateTransitionMetrics:
  Total transitions recorded: 12

  Aggregated Statistics:
  Created -> ReadyToTransfer:
    Count: 1
    Total duration: 5000 µs
    Average duration: 5000 µs
    Min duration: 5000 µs
    Max duration: 5000 µs
  ReadyToTransfer -> WaitingForDataFromQueue:
    Count: 3
    Total duration: 45000 µs
    Average duration: 15000 µs
    Min duration: 10000 µs
    Max duration: 20000 µs
  WaitingForDataFromQueue -> DataReady:
    Count: 3
    Total duration: 90000 µs
    Average duration: 30000 µs
    Min duration: 25000 µs
    Max duration: 35000 µs
  DataReady -> WaitingForSendComplete:
    Count: 3
    Total duration: 3000 µs
    Average duration: 1000 µs
    Min duration: 900 µs
    Max duration: 1200 µs
  WaitingForSendComplete -> ReadyToTransfer:
    Count: 3
    Total duration: 500000 µs
    Average duration: 166666.7 µs
    Min duration: 150000 µs
    Max duration: 180000 µs
  ReadyToTransfer -> Done:
    Count: 1
    Total duration: 2000 µs
    Average duration: 2000 µs
    Min duration: 2000 µs
    Max duration: 2000 µs
```

## API Reference

### Recording Transitions

```cpp
// Manual recording with explicit timing
metrics.recordTransition("FromState", "ToState", startTime, endTime);

// Or use the two-phase API
auto startTime = metrics.startTransition("FromState", "ToState");
// ... do work ...
metrics.endTransition(startTime);
```

### Querying Metrics

```cpp
// Get stats for a specific transition
const auto* stats = metrics.getTransitionStats("From", "To");
if (stats) {
  // Use stats->count, stats->totalDurationMicros, etc.
}

// Get all stats
for (const auto& [key, stats] : metrics.getAllStats()) {
  std::cout << key.first << " -> " << key.second
            << ": " << stats.getAverageMicros() << " µs avg\n";
}

// Get transition history
for (const auto& record : metrics.getTransitionHistory()) {
  std::cout << record.fromState << " -> " << record.toState
            << ": " << record.durationMicros << " µs\n";
}

// Get formatted output
std::cout << metrics.toString();

// Clear all metrics
metrics.clear();
```

## Performance Characteristics

- **CPU Overhead**: Minimal (single `clock_gettime` call per transition)
- **Memory Overhead**: O(n) where n = number of unique transition pairs
- **Thread Safety**: Not inherently thread-safe; use external synchronization if needed
- **No Allocations in Hot Path**: Statistics updated in-place

## Use Cases

1. **Performance Debugging**: Identify which state transitions are slow
2. **Capacity Planning**: Understand typical timing patterns
3. **SLA Monitoring**: Track if transitions meet performance targets
4. **Regression Detection**: Compare metrics across runs
5. **Profiling**: Identify optimization opportunities

## Testing

Run the comprehensive test suite:

```bash
cd /gpfs/zc2/u/dnb/velox
mkdir -p build && cd build
cmake ..
make cudf_exchange_test
./velox/experimental/cudf-exchange/tests/cudf_exchange_test
```

The test file `StateTransitionMetricsTest.cpp` contains 13 tests covering:
- Single and multiple transitions
- Statistics aggregation
- Min/max/average calculations
- History tracking
- Output formatting
- Edge cases

## Documentation

- **STATE_TRANSITION_METRICS.md**: Detailed design and usage documentation
- **INTEGRATION_EXAMPLES.md**: Real-world code examples showing various use patterns

## Architecture Decision: Automatic Measurement

The implementation modifies the `setState()` method to automatically record transitions. This was chosen because:

1. **Zero Manual Effort**: No need to instrument individual state transitions
2. **Consistency**: All transitions are measured uniformly
3. **No Missed Transitions**: Centralized measurement prevents gaps
4. **Maintainability**: Adding new states automatically gets measured

## Internal Implementation Details

```cpp
// In CudfExchangeServer.h
class CudfExchangeServer {
  void setState(ServerState newState) {
    ServerState oldState = state_.load(std::memory_order_seq_cst);
    if (oldState != newState) {
      // Automatically record the transition
      stateMetrics_.recordTransition(
          getStateNameForEnum(oldState),
          getStateNameForEnum(newState),
          lastStateChangeTime_,
          std::chrono::high_resolution_clock::now());
      lastStateChangeTime_ = std::chrono::high_resolution_clock::now();
    }
    state_.store(newState, std::memory_order_seq_cst);
  }

  StateTransitionMetrics stateMetrics_;
  std::chrono::time_point<std::chrono::high_resolution_clock> lastStateChangeTime_;
};
```

## Future Enhancements

Potential additions:
- Percentile tracking (p50, p95, p99)
- Thread-safe recording with atomics
- Metrics export (Prometheus, OpenTelemetry)
- Automatic anomaly detection
- Performance trend analysis
- Integration with distributed tracing systems

## Questions?

See `INTEGRATION_EXAMPLES.md` for detailed usage patterns and real-world scenarios.
