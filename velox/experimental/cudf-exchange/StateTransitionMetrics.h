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

#include <chrono>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace facebook::velox::cudf_exchange {

/// @brief Tracks state transition timing metrics.
/// Measures individual transition times and cumulative times for each unique
/// state transition pair. For example, transitions from "ReadyToTransfer" to
/// "WaitingForDataFromQueue" are all grouped together with their cumulative
/// time and count.
class StateTransitionMetrics {
 public:
  /// @brief Represents a single state transition with timing and byte
  /// information.
  struct TransitionRecord {
    std::string fromState;
    std::string toState;
    int64_t durationMicros; // Duration in microseconds
    uint64_t bytes; // Number of bytes transferred during this transition
  };

  /// @brief Represents aggregated metrics for a transition type.
  struct TransitionStats {
    int64_t totalDurationMicros; // Cumulative time in microseconds
    uint64_t count; // Number of times this transition occurred
    int64_t minDurationMicros;
    int64_t maxDurationMicros;
    uint64_t totalBytes; // Total bytes transferred across all transitions
    uint64_t minBytes;
    uint64_t maxBytes;

    double getAverageMicros() const {
      return count > 0 ? static_cast<double>(totalDurationMicros) / count : 0.0;
    }

    double getAverageBytes() const {
      return count > 0 ? static_cast<double>(totalBytes) / count : 0.0;
    }

    /// @brief Calculate throughput in MB/s for this transition type.
    /// @return Throughput in megabytes per second.
    double getThroughputMBps() const {
      if (totalDurationMicros == 0)
        return 0.0;
      // Convert: (bytes / microseconds) * (1000000 microseconds / second) /
      // (1048576 bytes / MB)
      return (static_cast<double>(totalBytes) / totalDurationMicros) *
          (1000000.0 / 1048576.0);
    }

    /// @brief Calculate throughput in MB/s for average transition.
    /// @return Average throughput per transition in megabytes per second.
    double getAverageThroughputMBps() const {
      double avgMicros = getAverageMicros();
      if (avgMicros == 0.0)
        return 0.0;
      double avgBytes = getAverageBytes();
      return (avgBytes / avgMicros) * (1000000.0 / 1048576.0);
    }
  };

  StateTransitionMetrics() = default;

  /// @brief Records a state transition with timing and byte count.
  /// @param fromState The source state name.
  /// @param toState The destination state name.
  /// @param startTime The time point when the transition started.
  /// @param endTime The time point when the transition ended.
  /// @param bytes Number of bytes transferred during this transition. Default
  /// is 0.
  void recordTransition(
      const std::string& fromState,
      const std::string& toState,
      const std::chrono::time_point<std::chrono::high_resolution_clock>&
          startTime,
      const std::chrono::time_point<std::chrono::high_resolution_clock>&
          endTime,
      uint64_t bytes = 0);

  /// @brief Records a state transition, measuring from now to a future point.
  /// @param fromState The source state name.
  /// @param toState The destination state name.
  /// @param bytes Number of bytes to associate with this transition. Default is
  /// 0. Captures the current time point as the start of the transition.
  std::chrono::time_point<std::chrono::high_resolution_clock> startTransition(
      const std::string& fromState,
      const std::string& toState,
      uint64_t bytes = 0);

  /// @brief Completes a transition recording started with startTransition.
  /// @param startTime The time point returned from startTransition.
  /// @param bytes Optional: override the bytes value provided in
  /// startTransition.
  void endTransition(
      const std::chrono::time_point<std::chrono::high_resolution_clock>&
          startTime,
      uint64_t bytes = 0);

  /// @brief Gets statistics for a specific transition.
  /// @param fromState The source state.
  /// @param toState The destination state.
  /// @return TransitionStats if the transition exists, nullptr otherwise.
  const TransitionStats* getTransitionStats(
      const std::string& fromState,
      const std::string& toState) const;

  /// @brief Gets all recorded individual transitions.
  const std::vector<TransitionRecord>& getTransitionHistory() const {
    return transitionHistory_;
  }

  /// @brief Gets aggregated statistics for all transitions.
  const std::map<std::pair<std::string, std::string>, TransitionStats>&
  getAllStats() const {
    return transitionStats_;
  }

  /// @brief Sets the byte count for the pending transition (used with
  /// startTransition/endTransition).
  /// @param bytes Number of bytes to associate with the next endTransition
  /// call.
  void setTransitionBytes(uint64_t bytes) {
    pendingBytes_ = bytes;
  }

  /// @brief Clears all recorded metrics.
  void clear();

  /// @brief Returns a formatted string with all transition statistics.
  std::string toString() const;

 private:
  std::string makeKey(const std::string& fromState, const std::string& toState)
      const {
    return fromState + " -> " + toState;
  }

  std::vector<TransitionRecord> transitionHistory_;
  std::map<std::pair<std::string, std::string>, TransitionStats>
      transitionStats_;
  std::string pendingFromState_;
  std::string pendingToState_;
  uint64_t pendingBytes_{0};
};

} // namespace facebook::velox::cudf_exchange
