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
#include "velox/experimental/cudf-exchange/StateTransitionMetrics.h"

namespace facebook::velox::cudf_exchange {

void StateTransitionMetrics::recordTransition(
    const std::string& fromState,
    const std::string& toState,
    const std::chrono::time_point<std::chrono::high_resolution_clock>&
        startTime,
    const std::chrono::time_point<std::chrono::high_resolution_clock>& endTime,
    uint64_t bytes) {
  auto duration = endTime - startTime;
  auto durationMicros =
      std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  transitionHistory_.push_back(
      TransitionRecord{fromState, toState, durationMicros, bytes});

  auto key = std::make_pair(fromState, toState);
  auto& stats = transitionStats_[key];

  if (transitionStats_.count(key) == 1 && stats.count == 0) {
    // First occurrence
    stats.totalDurationMicros = durationMicros;
    stats.count = 1;
    stats.minDurationMicros = durationMicros;
    stats.maxDurationMicros = durationMicros;
    stats.totalBytes = bytes;
    stats.minBytes = bytes;
    stats.maxBytes = bytes;
  } else {
    // Subsequent occurrence
    stats.totalDurationMicros += durationMicros;
    stats.count++;
    stats.minDurationMicros = std::min(stats.minDurationMicros, durationMicros);
    stats.maxDurationMicros = std::max(stats.maxDurationMicros, durationMicros);
    stats.totalBytes += bytes;
    stats.minBytes = std::min(stats.minBytes, bytes);
    stats.maxBytes = std::max(stats.maxBytes, bytes);
  }
}

std::chrono::time_point<std::chrono::high_resolution_clock>
StateTransitionMetrics::startTransition(
    const std::string& fromState,
    const std::string& toState,
    uint64_t bytes) {
  pendingFromState_ = fromState;
  pendingToState_ = toState;
  pendingBytes_ = bytes;
  return std::chrono::high_resolution_clock::now();
}

void StateTransitionMetrics::endTransition(
    const std::chrono::time_point<std::chrono::high_resolution_clock>&
        startTime,
    uint64_t bytes) {
  auto endTime = std::chrono::high_resolution_clock::now();
  // Use provided bytes if non-zero, otherwise use pendingBytes_
  uint64_t bytesToRecord = (bytes != 0) ? bytes : pendingBytes_;
  recordTransition(
      pendingFromState_, pendingToState_, startTime, endTime, bytesToRecord);
}

const StateTransitionMetrics::TransitionStats*
StateTransitionMetrics::getTransitionStats(
    const std::string& fromState,
    const std::string& toState) const {
  auto key = std::make_pair(fromState, toState);
  auto it = transitionStats_.find(key);
  if (it != transitionStats_.end()) {
    return &it->second;
  }
  return nullptr;
}

void StateTransitionMetrics::clear() {
  transitionHistory_.clear();
  transitionStats_.clear();
  pendingFromState_.clear();
  pendingToState_.clear();
  pendingBytes_ = 0;
}

std::string StateTransitionMetrics::toString() const {
  std::stringstream out;
  out << "StateTransitionMetrics:\n";

  if (transitionStats_.empty()) {
    out << "  No transitions recorded.\n";
    return out.str();
  }

  out << "  Total transitions recorded: " << transitionHistory_.size() << "\n";
  out << "\n  Statistics:\n";

  // Aggregated Statistics with integrated individual transition details
  for (const auto& [key, stats] : transitionStats_) {
    out << "  " << key.first << " -> " << key.second << ":\n";
    out << "    Count: " << stats.count << "\n";
    out << "    Total duration: " << stats.totalDurationMicros << " µs\n";
    out << "    Average duration: " << stats.getAverageMicros() << " µs\n";
    out << "    Min duration: " << stats.minDurationMicros << " µs\n";
    out << "    Max duration: " << stats.maxDurationMicros << " µs\n";
    out << "    Total bytes: " << stats.totalBytes << "\n";
    out << "    Average bytes: " << stats.getAverageBytes() << "\n";
    out << "    Min bytes: " << stats.minBytes << "\n";
    out << "    Max bytes: " << stats.maxBytes << "\n";
    // Only output throughput if bytes were transferred
    if (stats.totalBytes > 0) {
      out << "    Overall throughput: " << stats.getThroughputMBps()
          << " MB/s\n";
      out << "    Average throughput: " << stats.getAverageThroughputMBps()
          << " MB/s\n";
    }
    // List individual transitions for this transition type in CSV format
    out << "    Individual transitions:\n";
    out << "      Index, Duration µs, Bytes, Throughput MB/s\n";
    size_t count = 0;
    for (size_t i = 0; i < transitionHistory_.size(); ++i) {
      const auto& record = transitionHistory_[i];
      if (record.fromState == key.first && record.toState == key.second) {
        out << "      " << count << ", " << record.durationMicros << ", "
            << record.bytes;
        if (record.bytes > 0 && record.durationMicros > 0) {
          double throughput =
              (static_cast<double>(record.bytes) / record.durationMicros) *
              (1000000.0 / 1048576.0);
          out << ", " << throughput;
        }
        out << "\n";
        count++;
      }
    }
  }

  return out.str();
}

} // namespace facebook::velox::cudf_exchange
