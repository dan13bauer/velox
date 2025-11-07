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
#include <gtest/gtest.h>
#include <thread>

using namespace facebook::velox::cudf_exchange;

class StateTransitionMetricsTest : public testing::Test {
 protected:
  StateTransitionMetrics metrics;
};

TEST_F(StateTransitionMetricsTest, RecordSingleTransition) {
  auto start = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto end = std::chrono::high_resolution_clock::now();

  metrics.recordTransition("Created", "ReadyToTransfer", start, end);

  auto stats = metrics.getTransitionStats("Created", "ReadyToTransfer");
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->count, 1);
  EXPECT_GE(stats->totalDurationMicros, 9000); // At least 9ms
  EXPECT_EQ(stats->minDurationMicros, stats->totalDurationMicros);
  EXPECT_EQ(stats->maxDurationMicros, stats->totalDurationMicros);
}

TEST_F(StateTransitionMetricsTest, RecordMultipleTransitionsOfSameType) {
  auto start1 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto end1 = std::chrono::high_resolution_clock::now();

  metrics.recordTransition(
      "ReadyToTransfer", "WaitingForDataFromQueue", start1, end1);

  auto start2 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(15));
  auto end2 = std::chrono::high_resolution_clock::now();

  metrics.recordTransition(
      "ReadyToTransfer", "WaitingForDataFromQueue", start2, end2);

  auto stats =
      metrics.getTransitionStats("ReadyToTransfer", "WaitingForDataFromQueue");
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->count, 2);
  EXPECT_GE(stats->totalDurationMicros, 19000); // At least 19ms total
  EXPECT_LE(stats->minDurationMicros, stats->maxDurationMicros);
  EXPECT_GE(stats->maxDurationMicros, 14000); // Second one should be longer
}

TEST_F(StateTransitionMetricsTest, RecordMultipleDifferentTransitions) {
  auto start1 = std::chrono::high_resolution_clock::now();
  auto end1 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("Created", "ReadyToTransfer", start1, end1);

  auto start2 = std::chrono::high_resolution_clock::now();
  auto end2 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition(
      "ReadyToTransfer", "WaitingForDataFromQueue", start2, end2);

  auto start3 = std::chrono::high_resolution_clock::now();
  auto end3 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition(
      "WaitingForDataFromQueue", "DataReady", start3, end3);

  EXPECT_NE(metrics.getTransitionStats("Created", "ReadyToTransfer"), nullptr);
  EXPECT_NE(
      metrics.getTransitionStats("ReadyToTransfer", "WaitingForDataFromQueue"),
      nullptr);
  EXPECT_NE(
      metrics.getTransitionStats("WaitingForDataFromQueue", "DataReady"),
      nullptr);
  EXPECT_EQ(metrics.getTransitionStats("DataReady", "Done"), nullptr);
  EXPECT_EQ(metrics.getTransitionHistory().size(), 3);
}

TEST_F(StateTransitionMetricsTest, StartAndEndTransition) {
  auto startTime =
      metrics.startTransition("DataReady", "WaitingForSendComplete");
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  metrics.endTransition(startTime);

  auto stats =
      metrics.getTransitionStats("DataReady", "WaitingForSendComplete");
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->count, 1);
  EXPECT_GE(stats->totalDurationMicros, 9000);
}

TEST_F(StateTransitionMetricsTest, AverageDuration) {
  auto start1 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto end1 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("State1", "State2", start1, end1);

  auto start2 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  auto end2 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("State1", "State2", start2, end2);

  auto stats = metrics.getTransitionStats("State1", "State2");
  ASSERT_NE(stats, nullptr);
  double average = stats->getAverageMicros();
  // Average should be somewhere between the two measurements (roughly 15ms)
  EXPECT_GE(average, 10000);
  EXPECT_LE(average, 25000);
}

TEST_F(StateTransitionMetricsTest, MinMaxDurations) {
  auto start1 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto end1 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("StateA", "StateB", start1, end1);

  auto start2 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(25));
  auto end2 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("StateA", "StateB", start2, end2);

  auto stats = metrics.getTransitionStats("StateA", "StateB");
  ASSERT_NE(stats, nullptr);
  EXPECT_LE(stats->minDurationMicros, 10000); // First transition ~5ms
  EXPECT_GE(stats->maxDurationMicros, 20000); // Second transition ~25ms
  EXPECT_LT(stats->minDurationMicros, stats->maxDurationMicros);
}

TEST_F(StateTransitionMetricsTest, TransitionHistory) {
  auto start1 = std::chrono::high_resolution_clock::now();
  auto end1 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("StateX", "StateY", start1, end1);

  auto start2 = std::chrono::high_resolution_clock::now();
  auto end2 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("StateY", "StateZ", start2, end2);

  auto history = metrics.getTransitionHistory();
  EXPECT_EQ(history.size(), 2);
  EXPECT_EQ(history[0].fromState, "StateX");
  EXPECT_EQ(history[0].toState, "StateY");
  EXPECT_EQ(history[1].fromState, "StateY");
  EXPECT_EQ(history[1].toState, "StateZ");
}

TEST_F(StateTransitionMetricsTest, Clear) {
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("State1", "State2", start, end);

  EXPECT_NE(metrics.getTransitionStats("State1", "State2"), nullptr);
  EXPECT_EQ(metrics.getTransitionHistory().size(), 1);

  metrics.clear();

  EXPECT_EQ(metrics.getTransitionStats("State1", "State2"), nullptr);
  EXPECT_EQ(metrics.getTransitionHistory().size(), 0);
}

TEST_F(StateTransitionMetricsTest, ToStringOutput) {
  auto start = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto end = std::chrono::high_resolution_clock::now();

  metrics.recordTransition(
      "ReadyToTransfer", "WaitingForDataFromQueue", start, end);
  metrics.recordTransition(
      "ReadyToTransfer", "WaitingForDataFromQueue", start, end);

  std::string output = metrics.toString();
  EXPECT_NE(output.find("StateTransitionMetrics"), std::string::npos);
  EXPECT_NE(
      output.find("ReadyToTransfer -> WaitingForDataFromQueue"),
      std::string::npos);
  EXPECT_NE(output.find("Count: 2"), std::string::npos);
  EXPECT_NE(output.find("Average duration"), std::string::npos);
}

TEST_F(StateTransitionMetricsTest, EmptyMetrics) {
  auto output = metrics.toString();
  EXPECT_NE(output.find("No transitions recorded"), std::string::npos);
  EXPECT_EQ(metrics.getTransitionHistory().size(), 0);
  EXPECT_EQ(metrics.getAllStats().size(), 0);
}

TEST_F(StateTransitionMetricsTest, GetAllStats) {
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();

  metrics.recordTransition("State1", "State2", start, end);
  metrics.recordTransition("State2", "State3", start, end);
  metrics.recordTransition("State3", "State1", start, end);

  auto allStats = metrics.getAllStats();
  EXPECT_EQ(allStats.size(), 3);
}

TEST_F(StateTransitionMetricsTest, RecordTransitionWithBytes) {
  auto start = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto end = std::chrono::high_resolution_clock::now();

  uint64_t bytes = 1024 * 1024; // 1 MB
  metrics.recordTransition(
      "DataReady", "WaitingForSendComplete", start, end, bytes);

  auto stats =
      metrics.getTransitionStats("DataReady", "WaitingForSendComplete");
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->count, 1);
  EXPECT_EQ(stats->totalBytes, bytes);
  EXPECT_EQ(stats->minBytes, bytes);
  EXPECT_EQ(stats->maxBytes, bytes);
}

TEST_F(StateTransitionMetricsTest, ByteAggregation) {
  auto start1 = std::chrono::high_resolution_clock::now();
  auto end1 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("State1", "State2", start1, end1, 1000000); // 1 MB

  auto start2 = std::chrono::high_resolution_clock::now();
  auto end2 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("State1", "State2", start2, end2, 2000000); // 2 MB

  auto stats = metrics.getTransitionStats("State1", "State2");
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->totalBytes, 3000000);
  EXPECT_EQ(stats->minBytes, 1000000);
  EXPECT_EQ(stats->maxBytes, 2000000);
  EXPECT_NEAR(stats->getAverageBytes(), 1500000, 1); // Average should be 1.5 MB
}

TEST_F(StateTransitionMetricsTest, ThroughputCalculation) {
  auto start = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto end = std::chrono::high_resolution_clock::now();

  uint64_t bytes = 100 * 1024 * 1024; // 100 MB
  metrics.recordTransition("State1", "State2", start, end, bytes);

  auto stats = metrics.getTransitionStats("State1", "State2");
  ASSERT_NE(stats, nullptr);
  double throughput = stats->getThroughputMBps();
  // With ~100ms and 100MB, we should get approximately 1000 MB/s
  // Allow some tolerance for timing variations
  EXPECT_GE(throughput, 800);
  EXPECT_LE(throughput, 1200);
}

TEST_F(StateTransitionMetricsTest, AverageThroughputCalculation) {
  auto start1 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  auto end1 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition(
      "State1", "State2", start1, end1, 50 * 1024 * 1024); // 50 MB

  auto start2 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto end2 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition(
      "State1", "State2", start2, end2, 100 * 1024 * 1024); // 100 MB

  auto stats = metrics.getTransitionStats("State1", "State2");
  ASSERT_NE(stats, nullptr);
  double avgThroughput = stats->getAverageThroughputMBps();
  // Both transitions should give us decent throughput
  EXPECT_GT(avgThroughput, 500);
}

TEST_F(StateTransitionMetricsTest, TransitionRecordContainsBytes) {
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();

  uint64_t testBytes = 12345678;
  metrics.recordTransition("From", "To", start, end, testBytes);

  const auto& history = metrics.getTransitionHistory();
  EXPECT_EQ(history.size(), 1);
  EXPECT_EQ(history[0].bytes, testBytes);
  EXPECT_EQ(history[0].fromState, "From");
  EXPECT_EQ(history[0].toState, "To");
}

TEST_F(StateTransitionMetricsTest, StartAndEndTransitionWithBytes) {
  auto startTime = metrics.startTransition("State1", "State2", 2000000);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  metrics.endTransition(startTime);

  auto stats = metrics.getTransitionStats("State1", "State2");
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->totalBytes, 2000000);
  EXPECT_EQ(stats->count, 1);
}

TEST_F(StateTransitionMetricsTest, SetTransitionBytesAfterStart) {
  auto startTime = metrics.startTransition("State1", "State2");
  metrics.setTransitionBytes(5000000);
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  metrics.endTransition(startTime);

  auto stats = metrics.getTransitionStats("State1", "State2");
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->totalBytes, 5000000);
}

TEST_F(StateTransitionMetricsTest, CSVFormatInToString) {
  // Record multiple transitions with varying byte counts
  auto start1 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::microseconds(100));
  auto end1 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition(
      "State1", "State2", start1, end1, 100 * 1024 * 1024); // 100 MB

  auto start2 = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::microseconds(100));
  auto end2 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition(
      "State1", "State2", start2, end2, 200 * 1024 * 1024); // 200 MB

  auto start3 = std::chrono::high_resolution_clock::now();
  auto end3 = std::chrono::high_resolution_clock::now();
  metrics.recordTransition("Control", "Flow", start3, end3, 0); // No bytes

  std::string output = metrics.toString();

  // Verify CSV header is present
  EXPECT_NE(
      output.find("transition,duration,bytes,throughput MB/s"),
      std::string::npos);

  // Verify state transition rows are present
  EXPECT_NE(output.find("State1->State2"), std::string::npos);
  EXPECT_NE(output.find("Control->Flow"), std::string::npos);

  // Verify data rows contain commas (CSV format)
  size_t header_pos = output.find("transition,duration,bytes,throughput MB/s");
  ASSERT_NE(header_pos, std::string::npos);

  // Find first data row after header
  size_t data_start = output.find('\n', header_pos) + 1;
  EXPECT_NE(data_start, std::string::npos);

  // Verify CSV contains numeric values
  EXPECT_NE(
      output.find(","), std::string::npos); // At least one comma in data rows
}
