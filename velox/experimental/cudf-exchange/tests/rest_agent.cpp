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
#include <nlohmann/json.hpp>
#include <atomic>
#include <csignal>
#include <iostream>
#include "httplib.h"

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/text/RegisterTextWriter.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/experimental/cudf-exchange/Communicator.h"
#include "velox/experimental/cudf-exchange/CudfOutputQueueManager.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetConnector.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

#include <cuda_runtime.h>
#include <folly/init/Init.h>
#include <algorithm>
#include <atomic>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <string>

using json = nlohmann::json;

using namespace facebook::velox;
using namespace facebook::velox::exec;

std::string kDummyCoordinatorUrl{"localhost:1/nowhere"};

DEFINE_uint32(port, 24356 + 3, "Port number");

DEFINE_uint32(rest_port, 10000, "Rest Port number");

DEFINE_uint32(cudfChunkSizeGB, 1, "cuDF Parquet chunk size to read in GB");
DEFINE_int32(cuda_device, -1, "Cuda device or -1 for not setting the device");
DEFINE_bool(use_hive, false, "Use Hive Connector");
DEFINE_int64(queueSizeGb, 1, "Queue size");

std::atomic<bool> stopRequested(false);

void signalHandler(int signum) {
  std::cout << "\nSignal (" << signum
            << ") received. Shutting down gracefully...\n";
  stopRequested.store(true);
}

int main(int argc, char* argv[]) {
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  httplib::Server server;
  int nextTaskId = 0;
  std::unordered_map<std::string, std::shared_ptr<facebook::velox::exec::Task>>
      hashmap;
  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  // Default memory allocator used throughout this example.
  const memory::MemoryManagerOptions options;
  memory::MemoryManager::initialize(options);
  auto pool = memory::memoryManager()->addLeafPool();

  // Register file systems and connectors
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  const std::string kHiveConnectorId = "test-hive";

  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);
  parquet::registerParquetReaderFactory();

  // Register functions and serdes.
  filesystems::registerLocalFileSystem();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  // parse::registerTypeResolver();
  exec::ExchangeSource::registerFactory(
      facebook::velox::exec::test::createLocalExchangeSource);
  // Register the presto serialized/deserializer.
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    facebook::velox::serializer::CompactRowVectorSerde::
        registerNamedVectorSerde();
  }

  // Register the CUDF Parquet reader
  // It notices its existence with a hardcoded name "test-parquet"
  // set NOT means it will not be recognized
  const char* kCudfParquetConnectorName = "test-parquet";
  if (FLAGS_use_hive) {
    kCudfParquetConnectorName = "NOT-test-parquet";
    facebook::velox::cudf_velox::CudfOptions::getInstance()
        .setParquetConnectorRegistered(false);

  } else {
    facebook::velox::cudf_velox::CudfOptions::getInstance()
        .setParquetConnectorRegistered(true);
  }

  if (!facebook::velox::connector::hasConnectorFactory(
          kCudfParquetConnectorName)) {
    facebook::velox::connector::registerConnectorFactory(
        std::make_shared<facebook::velox::cudf_velox::connector::parquet::
                             ParquetConnectorFactory>(
            kCudfParquetConnectorName));
    // This is how ToCudf recognizes that we are using the
    // cudf-based parquet reader that produces Cudf vectors.
  }

  std::unordered_map<std::string, std::string> c = {};
  LOG(INFO) << "reading " << FLAGS_cudfChunkSizeGB << "GB chunks at once";

  long int cudfChunkSizeBytes =
      static_cast<long int>(FLAGS_cudfChunkSizeGB) * 1024L * 1024L * 1024L;

  c[facebook::velox::cudf_velox::connector::parquet::ParquetConfig::
        kMaxChunkReadLimit] = std::to_string(cudfChunkSizeBytes);
  std::shared_ptr<const facebook::velox::config::ConfigBase> properties =
      std::make_shared<const facebook::velox::config::ConfigBase>(std::move(c));
  std::shared_ptr<facebook::velox::connector::Connector> connector =
      facebook::velox::connector::getConnectorFactory(kCudfParquetConnectorName)
          ->newConnector(kCudfParquetConnectorName, std::move(properties));
  facebook::velox::connector::registerConnector(connector);

  // Enable cuDF operators
  facebook::velox::cudf_velox::registerCudf();

  auto communicator =
      cudf_exchange::Communicator::initAndGet(FLAGS_port, kDummyCoordinatorUrl);
  std::thread serverThread(
      &cudf_exchange::Communicator::run, communicator.get());

  server.Put(
      "/createTask",
      [&nextTaskId, &executor, &hashmap](
          const httplib::Request& req, httplib::Response& res) {
        try {
          auto selectedRowType =
              ROW({"station_name", "measurement"}, {VARCHAR(), DOUBLE()});
          core::PlanNodeId scanNodeId;
          int kNumDestinations = 1;

          auto readerPlan =
              exec::test::PlanBuilder()
                  .tableScan(asRowType(selectedRowType))
                  .capturePlanNodeId(scanNodeId)
                  .project({"station_name", "measurement"})
                  .partitionedOutput(
                      {}, // No partitioning key.
                      kNumDestinations, // just one destination.
                      std::vector<std::string>{
                          "station_name", "measurement"}, // output layout
                      VectorSerde::Kind::kCompactRow)
                  .planFragment();

          // std::unordered_map<std::string, std::string> configSettings;
          // auto queryCtx = core::QueryCtx::create(executor.get(),
          // core::QueryConfig(std::move(configSettings)));

          std::unordered_map<std::string, std::string> configSettings{
              {facebook::velox::core::QueryConfig::kMaxOutputBufferSize,
               std::to_string(FLAGS_queueSizeGb)}};
          auto queryCtx = core::QueryCtx::create(
              executor.get(), core::QueryConfig(std::move(configSettings)));

          std::cout << "Create a pool of " << std::to_string(FLAGS_queueSizeGb)
                    << std::endl;

          std::string taskId = "task" + std::to_string(nextTaskId);
          auto readerTask = exec::Task::create(
              taskId,
              readerPlan,
              0,
              queryCtx,
              exec::Task::ExecutionMode::kParallel);

          std::thread([readerTask, taskId]() {
            try {
              int kNumDrivers = 1;
              std::cout << "Starting task " << taskId << " asynchronously..."
                        << std::endl;
              readerTask->start(kNumDrivers);

              // Optionally wait for completion and print stats
              readerTask->taskCompletionFuture().wait();
              std::cout << "Task " << taskId << " completed." << std::endl;
            } catch (const std::exception& e) {
              std::cerr << "Task " << taskId << " failed: " << e.what()
                        << std::endl;
            }
          }).detach(); // detach makes it run independently

          std::cout << "createTask with taskId: " << taskId << std::endl;
          hashmap[taskId] = readerTask;

          json reply = {{"taskId", taskId}};
          nextTaskId += 1;
          res.set_content(reply.dump(), "application/json");

        } catch (std::exception& e) {
          res.status = 400;
          json err = {{"error", e.what()}};
          res.set_content(err.dump(), "application/json");
        }
      });

  server.Put(
      "/addSplit",
      [&hashmap, &kHiveConnectorId](
          const httplib::Request& req, httplib::Response& res) {
        try {
          auto body = json::parse(req.body);

          if (!body.contains("taskId") || !body.contains("Split")) {
            res.status = 400;
            res.set_content(R"({"error":"Bad Request"})", "application/json");
            return;
          }
          std::string taskId = body["taskId"];
          std::string split_filename = body["Split"];
          std::cout << "Received /addSpit, so we add " << split_filename
                    << " to taskId: " << taskId << std::endl;

          auto it = hashmap.find(taskId);

          // Checking if key present or not
          if (it == hashmap.end()) {
            res.status = 400;
            res.set_content(
                R"({"error":"Task does not exist"})", "application/json");
            return;
          }

          auto filePath =
              "file:" + std::filesystem::path(split_filename).string();
          std::cout << filePath << std::endl;
          std::shared_ptr<facebook::velox::connector::ConnectorSplit>
              connectorSplit;

          if (FLAGS_use_hive) {
            connectorSplit =
                std::make_shared<connector::hive::HiveConnectorSplit>(
                    kHiveConnectorId,
                    filePath,
                    dwio::common::FileFormat::PARQUET);
          } else {
            connectorSplit =
                std::make_shared<facebook::velox::cudf_velox::connector::
                                     parquet::ParquetConnectorSplit>(
                    "test-parquet",
                    std::filesystem::path(split_filename).string(),
                    0);
          }

          hashmap[taskId]->addSplit(
              "0", exec::Split{std::move(connectorSplit)});

          std::cout << "ADDED " << split_filename << " to taskId: " << taskId
                    << std::endl;

        } catch (std::exception& e) {
          res.status = 400;
          json err = {{"error", e.what()}};
          res.set_content(err.dump(), "application/json");
        }
      });

  server.Put(
      "/noMoreSplits",
      [&hashmap](const httplib::Request& req, httplib::Response& res) {
        try {
          auto body = json::parse(req.body);

          if (!body.contains("taskId")) {
            res.status = 400;
            res.set_content(R"({"error":"Bad Request"})", "application/json");
            return;
          }
          std::string taskId = body["taskId"];
          std::cout << "Received /noMoreSplits, for taskId: " << taskId
                    << std::endl;

          auto it = hashmap.find(taskId);

          // Checking if key present or not
          if (it == hashmap.end()) {
            res.status = 400;
            res.set_content(
                R"({"error":"Task does not exist"})", "application/json");
            return;
          }

          hashmap[taskId]->noMoreSplits("0");
          hashmap.erase(taskId);

        } catch (std::exception& e) {
          res.status = 400;
          json err = {{"error", e.what()}};
          res.set_content(err.dump(), "application/json");
        }
      });

  std::cout << "REST API listening on port" << FLAGS_rest_port;
  // server.listen("0.0.0.0", 20080);
  std::thread httpThread([&]() { server.listen("0.0.0.0", FLAGS_rest_port); });

  // Wait until Ctrl+C
  while (!stopRequested.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  std::cout << "Stopping HTTP server..." << std::endl;
  server.stop();
  httpThread.join();
  communicator->stop();
  // Clean up
  serverThread.join();
  facebook::velox::cudf_velox::unregisterCudf();
  pool.reset();
  executor.reset();
}
