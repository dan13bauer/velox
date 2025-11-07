#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Define possible values
block_sizes=(512 1024 2048 4096 8192)
#error_flags=(true false)
#blocking_flags=(true false)

SERVER=${HOME}/velox/_build/release/velox/experimental/cudf-exchange/tests/1brc_server
CLIENT=${HOME}/velox/_build/release/velox/experimental/cudf-exchange/tests/1brc_client

# Loop through all combinations
for block_size in "${block_sizes[@]}"; do
  #  for error_flag in "${error_flags[@]}"; do
  #    for blocking_flag in "${blocking_flags[@]}"; do
  echo "Starting 1brc server"
  CUDA_VISIBLE_DEVICES=6 \
    UCX_RNDV_PIPELINE_ERROR_HANDLING=y \
    UCX_TLS=tcp,cuda_copy,cuda_ipc \
    UCX_MAX_RNDV_RAILS=1 \
    ${SERVER} \
    -inputfiles=/gpfs/zc2/data/tpch/tpch-sf1-parquet/one_brc_parquet/measurements.parquet \
    -v=3 --logtostdout -velox_cudf_memory_resource=pool \
    -cudfChunkSizeMB=${block_size} \
    -maxOutputBufferSizeMB=32768 >&server_1_bufSize_${block_size}.log &
  pid1=$!

  sleep 20
  echo "Starting 1brc client"
  CUDA_VISIBLE_DEVICES=7 \
    UCX_RNDV_PIPELINE_ERROR_HANDLING=y UCX_TLS=tcp,cuda_copy,cuda_ipc \
    UCX_MAX_RNDV_RAILS=1 \
    $CLIENT \
    -v=3 --logtostdout -velox_cudf_memory_resource=pool \
    -nodes="http://sally:24356" >&client_bufSize_${block_size}.log &
  pid2=$!

  # Wait for both processes to finish before continuing
  wait $pid1
  wait $pid2

  #    done
  #  done
done
