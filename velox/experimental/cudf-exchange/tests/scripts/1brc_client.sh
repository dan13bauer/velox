UCX_TLS=tcp,cuda_copy,cuda_ipc UCX_MAX_RNDV_RAILS=1 UCX_LOG_LEVEL=info UCX_PROTO_INFO=n UCX_RNDV_PIPELINE_ERROR_HANDLING=y  CUDA_VISIBLE_DEVICES=7 /workspace/velox/_build/release/velox/experimental/cudf-exchange/tests/1brc_client -v=3 --logtostdout -velox_cudf_memory_resource=async  -nodes="http://127.0.0.1:50000"

