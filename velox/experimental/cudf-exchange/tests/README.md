# How to run the tests:

Run all tests:
CUDA_VISIBLE_DEVICES=7 UCX_TCP_CM_REUSEADDR=y ./_build/release/velox/experimental/cudf-exchange/tests/cudf_exchange_test -v=3 -logtostdout  

Run only a selected test:
CUDA_VISIBLE_DEVICES=7 UCX_TCP_CM_REUSEADDR=y ./_build/release/velox/experimental/cudf-exchange/tests/cudf_exchange_test -v=3 -logtostdout  --gtest_filter=CudfExchangeTest.basicTest
