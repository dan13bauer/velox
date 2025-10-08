# How to run the tests:

Run all tests:
./_build/release/velox/experimental/cudf-exchange/tests/cudf_exchange_test -v=3 -logtostdout  

Run only a selected test:
./_build/release/velox/experimental/cudf-exchange/tests/cudf_exchange_test -v=3 -logtostdout  --gtest_filter=CudfExchangeTest.basicTest
