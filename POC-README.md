# Exchange Transport Integration — PoC

Velox already chooses its **output** transport per plan node through a registry
(upstream **PR #16980**). This branch supplies the missing **receive**-side half of
that mechanism, then wires a complete UCX/RDMA stack through both halves. A plan can
name UCX on the producing side, the consuming side, or both, and get GPU-to-GPU
transfers end to end.

**The receive-side foundation.** Exchange was hard-wired to a single in-memory
`ExchangeClient`, `LocalPlanner` built the `Exchange` / `MergeExchange` operators
directly, and an alternative transport needed the ad-hoc
`Operator::toOperator(..., exchangeClient)` hook. Now an exchange plan node carries a
**`transportKind`**, and the engine resolves *both* the client and the operator for
that node from a query-scoped **`ExchangeTransportRegistry`**, mirroring the
output-side registry. A transport contributes a `(client factory, operator factory)`
pair keyed by a transport-kind string. The in-memory transport is a seeded default; a
plan naming a transport with no registered entry fails the query rather than falling
back to it.

**The UCX stack on top.** The `velox/experimental/ucx-exchange` module is registered
into both registries: its client and exchange operator on the receive side, its output
queue manager and partitioned-output operator on the send side. Registration is gated
on a session configuration key, the module is compiled as part of a cuDF build for the
first time, and a `kUcx` merge exchange is served by a device sort instead of a host
k-way merge.

Branch: `poc-exchange-transport-integration`, on `oss-velox/main` (which contains
#16980), pushed to the `dan13bauer` fork.

---

## Where this is documented

This file is orientation only. The substance lives in two design documents, and is
deliberately not repeated here.

- **[`docs/designs/exchange-transport-registry-poc.md`](docs/designs/exchange-transport-registry-poc.md)**
  — the design and the as-built record. Goal and scope, the full output-side mirror
  table, what each layer turned out to be (including where the plan and the outcome
  diverged), the two design claims that did not survive implementation, the
  verification recipe, and the remaining limitations.
- **[`docs/designs/upstream-pr-plan.md`](docs/designs/upstream-pr-plan.md)** — how
  this branch splits into upstream pull requests. Grouped by what each PR depends on
  and what it can break rather than by directory, and ordered so that every merge
  leaves a tree that both builds and runs. Also records the one PR gated on a
  Prestissimo migration, and which open upstream ucx-exchange PRs this supersedes.

Further reading referenced from those: the merge-over-UCX design in
`docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md`.

---

## Reading the code

Links point at this branch on the `dan13bauer` fork.

| Concern | Entry point |
|---|---|
| Abstract client interface | [`velox/exec/ExchangeClient.h`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/ExchangeClient.h) |
| Context and factory typedefs | [`velox/exec/ExchangeFactory.h`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/ExchangeFactory.h) |
| The receive-side registry | [`velox/exec/ExchangeTransportRegistry.h`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/ExchangeTransportRegistry.h)<br>[`velox/exec/ExchangeTransportRegistry.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/ExchangeTransportRegistry.cpp) |
| Built-in in-memory transport | [`velox/exec/InMemoryExchangeClient.h`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/InMemoryExchangeClient.h)<br>[`velox/exec/InMemoryExchangeClient.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/InMemoryExchangeClient.cpp) |
| Client resolution — `Task::createExchangeClientLocked` | [`velox/exec/Task.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/Task.cpp) |
| Operator resolution — `LocalPlanner::createDriver` | [`velox/exec/LocalPlanner.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/LocalPlanner.cpp) |
| Merge path — `MergeExchange::addMergeSources` | [`velox/exec/Merge.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/Merge.cpp) |
| UCX client | [`velox/experimental/ucx-exchange/UcxExchangeClient.h`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/ucx-exchange/UcxExchangeClient.h)<br>[`velox/experimental/ucx-exchange/UcxExchangeClient.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/ucx-exchange/UcxExchangeClient.cpp) |
| UCX registration, both registries | [`velox/experimental/ucx-exchange/UcxExchangeRegistration.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/ucx-exchange/UcxExchangeRegistration.cpp) |
| UCX output buffering (send side) | [`velox/experimental/ucx-exchange/UcxOutputQueueManager.h`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/ucx-exchange/UcxOutputQueueManager.h)<br>[`velox/experimental/ucx-exchange/UcxOutputQueueManager.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/ucx-exchange/UcxOutputQueueManager.cpp) |
| UCX partitioned output operator | [`velox/experimental/ucx-exchange/UcxPartitionedOutput.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/ucx-exchange/UcxPartitionedOutput.cpp) |
| Merge over UCX — `MergeExchangeAdapter` | [`velox/experimental/cudf/exec/OperatorAdapters.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/cudf/exec/OperatorAdapters.cpp) |
| Configuration — `cudf.exchange` gates registration | [`velox/experimental/cudf/CudfConfig.h`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/cudf/CudfConfig.h) |
| Registry unit test | [`velox/exec/tests/ExchangeTransportRegistryTest.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/tests/ExchangeTransportRegistryTest.cpp) |
| Resolution test | [`velox/exec/tests/ExchangeTransportTest.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/exec/tests/ExchangeTransportTest.cpp) |
| UCX tests (GPU), incl. task-level cases | [`velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`](https://github.com/dan13bauer/velox/blob/poc-exchange-transport-integration/velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp) |

---

## Trying it

UCX is a hard requirement of a cuDF build, so configuring with
`VELOX_ENABLE_CUDF=ON` requires a system UCX and fails at the dependency probe with
a named remedy if it is absent. Registration is gated at runtime: a worker gets the
UCX transports only when `cudf.exchange` is true.

The build and test recipe for the GPU image is in the design document's
Verification section rather than duplicated here.
