# Exchange Transport Registry — Receive-Side PoC

Pluggable, per-node transport selection for the **exchange (receive) side** of Velox
query execution, with a real UCX/RDMA transport wired through it on both ends of a
shuffle. The receive side mirrors the already-merged output-side transport registry
(upstream **PR #16980**).

Before this work, exchange was hard-wired to a single in-memory `ExchangeClient`,
`LocalPlanner` built the `Exchange` / `MergeExchange` operators directly, and an
alternative transport needed the ad-hoc `Operator::toOperator(..., exchangeClient)`
hook. Now an exchange plan node carries a **`transportKind`**, and the engine
resolves *both* the client and the operator for that node from a query-scoped
**`ExchangeTransportRegistry`**. A transport contributes a `(client factory,
operator factory)` pair keyed by a transport-kind string. The in-memory transport is
a seeded default; a plan naming a transport with no registered entry fails the query
rather than falling back.

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

| Concern | Entry point |
|---|---|
| Abstract client interface | `velox/exec/ExchangeClient.h` |
| Context and factory typedefs | `velox/exec/ExchangeFactory.h` |
| The registry | `velox/exec/ExchangeTransportRegistry.{h,cpp}` |
| Built-in in-memory transport | `velox/exec/InMemoryExchangeClient.{h,cpp}` |
| Client resolution (per exchange node, Task-owned) | `Task::createExchangeClientLocked` |
| Operator resolution | `LocalPlanner::createDriver` |
| Merge path | `Merge.cpp` — `MergeExchange::addMergeSources` |
| UCX client | `velox/experimental/ucx-exchange/UcxExchangeClient.{h,cpp}` |
| UCX registration, both registries | `velox/experimental/ucx-exchange/UcxExchangeRegistration.cpp` |
| UCX output buffering | `velox/experimental/ucx-exchange/UcxOutputQueueManager.{h,cpp}` |
| Merge over UCX (operator substitution) | `velox/experimental/cudf/exec/OperatorAdapters.cpp` — `MergeExchangeAdapter` |
| Configuration | `velox/experimental/cudf/CudfConfig.h` — `cudf.exchange` gates registration |

Tests: `velox_exchange_transport_registry_test` and `ExchangeTransportTest` on the
CPU path; `ucx_exchange_test` (GPU) for the UCX transport, including task-level
cases that run a two-fragment plan over UCX and the same plan over the default
transport asserted against identical rows.

---

## Trying it

UCX is a hard requirement of a cuDF build, so configuring with
`VELOX_ENABLE_CUDF=ON` requires a system UCX and fails at the dependency probe with
a named remedy if it is absent. Registration is gated at runtime: a worker gets the
UCX transports only when `cudf.exchange` is true.

The build and test recipe for the GPU image is in the design document's
Verification section rather than duplicated here.
