# Exchange Transport Registry — Receive-Side PoC

## Overview

This PoC brings **pluggable, per-node transport selection to the exchange
(receive) side** of Velox query execution, mirroring the already-merged
output-side transport registry (upstream **PR #16980**, "Select output transport
per PartitionedOutput node via a pluggable registry").

Before this change, exchange was hard-wired to a single, HTTP-style in-memory
`ExchangeClient`, and `LocalPlanner` built the `Exchange` / `MergeExchange`
operators directly. Supporting an alternative transport (e.g. UCX/RDMA) required
an ad-hoc `Operator::toOperator(..., exchangeClient)` extensibility hook.

After this change, an exchange plan node carries a **`transportKind`**, and the
engine resolves *both* the exchange client and the exchange operator for that
node from a query-scoped **`ExchangeTransportRegistry`**. A transport contributes
a `(client factory, operator factory)` pair keyed by a transport-kind string;
the built-in in-memory transport is a seeded default, and an unregistered
transport fails fast.

**Scope of the first step.** Exactly as **PR #16980 landed the output-side
registry infrastructure *without* wiring in the cuDF partitioned-output
adapter**, this PoC first landed the receive-side registry infrastructure
*without* wiring in any real (non-in-memory) transport, leaving the in-memory
transport as the only registered entry. The real **UCX** transport (client +
operator, plus its output buffer manager) has since been wired in on top — see
[Integrated](#integrated-the-ucx-exchange-components).

The work is on branch `poc-exchange-transport-integration` (pushed to the
`dan13bauer` fork), built on merged `main` (which already contains #16980).

---

## Symmetry with PR #16980

The receive side is a deliberate, near-line-for-line mirror of the output side.
The one structural difference is highlighted in the last rows: an exchange client
is **created per node and owned by `Task`**, whereas an output buffer manager is a
longer-lived shared instance — so the exchange entry stores *two factories*
(client + operator) rather than *a manager instance + one operator factory*.

| Concern | Output side — PR #16980 | Receive side — this PoC |
|---|---|---|
| Registry class | `OutputTransportRegistry` (`velox/exec/OutputTransportRegistry.{h,cpp}`) | `ExchangeTransportRegistry` (`velox/exec/ExchangeTransportRegistry.{h,cpp}`) |
| Backing store | `ScopedRegistry<std::string, OutputTransportEntry>` (query-scoped over global) | `ScopedRegistry<std::string, ExchangeTransportEntry>` (query-scoped over global) |
| Registry key | `kRegistryKey = "outputTransports"` | `kRegistryKey = "exchangeTransports"` |
| Lookup / lifecycle API | `global()` / `create()` / `tryGet()` ×2 / `getAll()` ×2 / `unregisterAll()` ×2 | identical set |
| Factory typedefs header | `PartitionedOutputFactory.h` | `ExchangeFactory.h` |
| Node field carrying the choice | `transportKind` on `core::PartitionedOutputNode` | `transportKind` on `core::ExchangeNode` **and** `core::MergeExchangeNode` |
| Transport-kind constants | `core::TransportKind` (`kInMemory` default, `kUcx`) | same constants, reused |
| Built-in default (seeded, restored by `unregisterAll()`) | in-memory output buffer manager | `InMemoryExchangeClient` |
| Fail-fast on unknown transport | yes | yes (`VELOX_USER_CHECK_NOT_NULL`, "No exchange transport registered for transport: …") |
| Standalone registry unit test | `OutputTransportRegistryTest.cpp` | `ExchangeTransportRegistryTest.cpp` |
| End-to-end resolution test | `OutputTransportTest.cpp` | `ExchangeTransportTest.cpp` |
| **Abstract interface** | `OutputBufferManager` (buffer/flush surface) | `ExchangeClient` (control plane: `addRemoteTaskId`/`noMoreRemoteTasks`/`close`/`stats`/`toJson`) |
| **Entry shape** | `OutputTransportEntry { manager; makeOutputOperator }` + `make()` weak-capture helper (binds one long-lived manager to its operator) | `ExchangeTransportEntry { makeClient; makeOperator }` — plain aggregate, **no `make()` helper** (client is per-node, `Task`-owned, so nothing long-lived to bind weakly) |
| **What is NOT wired (this step)** | cuDF partitioned-output adapter | UCX exchange (client + operator) |

---

## What changed (detailed)

The branch is five commits on base `49e8aac49` (design/plan doc omitted below).

### 1. Rename `ExchangeClient` → `InMemoryExchangeClient` — `985f4f7ad`
Applies the net diff of upstream PR #18110. The concrete transport class
`ExchangeClient` (and `ExchangeClientTest`) becomes `InMemoryExchangeClient`
(`InMemoryExchangeClientTest`), freeing the `ExchangeClient` name for the new
abstract interface. Pure rename — no behavior change.

### 2. `transportKind` on the exchange nodes — `f8ffad8dc`
Adds a `transportKind` string to `core::ExchangeNode` and
`core::MergeExchangeNode`, reusing `core::TransportKind` (`kInMemory{"in-memory"}`
default, `kUcx{"UCX"}`). Threaded through:
- `PlanBuilder::exchange()` / `mergeExchange()` (now take an optional
  `transportKind`, defaulting to in-memory);
- serde — **backward compatible** via `getDefault("transportKind", kInMemory)`,
  so old serialized plans still deserialize;
- `TraceUtil`.

Serde round-trip tests cover a non-default (`kUcx`) transport for both node types.

### 3. Registry + abstract client + plain-exchange resolution — `5cdc0a25d`
- **`velox/exec/ExchangeClient.h`** — new *abstract* control-plane interface with
  five pure-virtuals: `addRemoteTaskId`, `noMoreRemoteTasks`, `close`, `stats`,
  `toJson`. `InMemoryExchangeClient` now implements it; the transport-specific
  data plane (`next()` / `queue()` / `pool()`) stays concrete and non-virtual.
- **`velox/exec/ExchangeFactory.h`** — typedefs: `ExchangeClientContext` (the
  `Task`-supplied build context), `ExchangeClientFactory`
  (`context → shared_ptr<ExchangeClient>`), and `ExchangeFactory`
  (`operatorId, ctx, ExchangeNode, client → unique_ptr<Operator>`).
- **`velox/exec/ExchangeTransportRegistry.{h,cpp}`** — faithful mirror of
  `OutputTransportRegistry`. `ExchangeTransportEntry` is a plain aggregate
  `{ makeClient, makeOperator }`. The in-memory transport is a real seeded entry
  (`InMemoryExchangeClient::makeDefaultTransportEntry()`), restored by
  `unregisterAll()`.
- **`Task`** resolves the entry by the node's `transportKind`
  (`createExchangeClientLocked`), builds the `Task`-owned client via `makeClient`,
  and stores it as the *abstract* `ExchangeClient` — calling only control-plane
  methods on it. **`LocalPlanner`** builds the operator via the entry's
  `makeOperator` (which downcasts the client to its concrete type). An
  unregistered transport fails fast.
- **Removed** the old extensibility path:
  `Operator::toOperator(..., exchangeClient)` / `fromPlanNode(..., exchangeClient)`
  and `PlanNode::requiresExchangeClient()`.
- **Migrated** (not deleted) the `customPlanNodeWithExchangeClient` test to the
  registry, preserving coverage of a custom exchange operator.
- New tests: `ExchangeTransportRegistryTest` (global vs query-scoped resolution,
  isolation mode, `unregisterAll` re-seed, seeded default) and
  `ExchangeTransportTest` (operator/client resolution, default, fail-fast).

### 4. MergeExchange resolution — `398bdd31f`
`MergeExchange` now resolves its per-source client from the registry by
`transportKind`, downcasting to `InMemoryExchangeClient` for the merge data plane
(`MergeExchangeSource::next()`). To keep merge's buffering semantics unchanged
through the shared client factory, the two buffer-sizing knobs move into
`ExchangeClientContext` as **caller-set fields**:
- `maxExchangeBufferSize` (`int64_t`) and `minExchangeOutputBatchBytes`
  (`uint64_t`);
- the plain path sets them from `queryConfig`;
- the merge path sets them to its per-source byte budget and `0` ("deliver
  immediately, don't block other sources").

The three remaining in-memory knobs (`requestDataSizesMaxWaitSec`, single-source
optimization, lazy fetching) now come from `queryConfig`; this is a **no-op at
default config** (defaults match the previous hard-coded values) and only takes
effect if a user overrides them. Adds `mergeExchangeUsesTransportKind`.

### 5. Cleanup — `9670e11c2`
Removes a now-dead `InMemoryExchangeClient` forward declaration in `Driver.h`,
refreshes two `Task.h` doc comments that still named the concrete client, and
appends the transport kind to the MergeExchange downcast-failure message.

### Verification
Every commit was built in the cuDF-enabled image (full `velox/exec` suite
compiled) and the relevant tests run on the CPU path:
`velox_exchange_transport_registry_test` (6/6), `ExchangeTransportTest` (5/5),
`PlanNodeSerdeTest` (incl. `kUcx` round-trip), and the full merge-exchange
regression across every serde × compression parameter (`mergeExchange`,
`abortMergeExchange`, `mergeExchangeWithSpill`, `mergeExchangeOverEmptySources`,
migrated `customPlanNodeWithExchangeClient`).

---

## Design notes

- **Control plane vs data plane.** The abstract `ExchangeClient` exposes only what
  `Task` needs to manage upstream producers. Each transport's data plane stays
  concrete and non-virtual on its client, and the operator factory downcasts once.
  This keeps the registry transport-neutral while letting operators use
  transport-specific fetch paths.
- **Why two factories, not a stored manager.** On the output side an
  `OutputTransportEntry` stores a long-lived `OutputBufferManager` instance plus
  an operator factory (bound via a weak-capturing `make()` helper). On the receive
  side the exchange client is created *per exchange node* and owned by `Task`, so
  the entry stores a **client factory** (`makeClient`) instead of a manager
  instance — hence a plain aggregate with no `make()` helper.
- **Merge buffering fidelity.** Sizing the buffer via caller-set context fields
  (rather than reading `queryConfig` inside the factory) lets the merge path keep
  its per-source budget and immediate-delivery semantics unchanged while still
  resolving its client through the same registry factory.

---

## Integrated: the ucx-exchange components

The `velox/experimental/ucx-exchange/` module (a UCX/RDMA transport with GPU/cuDF
integration) was the intended first *real* transport, and it is now wired in.
What each step turned out to be:

### 0. The module is in the build
`add_subdirectory(experimental/ucx-exchange)` sits under the `VELOX_ENABLE_CUDF`
branch of `velox/CMakeLists.txt`, so `velox_ucx_exchange` and `ucx_exchange_test`
build in the cuDF configuration. The module links `ucxx::ucxx`,
`find_package(ucx REQUIRED)`, cuDF and CUDA, all of which the GPU build image
provides.

### 1. `UcxExchangeClient` implements `exec::ExchangeClient`
It derives from `exec::ExchangeClient` and keeps `enable_shared_from_this` on the
concrete class. `addRemoteTaskId` takes a `const std::string&`, `stats()` dropped
its `const`, and the five control-plane methods are `override`. The data plane
(`next()` / `queue()`) stayed concrete, and `stats()` is still the PoC stub.

### 2. The UCX transport entries are registered
`registerUcxExchange()` (`UcxExchangeRegistration.cpp`) seeds **both** registries
under `core::TransportKind::kUcx`, each insert passing `overwrite=true` so the
call is idempotent:
- `ExchangeTransportRegistry` gets `makeClient`, which builds a
  `UcxExchangeClient` from the `ExchangeClientContext`, and `makeOperator`, which
  downcasts the abstract client back to `UcxExchangeClient` and builds a
  `UcxExchange`;
- `OutputTransportRegistry` gets an entry pairing the `UcxOutputQueueManager`
  singleton with a factory for `UcxPartitionedOutput`, so a `kUcx`
  `PartitionedOutputNode` makes `Task` install that manager as its output buffer
  manager.

`registerCudf()` calls `registerUcxExchange()` whenever `CudfConfig::exchange` is
true, so a worker configured with `cudf.exchange=true` gets both transports
without a separate call.

### 3. `ucx_exchange_test` runs through the registries
`SinkDriverMock` and the `UcxExchangeTest` cases resolve their client and operator
through `ExchangeTransportRegistry::tryGet` instead of constructing them; the
fixture registers in `SetUp()` and clears both registries in `TearDown()`. Three
cases go further and run real `Task`s end to end: a two-fragment
`Values -> PartitionedOutput` / `Exchange -> consumer` plan on `kUcx`, the same
plan on the default in-memory transport asserted against the same rows, and a
`kUcx` `MergeExchange` asserted to be globally ordered.

### MergeExchange over UCX: a device sort, not a merge
The follow-on is handled, but not the way this section originally anticipated.
`Merge.cpp` still downcasts the resolved client to `InMemoryExchangeClient` and
nothing in `velox/exec/` learned about UCX. Instead a cuDF operator adapter
(`MergeExchangeAdapter`) replaces the `exec::MergeExchange` of a `kUcx`
`MergeExchangeNode` at driver-build time with `{UcxExchange, CudfOrderBy}`: the
whole input arrives over UCX on one driver and is sorted once on the device. That
is globally ordered output by a different algorithm, and it is the only option
available — the host k-way merger caches raw pointers to its key columns, while
the UCX receive path yields `CudfVector`s whose columns live on the device with
no host children to point at. See
`docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md`.

### Remaining limitations
- **The UCX link into cuDF is ungated.** `velox_cudf_exec` links
  `velox_ucx_exchange` unconditionally, so configuring cuDF without UCX fails.
- **No true device-side k-way merge.** `cudf::merge` would need per-source
  attribution of incoming batches. `PackedTableWithStream` carries only
  `{packedTable, stream}` and the receive queue is a single FIFO across all
  remote sources, so attribution would have to be added to the wire protocol.
- **Nothing inserts GPU/host conversions around the UCX exchange operators.**
  Neither `UcxExchange` nor `UcxPartitionedOutput` is a `CudfOperator`, and no
  operator adapter claims `Exchange` or `PartitionedOutput` — they resolve through
  the two registries instead — so `CompileState` treats both as CPU operators and
  places no `CudfFromVelox` / `CudfToVelox` beside them. A GPU operator feeding a
  `kUcx` `PartitionedOutput` therefore gets a `CudfToVelox` spliced in before it
  and then trips that operator's `CudfVector` check, and a `kUcx` `Exchange`
  feeding a GPU operator gets a `CudfFromVelox` that cannot read a
  device-resident `CudfVector`. The end-to-end cases sidestep this by feeding
  `Values` device-resident batches and converting the consumer's output in the
  test. A substituted `MergeExchange` is unaffected, because `CudfOrderBy` is a
  `CudfOperator` and gets its `CudfToVelox` in the normal way.

---

## Landing this upstream

The branch is not meant to land as one change. `docs/designs/upstream-pr-plan.md`
breaks it into a sequence of upstream pull requests, grouped by what each one
depends on and what it can break rather than by directory, and ordered so that
every merge leaves a tree that both builds and runs. It also records the one PR
that cannot land without a Prestissimo migration ahead of it, and which of the
open upstream ucx-exchange PRs this work supersedes.
