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

**Scope of this first step.** Exactly as **PR #16980 landed the output-side
registry infrastructure *without* wiring in the cuDF partitioned-output
adapter**, this PoC lands the receive-side registry infrastructure *without*
wiring in any real (non-in-memory) transport. The in-memory transport is the
only registered entry. Wiring the real **UCX** transport (client + operator)
into the registry is the documented next step — see
[Outlook](#outlook-integrating-the-ucx-exchange-components).

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

## Outlook: integrating the ucx-exchange components

The `velox/experimental/ucx-exchange/` module (a UCX/RDMA transport with GPU/cuDF
integration) is the intended first *real* transport. Wiring it in is a separate
step with a build-system prerequisite. The pieces, roughly in order:

### 0. Prerequisite — put the module in the build
`velox/experimental/ucx-exchange/` is **not currently referenced by any
`add_subdirectory`**, so `velox_ucx_exchange` and `ucx_exchange_test` are never
built. First add `add_subdirectory(experimental/ucx-exchange)` under the
`VELOX_ENABLE_CUDF` branch of `velox/CMakeLists.txt`, and confirm the module still
compiles (it links `ucxx::ucxx`, `find_package(ucx REQUIRED)`, cuDF and CUDA —
availability of these in the build image needs verifying, since the module has not
been compiled in this configuration).

### 1. `UcxExchangeClient` implements `exec::ExchangeClient`
`UcxExchangeClient` already has all the needed methods; adapting it is mostly
signature alignment:
- add `: public exec::ExchangeClient` (keep `enable_shared_from_this` on the
  concrete class);
- `addRemoteTaskId(std::string_view)` → `addRemoteTaskId(const std::string&)`
  (the body works unchanged);
- drop `const` on `stats()` to match the interface, and add `override` to all
  five control-plane methods (`toJson()` already matches);
- keep the data plane (`next()` / `queue()`) concrete. The current `stats()` is a
  stub — left as-is for the PoC (leave a `// TODO`).

### 2. Register the UCX transport entry
Add a `registerUcxExchangeTransport()` free function (in a small registration TU
in the module) that inserts a `kUcx` entry into
`ExchangeTransportRegistry::global()`:
- **`makeClient`** builds a `UcxExchangeClient` from the `ExchangeClientContext`
  (capturing any process-wide UCX infrastructure it needs);
- **`makeOperator`** downcasts the abstract client to `UcxExchangeClient` and
  builds a `UcxExchange`. Note `UcxExchange`'s ctor takes a generic
  `core::PlanNode` and a `shared_ptr<UcxExchangeClient>`, so the registry's
  `ExchangeNode` (an is-a `PlanNode`) passes through directly after the downcast.

Call this registration where the UCX/cuDF module initializes.

### 3. Route `ucx_exchange_test` through the registry
The UCX test harness currently constructs `UcxExchangeClient` / `UcxExchange`
directly (in `SinkDriverMock` and a couple of `UcxExchangeTest` cases). Change
those sites to resolve via the registry (`tryGet` → `makeClient` / `makeOperator`)
so the test exercises the real resolution path. Register the transport in the
fixture `SetUp` and `unregisterAll()` in teardown. Build and run on GPU
(`--gpus all`, `cudf.enabled=true`; the target is labeled `cuda_driver`).

### Known follow-on: MergeExchange over a non-in-memory transport
The merge path currently **downcasts the resolved client to
`InMemoryExchangeClient`** because `MergeExchangeSource::next()` needs the concrete
data plane (the abstract `ExchangeClient` has no `next()`). A UCX-backed
`MergeExchange` would therefore need either a shared data-plane abstraction on the
interface or a merge-source adapter for the UCX client. Plain (non-merge)
`Exchange` over UCX has no such constraint — its operator is fully transport-owned
via `makeOperator`.
