# Exchange transport integration — proof-of-concept branch

Branch: `poc-exchange-transport-integration`, on `oss-velox/main` (which contains
PR #16980).

Every layer sits on one branch so the whole path can be built and tested together.
Splitting it into upstream pull requests is planned separately in
[`upstream-pr-plan.md`](upstream-pr-plan.md).

## Scope

**What already existed.** Velox selects the **output** transport per plan node through
`OutputTransportRegistry` (PR #16980): `core::PartitionedOutputNode` carries a
`transportKind`, an abstract `OutputBufferManager` sits behind it, and the in-memory
transport is a seeded default entry. The transport-id constants live in
`core::TransportKind` in `velox/core/PlanNode.h` — `kInMemory{"in-memory"}` as the
default, `kHttp` as a deprecated alias of it, and `kUcx{"UCX"}`. The receive side had
no equivalent: exchange was hard-wired to one concrete client, and `LocalPlanner`
built the `Exchange` and `MergeExchange` operators directly. The
`velox/experimental/ucx-exchange` module was present but unwired — its client and
operator were constructed only in tests, `UcxExchangeClient` was a standalone class
rather than an implementation of an engine interface, and no UCX entry existed in the
output registry.

**What this branch adds.**

- The receive-side half of the mechanism: a `transportKind` on the exchange plan
  nodes and an `ExchangeTransportRegistry` that resolves both the client and the
  operator for a node, mirroring the output side.
- The UCX send side, registered into `OutputTransportRegistry` so a `kUcx`
  `PartitionedOutputNode` gets UCX output buffering.
- The UCX receive side, implementing the new abstract client interface and registered
  into `ExchangeTransportRegistry`.
- A merge exchange over UCX, served by a device sort rather than a host merge.
- The configuration and build work that makes the module selectable and compiled.

**What UCX does not implement.** The UCX client satisfies the control-plane contract
and no more. It is not owned or accounted the way the in-memory client is: no
memory-pool accounting, no byte-budget backpressure (its flow control is count-based),
no request scheduler, and `stats()` returns an empty map. These are consequences of a
push transport plus the limits of a proof of concept, not of the registry design.

## Send- and receive-side design symmetry

The receive side is a deliberate, near-line-for-line mirror. The one structural
difference is the entry shape, for the reason spelled out after the table.

| Concern | Send side (#16980) | Receive side (this branch) |
|---|---|---|
| Plan-node field | `transportKind` on `core::PartitionedOutputNode` | `transportKind` on `core::ExchangeNode` **and** `core::MergeExchangeNode` |
| Transport-kind constants | `core::TransportKind` (`kInMemory` default, `kUcx`) | same constants, reused |
| Registry class | `OutputTransportRegistry` (`velox/exec/OutputTransportRegistry.{h,cpp}`) | `ExchangeTransportRegistry` (`velox/exec/ExchangeTransportRegistry.{h,cpp}`) |
| Backing store | `ScopedRegistry<std::string, OutputTransportEntry>` (query-scoped over global) | `ScopedRegistry<std::string, ExchangeTransportEntry>` (query-scoped over global) |
| Registry key | `kRegistryKey = "outputTransports"` | `kRegistryKey = "exchangeTransports"` |
| Lookup / lifecycle API | `global()` / `create()` / `tryGet()` ×2 / `getAll()` ×2 / `unregisterAll()` ×2 | identical set |
| Factory typedefs header | `PartitionedOutputFactory.h` | `ExchangeFactory.h` |
| **Entry shape** | `OutputTransportEntry { manager; makeOutputOperator }` + a `make()` weak-capture helper binding one long-lived manager to its operator | `ExchangeTransportEntry { makeClient; makeOperator }` — plain aggregate, **no `make()` helper**, since the client is per-node and Task-owned |
| Interface / concrete | abstract `OutputBufferManager` / `DefaultOutputBufferManager` | abstract `ExchangeClient` (control plane: `addRemoteTaskId` / `noMoreRemoteTasks` / `close` / `stats` / `toJson`) / `InMemoryExchangeClient` |
| Operator ctor | `PartitionedOutput(id, ctx, node, eagerFlush, manager)` | `Exchange(id, ctx, node, client)` |
| Built-in default (seeded, restored by `unregisterAll()`) | in-memory output buffer manager | `InMemoryExchangeClient` |
| Fail-fast on unknown transport | yes | yes — `VELOX_USER_CHECK_NOT_NULL`, "No exchange transport registered for transport: …" |
| UCX impl | `UcxOutputQueueManager` + `UcxPartitionedOutput` | `UcxExchangeClient` + `UcxExchange` |
| Standalone registry unit test | `OutputTransportRegistryTest.cpp` | `ExchangeTransportRegistryTest.cpp` |
| End-to-end resolution test | `OutputTransportTest.cpp` | `ExchangeTransportTest.cpp` |

The asymmetry in the entry shape follows from ownership. There is no long-lived,
process-wide manager instance on the receive side. `Task::createExchangeClientLocked`
builds one client per exchange plan node and stores that single `shared_ptr` in both
`exchangeClients_` (by pipeline) and `exchangeClientByPlanNode_` (by plan-node id);
every `Exchange` operator instance for that node, across all drivers, receives the
same client. Because the client is per-task, per-node state rather than a process-wide
singleton, the query-scoped registry cannot hold an instance — it holds a client
factory alongside the operator factory. In the output-side mapping the exchange client
is the analog of `OutputBuffer`, not of the `OutputBufferManager` service, and the
registry is never keyed by task.

## Architecture

### Receive-side foundation

`core::ExchangeNode` and `core::MergeExchangeNode` carry a `transportKind` next to
their existing `serdeKind`. The `Builder` requires it explicitly; the pre-existing
constructors remain behind `VELOX_ENABLE_BACKWARD_COMPATIBILITY`; deserializing a plan
that predates the field defaults it to `kInMemory`; `addDetails` prints it.

`velox/exec/ExchangeClient.h` is an abstract control-plane interface —
`addRemoteTaskId`, `noMoreRemoteTasks`, `close`, `stats`, `toJson`. The former
concrete class is `InMemoryExchangeClient` and implements it. Each transport's data
plane stays concrete and non-virtual on its own client, so the operator factory
downcasts once and the registry stays transport-neutral.

`ExchangeTransportRegistry` holds a plain-aggregate entry of two factories, keyed by
transport id, with per-query override over a global scope. The in-memory entry is
seeded and restored by `unregisterAll()`. `Task::createExchangeClientLocked` resolves
the entry by the node's `transportKind` and builds the client from it;
`LocalPlanner::createDriver` builds the operator from the same entry. A plan naming an
unregistered transport fails the query.

The pre-registry extensibility path is gone: the client-taking
`Operator::PlanNodeTranslator::toOperator` overload, the matching `fromPlanNode`
overload, and `core::PlanNode::requiresExchangeClient()`. In its place
`DriverFactory::needsExchangeClient()` casts the leaf node to `core::ExchangeNode` and
returns it, so `Task` reads the transport id without a second lookup.

That removal is safe within Velox and breaks Prestissimo, which is the only user of
the surface. Its `ShuffleReadNode` derives from `core::PlanNode`, overrides
`requiresExchangeClient()` to return true, and overrides the client-taking
`toOperator`. Under the node-type cast it receives no client and gets a null pointer
at runtime with nothing failing to compile. The branch does not carry a compatibility
path for it; [`upstream-pr-plan.md`](upstream-pr-plan.md) does, and gates the removal
on a Prestissimo migration.

### UCX on the send side

`UcxOutputQueueManager` implements `exec::OutputBufferManager`, so it can be a
registry entry rather than a special case. Its data plane stays outside that interface
because its payloads are GPU-resident `cudf::packed_columns`, driven directly by
`UcxPartitionedOutput` and the UCX server. The registry entry pairs the manager
singleton with a factory for `UcxPartitionedOutput`, so a `kUcx`
`PartitionedOutputNode` makes `Task` install that manager as the task's output buffer
manager.

### UCX on the receive side

`UcxExchangeClient` derives from `exec::ExchangeClient` and overrides the five
control-plane methods; its `next()` and `queue()` data plane stays concrete.
`registerUcxExchange()` seeds both registries under `kUcx` in one call, each insert
passing `overwrite=true` so the call is idempotent. `unregisterUcxExchange()` removes
both entries — the output entry holds a strong reference to the manager singleton, so
leaving it behind would outlive the registration that created it.

Registration is gated on configuration rather than compilation: `registerCudf()` calls
`registerUcxExchange()` when `CudfConfig::exchange` is set, and `unregisterCudf()`
calls the counterpart unconditionally. A worker configured with `cudf.exchange=true`
gets both transports without a separate call. UCX is a hard requirement of a cuDF
build, since the GPU exchange operators link the module unconditionally; configuring
`VELOX_ENABLE_CUDF=ON` without a system UCX fails at the dependency probe with a
named remedy.

### Merge exchange over UCX

`MergeExchange` reads its node's `transportKind` and resolves a registry entry, then
downcasts the client to `InMemoryExchangeClient`. Merge is therefore in-memory-only
inside `velox/exec/`, and nothing there knows about UCX. The reason is the data plane:
`MergeExchangeSource` needs the client's `next()`, which is deliberately absent from
the abstract interface, so a generically resolved client would have to be cast
straight back to its concrete type.

Merge over UCX is handled one layer up instead. The cuDF operator adapter
`MergeExchangeAdapter` replaces the `exec::MergeExchange` of a `kUcx`
`MergeExchangeNode` at driver-build time with `{UcxExchange, CudfOrderBy}`: the whole
input arrives over UCX on one driver and is sorted once on the device. That is
globally ordered output by a different algorithm, and it is the only option available.
The host k-way merger caches raw pointers into its key columns, while the UCX receive
path yields `CudfVector`s whose columns live on the device with no host children to
point at. The design is in
[`../superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md`](../superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md).

## Design invariants

- Reuse `core::TransportKind` string constants; no parallel enum.
- The built-in in-memory transport is a real seeded registry entry, so every lookup is
  a plain read. An unregistered named transport fails fast.
- `transportKind` sits next to `serdeKind` on the node with no silent default. The
  `Builder` and factories require it explicitly. Only deserialization of a plan
  missing the field defaults to `kInMemory`.
- The registry entry is a plain aggregate. A single `tryGet(queryCtx, id)` returns it,
  with per-query override over global fallback.
- Operators receive their concrete client through the constructor, so the
  operator-to-client pairing lives in the type system rather than a runtime cast.
- The abstract client interface depends on no concrete implementation types: `stats()`
  returns a neutral `F14FastMap<string, RuntimeMetric>` and the header includes no
  concrete queue. Every method specifies its lifecycle ordering and thread-safety, and
  completion is defined by `noMoreRemoteTasks` and `close` rather than an
  implementation detail.

## Verification

The gating test is `ucx_exchange_test` (GPU, `cuda_driver` label, TIMEOUT 3000),
resolving UCX through the registries. Beyond the harness-level cases it runs
task-level plans: a two-fragment plan whose `PartitionedOutputNode` and `ExchangeNode`
both declare `kUcx`, resolved through both registries with `registerCudf()` standing
in for a worker's registration; a twin case running the same plan on the default
transport and asserting identical rows, which makes "the in-memory path is unchanged"
executable rather than assumed; and a case asserting a `kUcx` `MergeExchange` is
globally ordered, with two producers carrying interleaved key ranges so neither is
ordered alone.

On the CPU path: `ExchangeTransportRegistryTest` in its own binary
(`velox_exchange_transport_registry_test`, since it mutates global registry state),
`ExchangeTransportTest`, `PlanNodeSerdeTest` including a non-default `kUcx`
round-trip, `PlanNodeToStringTest`, and the existing exchange and task tests the
rename and registry touch.

## Remaining limitations

- **No true device-side k-way merge.** `cudf::merge` would need per-source attribution
  of incoming batches. `PackedTableWithStream` carries only `{packedTable, stream}`
  and the receive queue is a single FIFO across all remote sources, so attribution
  would have to be added to the wire protocol.
- **Nothing inserts GPU/host conversions around the UCX exchange operators.** Neither
  `UcxExchange` nor `UcxPartitionedOutput` is a `CudfOperator`, and no operator adapter
  claims `Exchange` or `PartitionedOutput` — they resolve through the two registries
  instead — so `CompileState` treats both as CPU operators and places no
  `CudfFromVelox` / `CudfToVelox` beside them. The end-to-end cases sidestep this by
  feeding `Values` device-resident batches and converting the consumer's output in the
  test. A substituted `MergeExchange` is unaffected, because `CudfOrderBy` is a
  `CudfOperator` and gets its `CudfToVelox` in the normal way.
- **A `kUcx` merge exchange the cuDF adapter does not claim has no transport.** With
  cuDF exchange disabled or on a CPU fallback the substitution does not happen, and
  the in-memory-only `MergeExchange` cannot serve the plan.

## Related

- [`upstream-pr-plan.md`](upstream-pr-plan.md) — how this branch splits into upstream
  pull requests.
- [`../../POC-README.md`](../../POC-README.md) — orientation and code entry points.
