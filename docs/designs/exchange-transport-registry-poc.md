# Exchange transport registry — receive-side proof-of-concept integration branch

Date: 2026-07-23 (implementation recorded 2026-08-14)
Status: **Implemented.** The receive-side registry and the UCX transport are wired
end-to-end on the branch. The upstream split is planned separately in
`docs/designs/upstream-pr-plan.md`.
Branch: `poc-exchange-transport-integration`, now 38 commits off `oss-velox/main`
(`844c77cc9`), which contains PR #16980.

## Goal

Build a proof-of-concept integration branch demonstrating the **receive-side**
exchange-transport registry — the symmetric counterpart to upstream PR #16980
(output side) — with a **real UCX transport wired through the registry** and the
GPU `ucx_exchange_test` passing end-to-end.

Unlike an upstream-shaped stack of one-concern-per-PR changes, this PoC
**co-locates every layer on one branch off merged `main`** so the whole receive
path can be built and tested together, including the UCX transport that #16980's
registry enables. Splitting it back out is the subject of
`docs/designs/upstream-pr-plan.md`, which supersedes the "clean 4-PR stack"
this document originally anticipated: the real split is twelve PRs plus the
already-open rename, because the layers below turned out to differ in what they
depend on and what they can break.

## Scope

- **In scope:** the four mirror layers (rename, node field, registry,
  mergeexchange) implemented to production fidelity, plus wiring the UCX
  transport into the new registry. Full pre-application of #16980's open review
  feedback to the abstract `ExchangeClient` interface. Scope grew during
  implementation to include the UCX **output** side, session configuration, and
  getting the module into the build at all — none of which is optional if the
  transport is to be selectable and testable.
- **Verification:** the branch must build and pass tests in the
  `prestobuild/dependency:cio-new` docker image (gcc/g++ 14, `--gpus all`); the
  gating test is `ucx_exchange_test`, exercised **through the registry**. CPU
  registry tests (`ExchangeTransportRegistryTest`, `ExchangeTransportTest`) also
  pass.
- **Out of scope:** the full UCX symmetry roadmap (Task ownership, memory-pool
  accounting, byte-budget backpressure — see `exchange-client-symmetry` memory).
  Only the minimum UCX adaptation needed to implement `exec::ExchangeClient` and
  resolve through the registry is in scope. Splitting the PoC into upstream-ready
  PRs was originally out of scope and is now planned in
  `docs/designs/upstream-pr-plan.md`, though not yet executed.

## Ground truth (verified against merged `main` = `dan13bauer/main`, 16980 = `b6040fcfc`)

The `TransportKind` and `OutputTransportRegistry` mirror sources are byte-identical
between the pre-merge fetched tree (`356710339`) and merged `main`; only unrelated
main drift (e.g. the `kRightAnti` join type) differs. So these facts, originally
verified against the fetched tree, still hold on the merged base:

1. **`TransportKind` lives in `velox/core/PlanNode.h`**, not `PlanFragment.h`.
   #16980 moves it there. Constants: `kInMemory{"in-memory"}` (default),
   `kHttp` = deprecated alias of `kInMemory`, `kUcx{"UCX"}` (note: uppercase).
   NODE FIELD's reuse of `TransportKind::kInMemory` therefore resolves against the
   base with no new definition.
2. **#16980 seeds only the in-memory registry entry.** UCX is NOT wired into
   `OutputTransportRegistry`. There is no send-side precedent for a UCX registry
   entry — the receive-side UCX wiring (commit 5) is genuinely new work.
3. **`velox/experimental/ucx-exchange` is already in the upstream 16980 tree**
   (identical tree object `455f721d…`). `ucx_exchange_test` builds without
   importing fork-only code.
4. **`UcxExchange` / `UcxExchangeClient` are unwired** — constructed only in
   tests. No transport-type selection exists in this tree (the selection logic
   lived in the fork's cudf driver-adapter, absent upstream). `UcxExchangeClient`
   is a standalone class, not an `exec::ExchangeClient`.
5. **`ucx_exchange_test` is a GPU test** (`cuda_driver` label, TIMEOUT 3000),
   driven by `SinkDriverMock` / `SourceDriverMock`, not Task/LocalPlanner.
6. **Deleting `toOperator(..., exchangeClient)` is in-tree safe.** Only the
   translator-fallback path (`LocalPlanner.cpp:717-722` → `Operator.cpp:179-184`
   → `Operator.h:141`) and `Driver.h:877` reference `requiresExchangeClient()`;
   the built-in Exchange/MergeExchange use the direct `dynamic_cast` branch
   (`LocalPlanner.cpp:551/558`). Nothing in `cudf`/`ucx` experimental depends on
   the overload.

   **Correction (2026-08-14): true of Velox, misleading about consequence.**
   Prestissimo is the sole user of that extensibility surface, and it uses all of
   it: `presto_cpp/main/operators/ShuffleRead.h` declares
   `class ShuffleReadNode : public velox::core::PlanNode`, overrides
   `requiresExchangeClient()` to `true`, and overrides
   `toOperator(..., shared_ptr<ExchangeClient>)`. So the deletion is safe to
   compile in this tree and breaks a downstream consumer. Two consequences the
   original plan missed, both recorded in `docs/designs/upstream-pr-plan.md`:
   the deletion cannot ship in the same PR as the registry, and the
   `Driver.h` predicate conversion below is **not** behavior-preserving.

## Output-side template (mirror source, all in the 16980 tree)

| Concern | Send side (#16980) | Receive side (this PoC) |
|---|---|---|
| Plan-node field | `PartitionedOutputNode.transportKind` | `ExchangeNode.transportKind` |
| Registry | `OutputTransportRegistry` | `ExchangeTransportRegistry` |
| Entry | `{ OutputBufferManager, PartitionedOutputFactory }` | `{ ExchangeClientFactory, ExchangeFactory }` |
| Operator ctor | `PartitionedOutput(id, ctx, node, eagerFlush, manager)` | `Exchange(id, ctx, node, client)` |
| Interface / concrete | abstract `OutputBufferManager` / `DefaultOutputBufferManager` | abstract `ExchangeClient` / `InMemoryExchangeClient` |
| UCX impl | `UcxOutputQueueManager` + `UcxPartitionedOutput` | `UcxExchangeClient` + `UcxExchange` |

Reference files to mirror: `OutputTransportRegistry.{h,cpp}`,
`OutputBufferManager.h` (abstract), `DefaultOutputBufferManager.h`,
`OutputBufferStats.h`, `PartitionedOutputFactory.h`,
`tests/OutputTransportRegistryTest.cpp`, `tests/OutputTransportTest.cpp`.

Note the one real asymmetry (from the plan memory): there is no long-lived,
process-wide "manager" instance on the receive side. The `ExchangeClient` is
**owned by the Task and shared across drivers** — `Task::createExchangeClientLocked`
(`Task.cpp:3739`) does a single `make_shared<ExchangeClient>` per exchange plan
node (`Task.cpp:3754`) and stores that one shared_ptr in both `exchangeClients_`
(by pipeline) and `exchangeClientByPlanNode_` (by plan-node id); all Exchange
operator instances for that node, across all drivers, receive the same client via
`getExchangeClientLocked` (`Task.cpp:1541`). There is one client per exchange
node, not one per operator instance. Because the client is per-task(-per-node)
state — not a process-wide singleton — the query-scoped, stateless registry cannot
hold the client instance; it holds an **ExchangeClient factory** (plus the
operator factory), which Task invokes once per node at the current
`make_shared<ExchangeClient>` site. In the output-side mapping, `ExchangeClient`
is thus the analog of `OutputBuffer` (per-task state), not the process-wide
`OutputBufferManager` service; the registry is never keyed by task.

## Branch shape — planned as 5 commits, landed as 38

The five layers below are what the branch contains, and the assembly approach held:
rebuild onto merged `main`, apply upstream #18110, then implement, linear history,
one concern per commit. The commit count grew because implementation surfaced work
the plan did not anticipate — the UCX output side, session configuration, module
build enablement, register/unregister symmetry, and end-to-end tests — and because
two attempts were made and reverted (below). The layer descriptions are kept as
written, with corrections marked where the outcome differed.

### Commit 1 — `refactor(exec)!: Rename ExchangeClient → InMemoryExchangeClient`
Apply upstream PR #18110's rename. On the merged-16980 base its net diff is
exactly the rename (15 files) and applies **cleanly** — 16980 is already in the
base, so there is no conflict to resolve. The concrete `exec::ExchangeClient`
becomes `InMemoryExchangeClient`, freeing the name for the abstract interface
introduced in commit 3.
- Keep the `!` + `BREAKING CHANGE:` footer (public `toOperator` override / removed
  header) as on #18110.

### Commit 2 — `feat(core): Add transportKind to ExchangeNode`
Cherry-pick `05439a07e` (reusable as-is; touches only `core/PlanNode.{h,cpp}` +
serde/builder/trace/toString tests).
- `transportKind` next to `serdeKind`; Builder requires it explicitly; old ctor
  behind `VELOX_ENABLE_BACKWARD_COMPATIBILITY`; `create()` defaults to
  `kInMemory` for old serialized plans only; `addDetails` prints the transport.
- Conflict point: the base already carries 16980's `PlanNode.{h,cpp}` and
  serde/toString edits (for `PartitionedOutputNode.transportKind`) plus Commit 1's
  rename touching `MultiFragmentTest.cpp`; resolve so all coexist.

### Commit 3 — `feat(exec): Exchange transport registry and per-transport resolution`
New code, mirroring `OutputTransportRegistry`.
- Abstract `ExchangeClient` interface (control plane only:
  `addRemoteTaskId` / `noMoreRemoteTasks` / `close` / `stats`); concrete
  `InMemoryExchangeClient` implements it (data plane `next()`/`queue()` stays
  concrete). Pre-apply #16980's interface review lessons (below).
- `ExchangeTransportRegistry` = query-scoped (per-query override + global
  fallback), plain-aggregate entry `{ clientFactory, operatorFactory }`. In-memory
  entry seeded at creation and restored by `unregisterAll()`; a named transport
  with no entry FAILS FAST (`tryGet` + throw in Task, no fallback knowledge).
- Resolution: `Task` builds the client from the entry's factory at the current
  `make_shared<ExchangeClient>` site (`Task.cpp:3754`, in
  `createExchangeClientLocked`) — still once per exchange node, still stored in
  `exchangeClients_` / `exchangeClientByPlanNode_` and shared across drivers;
  `LocalPlanner.cpp:558` builds the operator from the entry's operator factory.
- **Delete** `toOperator(..., exchangeClient)` (`Operator.h:141`,
  `Operator.cpp:179-184`), the `requiresExchangeClient()` overrides
  (`PlanNode.h`) and the `LocalPlanner.cpp:717-722` fallback branch. The one
  remaining `requiresExchangeClient()` use — `DriverFactory::needsExchangeClient()`
  at `Driver.h:877` — is a legitimate "is this pipeline exchange-fed?" query, so
  it is **converted, not deleted**: replace the predicate with
  `dynamic_pointer_cast<const core::ExchangeNode>`, returning the
  `ExchangeNodePtr` so Task can read `transportKind` without a second lookup.
  Client stays Task-owned; registry never keyed by task.
  **Correction (2026-08-14): the conversion is not behavior-preserving.** The
  claim that `ExchangeNode` is the only node overriding the predicate to `true`
  holds in this tree but not downstream — `ShuffleReadNode` overrides it and is
  not an `ExchangeNode`, so the cast returns nullptr, no client is created, and
  the operator receives null at runtime with nothing failing to compile. Upstream,
  the predicate branch has to survive alongside the cast until Prestissimo
  migrates; on this branch it does not, because the branch has no downstream
  consumer to keep working. See `docs/designs/upstream-pr-plan.md`.
- CPU tests: `ExchangeTransportRegistryTest` (registration, per-query override vs
  global fallback, getAll, always-available in-memory default, isolation) in its
  own binary; `ExchangeTransportTest` (Task resolves client+operator by transport;
  errors on unregistered; uses default after clear) with a CPU test-transport
  double. Serde round-trip incl. non-default `kUcx` in `PlanNodeSerdeTest`;
  `PlanNodeToStringTest` for the new `addDetails`.

### Commit 4 — `feat(exec): Per-transport resolution for MergeExchange`
Extend resolution to `MergeExchangeNode` + `MergeExchange` operator +
`MergeSource` client creation (`MergeSource.cpp:224`), mirroring commit 3.

**Outcome: only half of this landed, deliberately.** `MergeExchange` reads the
node's `transportKind` and resolves a registry entry, but then downcasts the
result to `InMemoryExchangeClient` — so merge remains in-memory-only and nothing
in `velox/exec/` learned about UCX. Pushing resolution down into `MergeSource` was
implemented and then reverted: `MergeExchangeSource` needs the client's `next()`,
which is data plane and deliberately absent from the abstract interface, so a
generic client would have to be cast straight back to the concrete type. Merge
over UCX is instead handled one layer up — see the substitution below.

### Commit 5 — `feat(ucx): Wire UCX transport into ExchangeTransportRegistry`
The PoC payoff.
- Make `UcxExchangeClient` implement the abstract `exec::ExchangeClient`
  interface (minimum adaptation: satisfy the control-plane contract; do not take
  on the full symmetry roadmap).
- Register a UCX entry `{ clientFactory → UcxExchangeClient, operatorFactory →
  UcxExchange }` keyed `TransportKind::kUcx`.
- Extend the `ucx_exchange_test` harness (`SinkDriverMock` / `SourceDriverMock`)
  so `UcxExchange` / `UcxExchangeClient` are resolved **from the registry** for a
  plan with `transportKind = kUcx`, proving registry-driven selection end-to-end.

**Outcome: done, and it pulled in four more layers.** Registering the receive side
alone does not produce a working shuffle, so the branch also carries:
- the **UCX output side** — `UcxOutputQueueManager` implements
  `exec::OutputBufferManager` and registers in `OutputTransportRegistry`, so both
  ends of a shuffle are transport-selected;
- **session configuration** — `CudfConfig` keys gate and tune the transport, and
  registration happens from `registerCudf()` only when the gate is on;
- **module build enablement** — the module had never been compiled against current
  main; UCX is now a hard requirement of `VELOX_ENABLE_CUDF`, since the GPU
  exchange operators link it unconditionally;
- **register/unregister symmetry** — `unregisterUcxExchange()` removes both
  entries, because the output entry holds a strong reference to a process-wide
  singleton that would otherwise outlive the registration that created it.

### Merge over UCX — a device sort, not a merge
Not in the original plan. A cuDF operator adapter replaces the `exec::MergeExchange`
of a `kUcx` `MergeExchangeNode` at driver-build time with `{UcxExchange,
CudfOrderBy}`: the whole input arrives over UCX on one driver and is sorted once on
the device. That is globally ordered output by a different algorithm, and it is the
only option available — the host k-way merger caches raw pointers to its key
columns, while the UCX receive path yields `CudfVector`s whose columns live on the
device with no host children to point at. See
`docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md`.

## Design invariants (pre-applied, from #16980 review history)

- Reuse `core::TransportKind` string constants; no parallel enum.
- Built-in in-memory transport is a real seeded registry entry; every lookup is a
  plain read; unregistered named transport fails fast.
- Node ctor: `transportKind` next to `serdeKind`, no silent default; Builder and
  factories require it explicitly. Only serde deserialization of an old plan
  missing the field defaults to `kInMemory`.
- Registry entry is a plain aggregate; single `tryGet(queryCtx, id)` returns it;
  per-query override + global fallback.
- Operators receive their concrete client via constructor (pairing lives in the
  type system, not a runtime cast).
- Abstract `ExchangeClient` interface: depend on no concrete-impl types
  (`stats()` stays a neutral `F14FastMap<string, RuntimeMetric>`, no concrete
  queue include); fully specify each method's contract (lifecycle ordering,
  thread-safety — one instance driven from many driver threads — who calls it,
  "unknown taskId" vs valid value); define completion in terms of
  `noMoreRemoteTasks`/`close`, not impl detail.
- Comment discipline: `///` only for public API in headers; document every public
  class/method/member; comments explain *why*; no restating code.

## Verification

- Build + run in `prestobuild/dependency:cio-new` (gcc/g++ 14, `--gpus all`;
  override baked `CC/CXX`, `VELOX_BUILD_TESTING=ON`) per the
  `velox-gpu-unit-tests-in-dep-image` memory.
- Gating test: **`ucx_exchange_test`** green (GPU, `cuda_driver`, TIMEOUT 3000),
  resolving UCX through the registry.
- Also green: `ExchangeTransportRegistryTest` (own binary,
  `velox_exchange_transport_registry_test`, since it mutates global registry
  state), `ExchangeTransportTest`, `PlanNodeSerdeTest`, `PlanNodeToStringTest`,
  and the existing exchange/task tests touched by the rename and registry.
- Coverage grew past the harness-level gate. `ucx_exchange_test` now also runs a
  two-fragment **Task-level** plan whose `PartitionedOutputNode` and
  `ExchangeNode` both declare `kUcx`, resolved through both registries with
  `registerCudf()` standing in for a worker's registration; a twin case runs the
  same plan on the default transport and asserts identical rows, which makes "the
  in-memory path is unchanged" executable rather than assumed; and a third case
  asserts a `kUcx` `MergeExchange` is globally ordered, with two producers
  carrying interleaved key ranges so neither is ordered alone.
- `make format` (via the repo pre-commit hook, per the `formatting-use-githook`
  memory) before finishing.

## Risks / open points — outcomes

The risks the plan named all resolved:

- **Cherry-pick conflicts** in commit 2 were small, as expected, and the rename
  applied cleanly.
- **UCX client interface fit** held. `UcxExchangeClient` satisfies the abstract
  control plane with signature adjustments only. The push-model divergences (no
  request scheduler, count-based backpressure, empty `stats()`) remain as
  transport-imposed divergence, per `exchange-client-symmetry`.
- **Registry-driven selection in the GPU test** was the main harness work, and it
  landed: the mock drivers resolve both client and operators through the registry
  rather than constructing them.
- **Not upstream-shaped** is still true and now deliberate. The split is planned in
  `docs/designs/upstream-pr-plan.md` rather than left as unscoped future work.

## Remaining limitations

- **No true device-side k-way merge.** `cudf::merge` would need per-source
  attribution of incoming batches. `PackedTableWithStream` carries only
  `{packedTable, stream}` and the receive queue is a single FIFO across all remote
  sources, so attribution would have to be added to the wire protocol.
- **Nothing inserts GPU/host conversions around the UCX exchange operators.**
  Neither `UcxExchange` nor `UcxPartitionedOutput` is a `CudfOperator`, and no
  operator adapter claims `Exchange` or `PartitionedOutput` — they resolve through
  the two registries instead — so `CompileState` treats both as CPU operators and
  places no `CudfFromVelox` / `CudfToVelox` beside them. The end-to-end cases
  sidestep this by feeding `Values` device-resident batches and converting the
  consumer's output in the test. A substituted `MergeExchange` is unaffected,
  because `CudfOrderBy` is a `CudfOperator` and gets its `CudfToVelox` normally.
- **Merge is in-memory-only in `velox/exec/`.** A `kUcx` `MergeExchange` that the
  cuDF adapter does not claim — cuDF exchange disabled, CPU fallback — has no
  transport to run on. Upstream this is an explicit rejection rather than a silent
  fall back to the in-memory transport.

## Related

- `docs/designs/upstream-pr-plan.md` — how this branch splits into upstream PRs.
- `POC-README.md` — the as-built walkthrough of what landed.
- `exchange-transport-registry-plan` memory — the original 4-PR plan this PoC
  realizes, superseded on the split by the document above.
- `exchange-client-symmetry` — HTTP vs UCX client gaps; bounds the UCX adaptation.
- `pr-16980-output-transport-registry`, `pr-18110-rename-exchange-client`.
- `velox-gpu-unit-tests-in-dep-image` — the docker build/run recipe.
