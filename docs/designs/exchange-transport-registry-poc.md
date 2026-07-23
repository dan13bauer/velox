# Exchange transport registry — receive-side proof-of-concept integration branch

Date: 2026-07-23
Status: Design approved (brainstorming); implementation plan pending.
Branch: `poc-exchange-transport-integration` (off merged `main` = `dan13bauer/main` `49e8aac49`, which contains PR #16980 as commit `b6040fcfc`).

## Goal

Build a proof-of-concept integration branch demonstrating the **receive-side**
exchange-transport registry — the symmetric counterpart to upstream PR #16980
(output side) — with a **real UCX transport wired through the registry** and the
GPU `ucx_exchange_test` passing end-to-end.

Unlike the eventual clean 4-PR stack (rename → node field → registry →
mergeexchange, each against `main`), this PoC **co-locates every layer on one
branch off merged `main`** (which now contains #16980) so the whole receive path
can be built and tested together, including the UCX transport that #16980's
registry enables.

## Scope

- **In scope:** the four mirror commits (rename, node field, registry,
  mergeexchange) implemented to production fidelity, plus a fifth commit wiring
  the UCX transport into the new registry. Full pre-application of #16980's open
  review feedback to the abstract `ExchangeClient` interface.
- **Verification:** the branch must build and pass tests in the
  `prestobuild/dependency:cio-new` docker image (gcc/g++ 14, `--gpus all`); the
  gating test is `ucx_exchange_test`, exercised **through the registry**. CPU
  registry tests (`ExchangeTransportRegistryTest`, `ExchangeTransportTest`) also
  pass.
- **Out of scope:** splitting the PoC back into upstream-ready PRs; the full
  UCX symmetry roadmap (Task ownership, memory-pool accounting, byte-budget
  backpressure — see `exchange-client-symmetry` memory). Only the minimum UCX
  adaptation needed to implement `exec::ExchangeClient` and resolve through the
  registry is in scope.

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

## Branch shape — 5 commits on merged `main`

Assembly approach: **rebuild onto merged `main`, apply upstream #18110, then
implement** (see the plan's Phase 0). Linear history, one concern per commit;
the design + plan doc commits sit first, then the five below.

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
  `dynamic_pointer_cast<const core::ExchangeNode>` (behavior-preserving —
  `ExchangeNode` is the only node overriding the predicate to `true` and
  `MergeExchangeNode` derives from it, so the cast selects the same set; matches
  the sibling `needsPartitionedOutput`/`needsLocalExchange` idiom). Optionally
  return the `ExchangeNodePtr` so Task can read `transportKind` to resolve the
  factory without a second lookup. Client stays Task-owned; registry never keyed
  by task.
- CPU tests: `ExchangeTransportRegistryTest` (registration, per-query override vs
  global fallback, getAll, always-available in-memory default, isolation) in its
  own binary; `ExchangeTransportTest` (Task resolves client+operator by transport;
  errors on unregistered; uses default after clear) with a CPU test-transport
  double. Serde round-trip incl. non-default `kUcx` in `PlanNodeSerdeTest`;
  `PlanNodeToStringTest` for the new `addDetails`.

### Commit 4 — `feat(exec): Per-transport resolution for MergeExchange`
Extend resolution to `MergeExchangeNode` + `MergeExchange` operator +
`MergeSource` client creation (`MergeSource.cpp:224`), mirroring commit 3.

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
- Gating test: **`ucx_exchange_test`** green (GPU, `cuda_driver`), resolving UCX
  through the registry.
- Also green: `ExchangeTransportRegistryTest`, `ExchangeTransportTest`,
  `PlanNodeSerdeTest`, `PlanNodeToStringTest`, and the existing exchange/task
  tests touched by the rename and registry.
- `make format` (via the repo pre-commit hook, per the `formatting-use-githook`
  memory) before finishing.

## Risks / open points

- **Cherry-pick conflicts** in commit 2 (NODE FIELD) where the base's merged
  16980 already edited `PlanNode.{h,cpp}` and the serde/toString tests, and
  Commit 1's rename touched `MultiFragmentTest.cpp`. Expected small. Commit 1
  itself (the rename from #18110) applies cleanly — 16980 is already in the base.
- **UCX client interface fit.** `UcxExchangeClient`'s control-plane surface must
  map cleanly onto the abstract `ExchangeClient`; the push-model divergences
  (no request scheduler, count-based backpressure, empty `stats()`) are known
  (`exchange-client-symmetry`). We adapt only what the interface requires and
  leave the rest documented as transport-imposed divergence.
- **Registry-driven selection in the GPU test.** The mock drivers construct
  operators directly today; routing them through the registry factory is the
  main new test-harness work.
- This PoC branch is not upstream-shaped (design doc committed here, everything on
  one branch). The clean 4-PR split remains future work.

## Related memory

- `exchange-transport-registry-plan` — the full 4-PR plan this PoC realizes.
- `exchange-client-symmetry` — HTTP vs UCX client gaps; bounds the UCX adaptation.
- `pr-16980-output-transport-registry`, `pr-18110-rename-exchange-client`.
- `velox-gpu-unit-tests-in-dep-image` — the docker build/run recipe.
