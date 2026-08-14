# Upstream PR plan — exchange transport integration

## Purpose

The integration branch `poc-exchange-transport-integration` does two things. It makes
the transport an Exchange reads over a property of the query plan, resolved at runtime,
instead of a compile-time choice baked into the engine. And it adds a second transport —
a GPU-oriented one built on UCX — as the first real user of that mechanism, on both the
producing and consuming side of a shuffle.

That is roughly 2,800 changed lines across 61 files, developed as a single branch. No
reviewer can usefully assess it in one piece, and slicing it arbitrarily produces
intermediate states that either fail to compile or compile and then misbehave.

This document breaks the branch into a sequence of upstream pull requests. The goal is
that **every merge leaves a working tree**: it builds, its tests pass, and nothing that
worked before it stops working. Those are two separate obligations, and the second is
the harder one — a build break stops CI, while a runtime break passes CI and surfaces
later in someone else's cluster. Where a PR risks the latter, this plan says so and says
what to do about it.

## How this plan is organised

**It supersedes the open upstream ucx-exchange pull requests.** Those were written
against an earlier shape of the design and are no longer rebasable; they should be
closed as their replacements go up. The one exception is **#17422**, whose content is
not on this branch at all — it is a sibling to rebase onto this stack later, not a draft
to close.

**Velox main already carries the producing-side half of the design** (#16980): the
output transport is a plan property resolved through a registry, with an abstract output
buffer manager behind it. This plan is the consuming-side mirror of that, plus the UCX
implementation of both halves. Nothing here waits on further upstream work.

### The five tracks

- **Track A — core engine.** Make the receive-side transport a plan property and resolve
  it at runtime through a registry, so a transport can be added without touching the
  engine.
- **Track B — UCX producing side.** Make the module's output queue satisfy the engine's
  existing output-buffer contract.
- **Track C — configuration and build.** Make the module selectable through session
  configuration, and get it compiled by upstream CI at all.
- **Track D — UCX consuming side.** Make the module's client satisfy Track A's new
  contract, and register both halves of the transport so a plan can name it.
- **Track E — merge exchange over UCX.** Make a merge exchange work over the UCX
  transport, which the host-side k-way merger cannot do on device-resident data.

### Why five tracks rather than three

The obvious division is core engine / adapt the module / build. The tracks here are
grouped instead by **what a PR depends on and what it can break**, which cuts across
that:

- **The UCX module splits into two independent halves.** Its producing side needs only
  what main already has; its consuming side needs Track A's new contract. Treating
  "adapt the module" as one unit would chain the half that is ready now to the slowest
  work in the plan.
- **Build comes early, not last.** None of the module is compiled by upstream CI today,
  so until the build lands, every module PR is unverifiable by definition — approved by
  reviewers, compiled by nobody. Configuration leads the build within Track C, because
  the build compiles code that reads the configuration keys.
- **Track A is split by risk, not by feature.** Declaring the plan field is inert;
  adding the registry is purely additive; wiring resolution changes runtime behaviour;
  removing the old path breaks a downstream consumer. Only the last needs a cross-repo
  migration, so bundling them would hold the safe majority hostage to the one PR that
  cannot land alone.
- **Track E uses the transport rather than building it.** Merge exchange is the first
  operator to need the transport working end to end, so it trails everything.

**One cross-repo constraint.** A single PR in Track A deletes engine API that
Prestissimo overrides. That one is gated on a Prestissimo migration landing first;
every other PR in this plan is additive or explicitly backward-compatible.

The consumer in question is Prestissimo's **shuffle read**, which belongs to its
batch execution path rather than interactive Presto. Interactive queries keep a
producer's output in a buffer that the consumer fetches while both tasks are alive;
batch queries instead write partitioned output to an external shuffle system and let
the producer exit, so the consumer reads it back later. Shuffle read is the operator
that does that reading, and batch mode substitutes it for the ordinary exchange
operator on every partitioned edge. It matters here because it is built through the
very extensibility hook this plan removes — and note that it is orthogonal to UCX:
it is the ordinary in-memory exchange client reading from a different source, not a
different transport, which is why the earlier UCX work never covered it.

---

## PR set

### Track A — core engine

**A1 · `refactor(exec)!: Rename ExchangeClient to InMemoryExchangeClient` — in flight**

Renames today's concrete exchange client so the name `ExchangeClient` is free for the
abstract interface that follows. Already open and approved upstream as #18110, carrying
a backward-compatibility alias for downstream callers — not ours to re-file. Rebase this
branch onto it rather than re-deriving the rename.

- `velox/exec/InMemoryExchangeClient.{h,cpp}`
- `velox/exec/Exchange.{h,cpp}`
- `velox/exec/MergeSource.cpp`
- `velox/exec/tests/InMemoryExchangeClientTest.cpp`
- `velox/exec/CMakeLists.txt`

**A2 · `feat(core): Add transportKind to ExchangeNode`**

Adds a transport field to the exchange plan nodes, alongside the serialization-format
field they already carry, so a plan can name the transport it wants. Nothing reads the
field yet, so the change is inert. Deserializing a plan that predates the field defaults
it to the in-memory transport.

- `velox/core/PlanNode.{h,cpp}`
- `velox/core/tests/PlanNodeBuilderTest.cpp`
- `velox/exec/trace/TraceUtil.cpp`
- `velox/exec/tests/PlanNodeSerdeTest.cpp`
- `velox/exec/tests/PlanNodeToStringTest.cpp`
- `velox/exec/tests/utils/PlanBuilder.{h,cpp}`

*Must:* stay purely additive — the removal of the old `requiresExchangeClient()` hook
belongs to A5. Keep the pre-existing node constructors behind the
backward-compatibility macro, and expect a bundled Prestissimo change for the
open-source build, where that macro is never defined.

⚠️ Opens a runtime hole on the merge path: the merge operator inherits the new field but
ignores it, so a plan naming UCX is silently served over the in-memory transport. A4
closes this and should follow immediately.

**A3a · `feat(exec): Abstract ExchangeClient interface and ExchangeTransportRegistry`**

Turns the exchange client into an abstract control-plane interface, and adds a registry
that pairs each transport's client factory with the factory for its matching operator so
the two cannot diverge. Nothing resolves through the registry yet — it is exercised only
by its own tests, giving this PR no runtime surface at all. Its real job is to publish
the API that downstream consumers need in order to migrate.

- `velox/exec/ExchangeClient.h`
- `velox/exec/ExchangeFactory.h`
- `velox/exec/ExchangeTransportRegistry.{h,cpp}`
- `velox/exec/InMemoryExchangeClient.{h,cpp}`
- `velox/exec/Exchange.{h,cpp}`
- `velox/exec/CMakeLists.txt`
- `velox/exec/tests/CMakeLists.txt`
- `velox/exec/tests/ExchangeTransportRegistryTest.cpp`
- `velox/exec/tests/ExchangeTransportTest.cpp`

*Must:* not touch the task, the planner, or the driver factory — that is A3b. Preserve
the backward-compatibility alias A1 introduces rather than overwriting the header it
lives in.

**A3b · `feat(exec): Resolve the exchange client and operator through the registry`**

Makes the task build its exchange client, and the planner build its exchange operator,
from the registry entry the plan names. A plan naming a transport that is not registered
fails the query — deliberately, with no silent fall back to the in-memory transport.
Every runtime hazard in this plan lives in this one PR.

- `velox/exec/Task.{h,cpp}`
- `velox/exec/LocalPlanner.cpp`
- `velox/exec/Driver.h`

*Must:*
- Keep creating a client for leaf nodes that are *not* exchange nodes but still request
  one. Deciding by node type alone starves Prestissimo's shuffle-read operator of its
  client — no compile error, null pointer at runtime.
- Keep the planner's pre-existing operator-construction fallback alongside the new
  registry path.
- Leave both compatibility paths as **unconditional** code, never behind the
  backward-compatibility macro. That macro is defined only by Meta's internal build, so
  guarding them would pass internally and fail at runtime in the open-source build.
- Use designated initialisers for the new client-context struct at every call site; it
  has three adjacent integral fields that a reorder would silently transpose.

**A4 · `fix(exec): Reject non-in-memory transports in MergeExchange`**

Closes the hole A2 opens: the merge operator now reads the plan's transport field and
rejects anything but the in-memory transport instead of silently ignoring it. It makes
no registry call, so it stays off the cross-repo-gated part of Track A. Making merge
genuinely transport-generic is a separate project — that direction was attempted on this
branch and reverted.

- `velox/exec/Merge.{h,cpp}`

*Must:* leave the merge-source layer alone. Only one of the four client calls it makes
is off the abstract interface, so resolving a client through the registry and casting it
straight back to the concrete type would be indirection to nowhere.

**A5 · `refactor(exec)!: Remove the ExchangeClient-taking operator translator path`**

Deletes the pre-registry extensibility path — the client-taking operator translator, the
node hook that requested a client, and the compatibility branches A3b kept. This is the
only PR here that breaks a downstream consumer, and deletions cannot be aliased, so it
merges only after Prestissimo's shuffle read has stopped using that path — either by
dropping its own node and operator in favour of a plain exchange node, or by registering
an operator factory for them. See the open questions; that choice is unsettled and does
not change this PR's diff.

- `velox/exec/Operator.{h,cpp}`
- `velox/core/PlanNode.h`
- `velox/exec/LocalPlanner.cpp`
- `velox/exec/tests/MultiFragmentTest.cpp`
- removal of A1's compatibility alias header and its include edge

### Track B — UCX producing side

**B1 · `feat(ucx-exchange): Make UcxOutputQueueManager implement OutputBufferManager`**

Makes the module's output queue manager implement the engine's abstract output-buffer
contract, so it can be registered as a transport rather than special-cased. The
transport-specific data plane stays off that interface by design, since its payloads are
GPU-resident. Independent of Track A — it needs only what main already provides, so it
can land in parallel.

- `velox/experimental/ucx-exchange/UcxOutputQueueManager.{h,cpp}`
- `velox/experimental/ucx-exchange/UcxQueues.{h,cpp}`
- `velox/experimental/ucx-exchange/tests/UcxOutputQueueManagerTest.cpp`

*Must:* keep the registration alive until every task it initialised has finished, per the
interface's lifetime contract. In tests, drop the module's own entries specifically
rather than clearing the whole registry.

### Track C — configuration and build

**C1 · `feat(cudf): Add the UCX exchange configuration keys to CudfConfig`**

Adds the session-configuration keys that select and tune the UCX exchange, and parses
them into the cuDF configuration object. Purely additive and independently verifiable,
since its test touches nothing but the configuration type. Must precede the build PR,
which compiles code that reads these keys.

- `velox/experimental/cudf/CudfConfig.h`
- `velox/experimental/cudf/exec/ToCudf.cpp`
- `velox/experimental/cudf/tests/ConfigTest.cpp`

**C2 · `build(ucx-exchange): Build the ucx-exchange module against current main`**

Gets the module compiled by upstream CI for the first time, and fixes the drift that
accumulated while it was not: headers that upstream split, dependency APIs upstream
removed, and a missing library link. Also makes UCX a hard requirement of a cuDF build
and fails configuration with a named remedy when it is absent — the GPU exchange
operators link the module unconditionally, so a cuDF build without UCX cannot succeed,
and the old form degraded to a confusing late compile error instead of saying so.

- `velox/CMakeLists.txt`
- `CMake/resolve_dependency_modules/cudf.cmake`
- `velox/experimental/ucx-exchange/CMakeLists.txt`
- `velox/experimental/ucx-exchange/UcxExchangeServer.h`
- `velox/experimental/ucx-exchange/UcxExchangeSource.h`
- `velox/experimental/ucx-exchange/Communicator.{h,cpp}`
- `velox/experimental/ucx-exchange/UcxPartitionedOutput.{h,cpp}`
- `velox/experimental/ucx-exchange/tests/UcxTestData.cpp`
- `velox/experimental/ucx-exchange/tests/UcxTestHelpers.cpp`

**C3 · `test(ucx-exchange): Pick the communicator port per process and harden teardown`**

Makes the module's test suite reliable enough to be a gate for everything after it: the
listener port is chosen per process instead of hardcoded, and teardown no longer trusts
that setup succeeded or drops the communicator before joining its thread.

- `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`

### Track D — UCX consuming side

**D1 · `feat(ucx-exchange): Implement the abstract ExchangeClient interface`**

Makes the module's exchange client implement Track A's new control-plane interface,
adjusting the signatures that differ from it. Conformance only — nothing resolves this
client through the registry yet.

- `velox/experimental/ucx-exchange/UcxExchangeClient.{h,cpp}`
- `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`

**D2 · `feat(ucx-exchange): Register the UCX exchange and output transports`**

Registers both halves of the UCX transport, gated on the configuration key, and provides
the counterpart that removes them again — the output entry holds a strong reference to a
process-wide singleton, so leaving it behind outlives the registration that created it.
This is the first point at which a plan naming UCX actually gets UCX end to end, and it
carries the task-level tests that prove it, including a twin case on the default
transport asserting identical results.

- `velox/experimental/ucx-exchange/UcxExchangeRegistration.{h,cpp}`
- `velox/experimental/ucx-exchange/CMakeLists.txt`
- `velox/experimental/cudf/exec/ToCudf.cpp`
- `velox/experimental/ucx-exchange/tests/SinkDriverMock.{h,cpp}`
- `velox/experimental/ucx-exchange/tests/UcxTestHelpers.cpp`
- `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`
- `velox/experimental/ucx-exchange/tests/UcxOutputQueueManagerTest.cpp`

*Must:* land after C2, which is what guarantees the module is present to link against.
Registration must be idempotent, and the configuration object must be initialised before
registration runs or the gate sees a stale value.

### Track E — merge exchange over UCX

**E1 · `feat(cudf): Replace MergeExchange with UcxExchange and CudfOrderBy for UCX`**

Replaces a UCX merge exchange with a plain UCX exchange followed by a GPU sort, when the
GPU exchange is enabled. The host merger compares rows on the host and cannot work on
device-resident data, so the UCX path receives everything and sorts once on the device
instead. Also removes the UCX transports when the cuDF operators are unregistered,
keeping registration symmetric.

- `velox/experimental/cudf/exec/OperatorAdapters.cpp`
- `velox/experimental/cudf/exec/CudfOrderBy.{h,cpp}`
- `velox/experimental/cudf/exec/CMakeLists.txt`
- `velox/experimental/cudf/exec/ToCudf.cpp`
- `velox/experimental/cudf/tests/AdapterOperatorTest.cpp`
- `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`

*Note:* independent of A4. The substitution happens while the driver is built, before
the merge operator would ever run, so A4's guard covers the complementary case where the
substitution does not fire.

---

## Dependency graph

```
main  (output side already a plan property; scoped registry; transport ids)
│
├─ A1 rename ─────────┬─ A3a registry (pure additive) ──┐  [IN FLIGHT / non-draft]
│                     │        │                        │
├─ A2 node field ─────┴──┬─────┼────────────────────────┤
│                        │     │                        ▼
│                        │     │                   A3b resolution (carries compat)
│                        │     │                        └── A5 contract [Presto-gated]
│                        │     ▼                             ▲
│                        │  (Presto migration, ∥ with A3b) ──┘
│                        │
│                        └─ A4 merge guard   [off the Presto-gated front]
│
├─ C1 config keys ── C2 build ──┬── B1 ucx producing side ┐
│                               ├── C3 test fixes         │
│                               └── D1 ucx client ────────┴── D2 registration ── E1 merge over UCX
```

Two fronts that run independently and converge at D2: **A1→A3a→A3b** (core, coupled to
the Prestissimo migration) and **C1→C2→B1/D1** (UCX, self-contained). A3a exists so the
Prestissimo migration can start while A3b is still in review; A4 hangs off A2 rather
than A3b so the hole A2 opens closes immediately.

## Filing order

1. **C1** (config keys) and **A2** (node field) — neither depends on anything open.
2. **A4** — immediately after A2, closing the hole A2 opens.
3. **C2** (build) — unblocks CI signal for everything in Tracks B, C and D.
4. **A3a** — as soon as A1 merges. Purely additive; hands Prestissimo its migration
   target.
5. **B1**, **C3**, **D1** — parallel, none needs A3b.
6. **A3b** — carries the unconditional compatibility paths.
7. **D2** → **E1**.
8. **A5** — only after the Prestissimo migration merges.

## Open questions

### On A5 and the Prestissimo migration

1. **Who writes the Prestissimo side?** #28316 (A1's companion, ours, draft) sets the
   precedent that we do. A5's is larger in either of the two shapes below.

2. **Delete shuffle read, or register it?** This is the substantive decision, and it
   decides question 3 as well.

   Shuffle read's operator already *is* the engine's exchange operator: it derives from
   it and fabricates an exchange node internally with the compact-row serialization
   format. Everything specific to the external shuffle lives one layer lower, in a
   custom exchange source. The operator therefore differs from a plain exchange in only
   two respects — an operator-type label used in stats and plan output, and an override
   that returns its own compact-row serializer instance.

   - **Delete (cheaper).** That override is redundant: the engine's exchange operator
     already resolves the serializer from the node's format field through the named-serde
     registry, and Prestissimo does register compact row there. So batch translation can
     emit a plain exchange node with the compact-row format and the in-memory transport,
     and the node type, the operator, and the translator all go away. No new transport
     id, no registry entry, no re-parenting. The cost is the lost operator-type label —
     confirm nothing consumes it before choosing this.
   - **Register (preserves the label).** Keep the operator and give it a registry entry
     pairing the in-memory client factory with its own operator factory. This needs an
     id, and the id's only job is selecting the operator — it does not denote a
     transport. The registry accepts arbitrary strings and there is in-tree precedent for
     a non-standard one, so Prestissimo can define its own without touching the shared id
     list; confirm that open namespace is intended, then pick the string. Reusing the
     in-memory id does **not** work, because the planner would resolve the built-in entry
     and build a plain exchange operator instead.

3. **Serialized-form compatibility — likely a non-issue.** The coordinator sends the
   worker protocol JSON, not an engine-serialized plan; batch translation constructs the
   shuffle-read node locally from a remote-source node. The engine-level serde
   registration for it therefore serves plan serialization for tracing, replay and
   tests, not a coordinator-to-worker wire format, so gaining two serialized fields
   crosses no version boundary between them. The only residual question is whether any
   persisted engine-serialized plan is replayed across versions. Under the delete option
   this disappears, though deregistering the node type is itself worth a thought if a
   stored plan still names it.

4. **Is A5 committed work or an open follow-up?** What it removes costs roughly ten lines
   plus one virtual. If the Prestissimo migration is expensive, A5 can sit indefinitely
   without blocking anything else — but that changes how A3b's compatibility paths should
   be commented.

### On the Prestissimo companion PRs

5. **What does A2's companion pass at the node construction sites — the in-memory id, or
   a real per-node value?** The Presto change that carries per-node transport types into
   the plan fragment is still open, so the real value is not available on master yet.
   Either land a mechanical default now and revisit, or wait and do it once.
6. **Standalone Prestissimo PR, or bundled into the velox submodule advance?** The
   recorded pattern is that standalone follow-up PRs get closed unmerged and the change
   lands inside the advance. Decide which, and who runs the advance.
7. **Does #28316 grow to cover A2, or stay A1-only?** If A1 and A2 both merge before the
   next advance, one Prestissimo PR covering both is less churn, and #28316 is still a
   draft.
