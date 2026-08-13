# MergeExchange over UCX — Design

**Date:** 2026-08-12
**Branch:** `poc-exchange-transport-integration`
**Supersedes:** Task D1 of `docs/superpowers/plans/2026-08-10-ucx-exchange-integration.md`
(commits `13aa7dbbd`, `b40b01ab6`), which is reverted by this design.

All line references are as of `b40b01ab6`. Reverting commit 1 shifts `Merge.cpp`
line numbers back by a few lines; the cited functions are unchanged.

## Goal

Make a `MergeExchangeNode` whose `transportKind` is `core::TransportKind::kUcx`
produce globally ordered output over the UCX/cuDF transport, without changing
anything under `velox/exec/` and without weakening the fail-fast behaviour the
plan requires for unsupported transports.

## Problem

`MergeExchange` merges k pre-sorted streams on the host. Before emitting
anything, `SourceStream::fetchMoreData` caches raw pointers to the key columns:

```
velox/exec/Merge.cpp:518-530
  atEnd_ = !data_ || data_->size() == 0;
  if (!atEnd_) {
    for (auto& child : data_->children()) { ... }
    keyColumns_.clear();
    for (const auto& key : sortingKeys_) {
      keyColumns_.push_back(data_->childAt(key.first).get());   // <-- throws
    }
  }
```

The UCX receive path yields `cudf_velox::CudfVector`, which derives from
`RowVector` but passes an **empty** children vector to its base while passing a
real row count:

```
velox/experimental/cudf/vector/CudfVector.cpp:119-125 and 142-148
  : RowVector(pool, std::move(type), BufferPtr(nullptr), size,
              std::vector<VectorPtr>(),   // no host children
              std::nullopt),
```

Its columns live in a `cudf::table`/`packed_table` on the device, reachable only
through `getTableView()`. So `size()` is non-zero (`atEnd_` stays false),
`children()` is empty (that loop no-ops), and `childAt(key.first)` hits
`VELOX_CHECK_LT(index, childrenSize_, "Trying to access non-existing child in
RowVector: {}")` (`velox/vector/ComplexVector.h:116-123`). The host k-way merger
therefore cannot consume UCX output at all.

Task D1 attempted to supply a `MergeSource` for UCX. That made the failure worse
rather than better: it removed `Merge.cpp`'s targeted
*"Merge exchange requires an InMemoryExchangeClient for transport: UCX"* error and
replaced it with the `childAt` internal error several frames deeper, mentioning
neither UCX nor merge. D1 is reverted here.

## Approach

Do not merge. Receive everything over UCX and sort once on the device, which
yields the same globally ordered result by a different algorithm. This is what
PR #17614 did (`MergeExchangeAdapter`, `OperatorAdapters.cpp:1195-1246`), and it
is the only correct option available: a true device-side `cudf::merge` would need
per-source attribution of incoming batches, and `PackedTableWithStream`
(`velox/experimental/ucx-exchange/UcxExchangeQueue.h:30-44`) carries only
`{packedTable, stream}` while the queue is a single FIFO across all remote
sources. Adding attribution would touch the wire protocol and the data plane and
belongs in its own plan.

The transformation is expressed as a cuDF **operator adapter**, not as a registry
factory.

### Why an adapter and not the exchange registry

`velox/experimental/cudf/exec/OperatorAdapters.h` is already in the tree and
`OperatorAdapters.cpp:1132-1151` registers 21 adapters (TableScan, FilterProject,
Aggregation, HashJoin, OrderBy, TopN, Limit, Window, …). `CompileState::compile()`
contains no per-operator cases; it consults
`OperatorAdapterRegistry::getInstance()`. The three adapters absent from the tree
are `Exchange`, `MergeExchange` and `PartitionedOutput` — exactly the three that
this PoC routes through its two registries instead.

Three reasons the adapter is the right home:

1. **`velox/exec` stays untouched.** The registry keeps its two-factory shape
   (`makeClient`, `makeOperator`) and `Merge.cpp` keeps its existing
   `InMemoryExchangeClient` downcast, so nothing in the core learns about
   merge-over-UCX.
2. **The link direction forbids the alternative.** `CudfOrderBy` lives in
   `velox_cudf_exec`, which links `velox_ucx_exchange`, not the reverse
   (`velox/experimental/ucx-exchange/CMakeLists.txt:60-64`). A registry factory
   returning `{UcxExchange, CudfOrderBy}` cannot be written in the UCX module
   where `registerUcxExchange()` lives. The cuDF module is the only one that sees
   both.
3. **One node to two operators is what the mechanism is for.**
   `DriverFactory::replaceOperators(driver, begin, end, replaceWith)`
   (`velox/exec/Driver.h:841-845`) takes an arbitrary-length vector and
   *"Sets operator ids to be consecutive after the replace"*.
   `ToCudf.cpp:246` (`operatorsOffset += replaceOp.size() - 1 + keepOperator`)
   already accounts for expansion.

**Constraint note.** The plan's Global Constraints say *"No `OperatorAdapters`"*.
That constraint targets #17614's use of adapters **instead of** the two registries
for `Exchange` and `PartitionedOutput`; those continue to resolve through
`ExchangeTransportRegistry` and `OutputTransportRegistry` (Tasks B2 and C2).
`OperatorAdapters` is not #17614's invention — it is the upstream mechanism every
GPU operator swap already uses. Ruled in scope by the user on 2026-08-12.

### Why the two-operator split needs no extra plumbing

`UcxExchange` was already written for this shape:

```
velox/experimental/ucx-exchange/UcxExchange.cpp:47-64
  closeExchangeClientOnClose_{ucxExchangeClient == nullptr},
  processSplits_{driverCtx->driverId == 0},
  ...
  } else {
    // UcxExchangeClient is nullptr when this UcxExchange is used to
    // implement a MergeExchange. Create a new UCX exchange client.
    exchangeClient_ = std::make_shared<UcxExchangeClient>(
        task->taskId(), task->destination(),
        1 // number of consumers, is always 1.
    );
  }
```

and `getSplits()` returns immediately when `!processSplits_`
(`UcxExchange.cpp:79-87`). Passing a null client therefore gives a private
single-consumer client and driver-0-only split processing — reproducing
`MergeExchange::addMergeSources`' own `driverId != 0` early return
(`Merge.cpp:795-799`). No shared client, no client map (#17614 needed one only
for its *plain* exchange adapter), and no driver pinning.

`UcxExchange` also pulls its own splits (`getSplitOrFuture`, `UcxExchange.cpp:97`)
and registers remote task ids itself (line 74), like `exec::Exchange`
(`Exchange.cpp:107`, `:92`). Splits are keyed by plan node id, and the substituted
operator keeps the node, so it receives the `MergeExchangeNode`'s splits.

## Components

### `MergeExchangeAdapter` (new, `velox/experimental/cudf/exec/OperatorAdapters.cpp`)

Registered as the 22nd adapter beside the existing list at lines 1132-1151.

| Member | Behaviour |
|---|---|
| `canHandle(op)` | `dynamic_cast<const exec::MergeExchange*>(op) != nullptr` |
| `canRunOnGPU(op, planNode, ctx)` | `CudfConfig::getInstance().exchange` **and** the node downcasts to `core::MergeExchangeNode` **and** `transportKind() == core::TransportKind::kUcx` |
| `acceptsGpuInput()` | `false` — it is a source |
| `producesGpuOutput()` | `true` |
| `createReplacements(...)` | `{UcxExchange(id, ctx, node, nullptr), CudfOrderBy(id, ctx, node)}` |
| `keepOperator()` | not overridden; the base default (`false`) is correct |

The node must be fetched with `CompileState::getPlanNode(op->planNodeId())` and
downcast, because `MergeExchange::transportKind_` is private with no accessor
(`velox/exec/Merge.h:406`).

`keepOperator()` needs no override, and no 3-arg overload is added to the base
class: `createReplacements` is only reached when `canRunOnGPU` is true
(`ToCudf.cpp:179-183`), so a non-UCX node is left alone by that gate alone.
#17614 added a 3-arg `keepOperator` it did not need.

### `CudfOrderBy` — second constructor (`velox/experimental/cudf/exec/CudfOrderBy.{h,cpp}`)

The existing constructor takes `const std::shared_ptr<const core::OrderByNode>&`
and reads only `outputType()`, `id()`, `sortingKeys()`, `sortingOrders()`
(`CudfOrderBy.cpp:27-61`). `core::MergeExchangeNode` exposes all four publicly.
`CudfOperatorBase`'s node parameter is
`std::optional<std::shared_ptr<const core::PlanNode>>`
(`CudfOperator.h:106-116`), so a `MergeExchangeNode` passes through unchanged.

Three deliberate differences from #17614's version
(`CudfOrderBy.cpp:65-102` in that PR):

| | #17614 | This design |
|---|---|---|
| Parameter | `const core::PlanNodePtr&`, downcast + `VELOX_CHECK_NOT_NULL` inside | `const std::shared_ptr<const core::MergeExchangeNode>&`, compiler-enforced |
| Key extraction | 19 lines duplicated verbatim in both constructors | one private helper called by both, taking the node's `sortingKeys` and `sortingOrders` and populating the existing `sortKeys_`, `columnOrder_` and `nullOrder_` members against `outputType_` |
| `orderByNode_` | left unset by the new constructor | deleted — it is assigned once and never read |

The typed parameter is free: the adapter already holds a
`shared_ptr<const MergeExchangeNode>` because it needed `transportKind()`.

## Data flow

1. `LocalPlanner` builds the pipeline with a stock `exec::MergeExchange`.
   `needsExchangeClient()` is nullptr for a `MergeExchangeNode`, so Task creates
   no exchange client.
2. `CudfDriverAdapter::compile()` runs per driver; the adapter matches and splices
   in `{UcxExchange, CudfOrderBy}` with renumbered operator ids.
3. **Driver 0**: `UcxExchange::getSplits()` pulls the node's
   `RemoteConnectorSplit`s, registers each producer task id on its private
   client, then marks no-more-splits. Batches arrive as packed cudf tables and
   become one `CudfVector` each. `CudfOrderBy::doAddInput` accumulates them; at
   no-more-input it sorts once on device and emits.
4. **Drivers != 0**: `processSplits_` is false, `getSplits()` returns immediately,
   no remote task ids are registered, nothing arrives, the operator finishes, and
   its `CudfOrderBy` emits nothing.
5. Exactly one driver emits and it sorts the complete input, so the output is
   globally ordered. `producesGpuOutput = true` lets the framework insert a
   `CudfToVelox` transition when the downstream operator is CPU-side.

## Failure modes

All non-UCX cases funnel through `canRunOnGPU` and leave the CPU operator intact:

- `transportKind != kUcx`, or `cudf.exchange` off → `canRunOnGPU` false →
  `createReplacements` never called → stock `MergeExchange`.
- cuDF not registered at all → no adapter runs → stock `MergeExchange` → for a
  `kUcx` node, `Merge.cpp`'s downcast fails fast with *"Merge exchange requires an
  InMemoryExchangeClient for transport: UCX"*. This is the pre-D1 behaviour,
  restored by the revert, and it is what the plan's fail-fast constraint requires.
- `UcxExchange` with a null client sets `closeExchangeClientOnClose_`, so it owns
  and closes what it created.
- `CudfOrderBy::doAddInput` already `VELOX_CHECK_NOT_NULL`s its `CudfVector` cast,
  so a non-GPU vector fails loudly.

## Testing

- **Adapter selection (this design).** Assert that a driver built for a
  `MergeExchangeNode` with `kUcx` has its `MergeExchange` replaced by
  `[UcxExchange, CudfOrderBy]`, and that the same plan with `kInMemory` is left
  alone. No data movement, so no multi-task UCX fixture is needed.
- **Globally ordered output (deferred to Task F1).** Requires two or more UCX
  producer tasks feeding one merge consumer, asserting the merged result is
  ordered. F1 builds that harness anyway; its brief gains this as an explicit
  requirement rather than a second harness being built here.
- **Regression.** The four `MultiFragmentTest` merge cases — `mergeExchange`,
  `abortMergeExchange`, `mergeExchangeWithSpill`, `mergeExchangeOverEmptySources`
  in `velox_exec_test_group1` — must stay green (24/24 at the time of writing),
  and the full `ucx_exchange_test` suite must return to its pre-D1 counts.

## Commit plan

Five commits, split by directory per the project's commit-separation rule. A
fifth docs-reconciliation commit was added beyond the four originally planned
here.

1. Revert `b40b01ab6` — `velox/experimental/ucx-exchange/` only, deleting
   `UcxMergeSource.{h,cpp}` and its `CMakeLists.txt` entry. This must come first:
   it assigns `entry->makeMergeSource`, which only exists because of `13aa7dbbd`.
   Landed as `6eb0ea70d`.
2. Revert `13aa7dbbd` — `velox/exec/` only. Landed as `250502e08`.
3. `velox/experimental/cudf/` — `CudfOrderBy`'s typed second constructor, the
   shared key-extraction helper, the dead `orderByNode_` member removed.
   Landed as `d2ee13f91`.
4. `velox/experimental/cudf/` — `MergeExchangeAdapter`, its registration, and the
   selection test. Landed as `d5e47c417` (amended once during review; the pre-amend
   hash is superseded and intentionally omitted here).
5. `docs/` — reconcile this design doc and the parent plan with the commits
   actually landed above.

Commits 3 and 4 stay separate so the `CudfOrderBy` refactor is reviewable alone.

## Out of scope

- True device-side k-way merge via `cudf::merge`. Needs per-source attribution
  through `PackedTableWithStream`, the exchange protocol and the queue, plus
  per-source buffering. Its own plan.
- Any change to `velox/exec/`, the exchange registry, or the output registry.
- Reintroducing `Exchange` or `PartitionedOutput` operator adapters; those stay on
  the two registries.
