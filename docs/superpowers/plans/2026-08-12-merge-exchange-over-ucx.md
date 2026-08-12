# MergeExchange over UCX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a `MergeExchangeNode` with `transportKind == core::TransportKind::kUcx` produce globally ordered output by replacing the `MergeExchange` operator with `UcxExchange` + `CudfOrderBy`, and revert the abandoned merge-source approach.

**Architecture:** A new cuDF operator adapter (`MergeExchangeAdapter`) registered in the existing `OperatorAdapterRegistry` swaps one operator for two via `DriverFactory::replaceOperators`. Nothing under `velox/exec/` changes: the exchange registry keeps its two-factory shape and `Merge.cpp` keeps its `InMemoryExchangeClient` downcast, which restores fail-fast for unsupported transports. `UcxExchange` already supports this shape — a null client makes it self-provision a private single-consumer client, and `processSplits_{driverId == 0}` gives the driver-0 serialization that global ordering needs.

**Tech Stack:** C++20, CMake, Velox exec, cuDF (libcudf 26.08), UCX 1.20.1 + ucxx 0.51, CUDA, GoogleTest. Build via `gpu_build_run.sh` in the cuDF-enabled image.

**Design doc:** `docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md` — read it before Task 3. It explains why merging is impossible (`CudfVector` has no host children), why `cudf::merge` is ruled out (no per-source attribution), and why an adapter rather than a registry factory (link direction + `velox/exec` must stay untouched).

## Global Constraints

- **Do not modify anything under `velox/exec/`** except by reverting `13aa7dbbd`. The exchange registry keeps its two-factory shape (`makeClient`, `makeOperator`); the output registry is untouched.
- **`Exchange` and `PartitionedOutput` stay on the two registries.** The plan's parent constraint "No `OperatorAdapters`" targets transport *selection*; an adapter is used here only for the one-node-to-two-operators shape transform. Do not add `Exchange` or `PartitionedOutput` adapters.
- **The default HTTP/in-memory path must be behaviourally unchanged.** A `kInMemory` `MergeExchangeNode` must still get a stock `exec::MergeExchange`.
- **Transport-kind key** is `core::TransportKind::kUcx` (`"UCX"`); default `kInMemory` (`"in-memory"`). Reuse the existing constants — do not introduce new strings.
- **`UcxPartitionedOutput` keeps its 4-arg `eagerFlush` constructor**, and `UcxOutputQueueManager` keeps the signatures Task C1 settled against `velox/exec/OutputBufferManager.h`.
- **Naming/comments:** follow `.claude/CLAUDE.md` — `///` only for public API in headers, `//` elsewhere; camelCase members with trailing `_`; no `*Utils`/`*Helpers` names; runtime values at the end of error messages.
- **Commit separation:** never one commit mixing (a) `velox/experimental/ucx-exchange/`, (b) `velox/experimental/cudf/`, (c) files outside both.
- **Commits** are plain conventional commits with no bracketed prefix, made with `-s` (DCO), GPG-signed — verify `git log -1 --format='%G?'` prints `G`. Never `git add` `CLAUDE.md`, `CLAUDE.local.md`, or `.claude/` contents.
- **Formatting** goes through `~/.venvs/pre-commit/bin/pre-commit` (Python 3.12). Never `make format`, bare `clang-format`, or `git clang-format`. `~/.local/bin/pre-commit` runs under Python 3.9 and aborts before any hook executes.
- **Build/run** via `gpu_build_run.sh`; build dir is always `_build/release`, never `_build/debug`.

---

## File Structure

**Reverted (Task 1, `velox/experimental/ucx-exchange/`):**
- Delete: `UcxMergeSource.h`, `UcxMergeSource.cpp`
- Modify: `UcxExchangeRegistration.cpp` — drop the `makeMergeSource` assignment
- Modify: `CMakeLists.txt` — drop `UcxMergeSource.cpp` from `UCX_EXCHANGE_SOURCES`
- Modify: `tests/UcxExchangeTest.cpp` — drop the `mergeSourceOverUcx` case

**Reverted (Task 2, `velox/exec/`):**
- Modify: `ExchangeFactory.h` — drop the `MergeSourceFactory` typedef
- Modify: `ExchangeTransportRegistry.h` — drop the `makeMergeSource` field, restore the doc comments
- Modify: `InMemoryExchangeClient.cpp` — drop the third factory and the `MergeSource.h` include
- Modify: `Merge.cpp` — restore the `InMemoryExchangeClient` downcast in `MergeExchange::addMergeSources`
- Modify: `tests/ExchangeTransportTest.cpp` — drop the third invocation counter

**Modified (Task 3, `velox/experimental/cudf/exec/`):**
- `CudfOrderBy.h` — second constructor taking `MergeExchangeNode`; private helper; delete `orderByNode_`
- `CudfOrderBy.cpp` — helper implementation; both constructors delegate to it

**Modified (Task 4, `velox/experimental/cudf/`):**
- `exec/OperatorAdapters.cpp` — `MergeExchangeAdapter` + its registration
- `tests/AdapterOperatorTest.cpp` — the selection test

**Modified (Task 5, `docs/`):**
- `docs/superpowers/plans/2026-08-10-ucx-exchange-integration.md` — mark Task D1 superseded
- `docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md` — correct the revert ordering

---

## Task 1: Revert the ucx-exchange half of Task D1

Revert `b40b01ab6` **first**. It assigns `entry->makeMergeSource`, a field that only
exists because of `13aa7dbbd`; reverting `13aa7dbbd` first would leave the tree
uncompilable between the two commits. (The design doc lists the opposite order —
Task 5 corrects it.)

**Files:**
- Revert: commit `b40b01ab690caeead7da402811393e1c40ddf57d` (5 files, all under `velox/experimental/ucx-exchange/`)

**Interfaces:**
- Produces: a tree where `UcxMergeSource` no longer exists and `registerUcxExchange()` sets only `makeClient` and `makeOperator` on the `kUcx` receive entry, plus `manager` and `makeOutputOperator` on the output entry.

- [ ] **Step 1: Revert the commit**

```bash
git revert --no-edit --signoff b40b01ab690caeead7da402811393e1c40ddf57d
```

- [ ] **Step 2: Confirm what the revert touched**

```bash
git show --stat --format="" HEAD
```

Expected: exactly 5 files, all under `velox/experimental/ucx-exchange/`, with
`UcxMergeSource.h` and `UcxMergeSource.cpp` deleted. If any file outside that
directory appears, STOP and report — the commit separation rule is violated.

- [ ] **Step 3: Verify the signature and sign-off**

```bash
git log -1 --format='%G? %s'
git log -1 --format='%B' | grep -c 'Signed-off-by'
```

Expected: `G` on the first line, `1` from the grep. `git revert --signoff` preserves
GPG signing from your git config; if `%G?` is not `G`, STOP and report rather than
amending with `-c commit.gpgsign=false`.

- [ ] **Step 4: Build and run the UCX suite**

```bash
VELOX_DIR=/gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side \
TARGETS="ucx_exchange_test" \
RUN_CMDS="velox/experimental/ucx-exchange/tests/ucx_exchange_test|*" \
NAME=merge_t1 LOG=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/merge_t1.log \
bash /gpfs/zc2/u/dnb/velox_workspaces/tz_build/gpu_build_run.sh
```

Note this task's build will still fail to link or compile if `13aa7dbbd` is still
present and something references the removed source — it should not, because
`b40b01ab6` was self-contained on the UCX side. If the build fails with an error
about `makeMergeSource`, that means the revert was incomplete; report it.

Poll and check (never `cat` or `tail` the log — see LOG HYGIENE below):

```bash
L=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/merge_t1.log
for i in $(seq 1 120); do
  grep -Eq 'ALL_TESTS_PASSED|SOME_TESTS_FAILED|BUILD_FAILED|CONFIGURE_FAILED' $L && break
  sleep 20
done
grep -Eo 'BUILD_OK|BUILD_FAILED|CONFIGURE_FAILED|ALL_TESTS_PASSED|SOME_TESTS_FAILED' $L | sort -u
echo "RUN=$(grep -cE '^\[ *RUN' $L) OK=$(grep -cE '^\[ *OK' $L) FAILED=$(grep -cE '^\[  FAILED' $L)"
```

Expected: `BUILD_OK` + `ALL_TESTS_PASSED`, and the counts back to the pre-D1
baseline of **194 ran / 94 OK / 0 failed** (D1 had raised them to 208/95/0 by adding
14 param instances of `mergeSourceOverUcx`).

---

## Task 2: Revert the velox/exec half of Task D1

**Files:**
- Revert: commit `13aa7dbbd5c1c3f863e2fcd18919d42b6a189986` (5 files, all under `velox/exec/`)

**Interfaces:**
- Produces: `exec::ExchangeTransportEntry` back to its two-field aggregate `{makeClient, makeOperator}`; `MergeExchange::addMergeSources` back to downcasting the client to `InMemoryExchangeClient` and failing with *"Merge exchange requires an InMemoryExchangeClient for transport: {}"*. This is what restores fail-fast for a `kUcx` merge node when cuDF is not registered.

- [ ] **Step 1: Revert the commit**

```bash
git revert --no-edit --signoff 13aa7dbbd5c1c3f863e2fcd18919d42b6a189986
```

- [ ] **Step 2: Confirm what the revert touched**

```bash
git show --stat --format="" HEAD
```

Expected: exactly 5 files, all under `velox/exec/` (including
`velox/exec/tests/ExchangeTransportTest.cpp`). Nothing under either experimental
module.

- [ ] **Step 3: Verify no trace of the reverted API remains**

```bash
grep -rn "makeMergeSource\|MergeSourceFactory\|UcxMergeSource" velox/ || echo "CLEAN"
```

Expected: `CLEAN`. Any hit means one of the two reverts was incomplete.

- [ ] **Step 4: Verify the signature and sign-off**

```bash
git log -1 --format='%G? %s'
git log -1 --format='%B' | grep -c 'Signed-off-by'
```

Expected: `G`, then `1`.

- [ ] **Step 5: Build and run both affected exec suites**

```bash
VELOX_DIR=/gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side \
TARGETS="velox_exec_test_group1 velox_exec_infra_test" \
RUN_CMDS="velox/exec/tests/velox_exec_test_group1|*mergeExchange*:*abortMergeExchange*:*mergeExchangeOverEmptySources*
velox/exec/tests/velox_exec_infra_test|*ExchangeTransportTest*" \
NAME=merge_t2 LOG=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/merge_t2.log \
bash /gpfs/zc2/u/dnb/velox_workspaces/tz_build/gpu_build_run.sh
```

Two `RUN_CMDS` lines are safe here: neither binary binds the UCX fixture's port
21346. (`*mergeExchange*` also matches `mergeExchangeWithSpill`.)

- [ ] **Step 6: Check the results**

```bash
L=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/merge_t2.log
for i in $(seq 1 120); do
  grep -Eq 'ALL_TESTS_PASSED|SOME_TESTS_FAILED|BUILD_FAILED|CONFIGURE_FAILED' $L && break
  sleep 20
done
grep -Eo 'BUILD_OK|BUILD_FAILED|ALL_TESTS_PASSED|SOME_TESTS_FAILED' $L | sort -u
grep -E '^RC ' $L
echo "RUN=$(grep -cE '^\[ *RUN' $L) FAILED=$(grep -cE '^\[  FAILED' $L)"
```

Expected: `BUILD_OK` + `ALL_TESTS_PASSED`, `RC ... = 0` for both binaries,
**24 merge cases** in group1 and **5** `ExchangeTransportTest` cases, 0 failed.
D1's report recorded 24/24 and 5/5, so these are exact.

---

## Task 3: Give `CudfOrderBy` a `MergeExchangeNode` constructor

**Files:**
- Modify: `velox/experimental/cudf/exec/CudfOrderBy.h:29-62`
- Modify: `velox/experimental/cudf/exec/CudfOrderBy.cpp:27-61`
- Test: existing `velox/experimental/cudf/tests/OrderByTest.cpp` (no new test — this
  task adds no behaviour, and Task 4's test is what exercises the new constructor)

**Interfaces:**
- Consumes: `core::MergeExchangeNode`'s public `outputType()`, `id()`, `sortingKeys()`, `sortingOrders()`; `CudfOperatorBase`'s node parameter `std::optional<std::shared_ptr<const core::PlanNode>>` (`CudfOperator.h:106-116`).
- Produces: `CudfOrderBy(int32_t operatorId, exec::DriverCtx* driverCtx, const std::shared_ptr<const core::MergeExchangeNode>& mergeExchangeNode)` — used by Task 4's adapter.

- [ ] **Step 1: Add the declarations**

In `CudfOrderBy.h`, after the existing constructor (line 31-34):

```cpp
  /// Constructs from a MergeExchangeNode, for the case where a kUcx
  /// MergeExchange is replaced by UcxExchange + CudfOrderBy. The node supplies
  /// the same sorting keys and orders an OrderByNode would.
  CudfOrderBy(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::MergeExchangeNode>& mergeExchangeNode);
```

and in the `private:` section, add the helper declaration and **delete** the
`orderByNode_` member (line 56 — it is assigned once and never read):

```cpp
  // Translates 'sortingKeys' and 'sortingOrders' into the cuDF sort-key
  // channels and orders used by doNoMoreInput(), resolved against outputType_.
  void initializeSortKeys(
      const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
      const std::vector<core::SortOrder>& sortingOrders);
```

- [ ] **Step 2: Extract the helper in the .cpp**

Replace the body of the existing constructor (`CudfOrderBy.cpp:42-60`) with a call
to the helper, and add the helper definition:

```cpp
void CudfOrderBy::initializeSortKeys(
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders) {
  sortKeys_.reserve(sortingKeys.size());
  columnOrder_.reserve(sortingKeys.size());
  nullOrder_.reserve(sortingKeys.size());
  for (size_t i = 0; i < sortingKeys.size(); ++i) {
    const auto channel =
        exec::exprToChannel(sortingKeys[i].get(), outputType_);
    VELOX_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    sortKeys_.push_back(channel);
    const auto& sortingOrder = sortingOrders[i];
    columnOrder_.push_back(
        sortingOrder.isAscending() ? cudf::order::ASCENDING
                                   : cudf::order::DESCENDING);
    nullOrder_.push_back(
        (sortingOrder.isNullsFirst() ^ !sortingOrder.isAscending())
            ? cudf::null_order::BEFORE
            : cudf::null_order::AFTER);
  }
}
```

The existing constructor's initializer list keeps everything except
`orderByNode_(orderByNode)`, and its body becomes:

```cpp
  initializeSortKeys(orderByNode->sortingKeys(), orderByNode->sortingOrders());
```

- [ ] **Step 3: Add the second constructor**

```cpp
CudfOrderBy::CudfOrderBy(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::MergeExchangeNode>& mergeExchangeNode)
    : CudfOperatorBase(
          operatorId,
          driverCtx,
          mergeExchangeNode->outputType(),
          mergeExchangeNode->id(),
          "CudfOrderBy",
          nvtx3::rgb{64, 224, 208}, // Turquoise
          NvtxMethodFlag::kAll,
          std::nullopt,
          mergeExchangeNode) {
  initializeSortKeys(
      mergeExchangeNode->sortingKeys(), mergeExchangeNode->sortingOrders());
}
```

- [ ] **Step 4: Build and run the existing OrderBy tests**

```bash
VELOX_DIR=/gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side \
TARGETS="velox_cudf_order_by_test" \
RUN_CMDS="velox/experimental/cudf/tests/velox_cudf_order_by_test|*" \
NAME=merge_t3 LOG=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/merge_t3.log \
bash /gpfs/zc2/u/dnb/velox_workspaces/tz_build/gpu_build_run.sh
```

Expected: `BUILD_OK` + `ALL_TESTS_PASSED`, 0 failed. This is a pure refactor of the
existing constructor plus a new unused one, so every existing case must still pass
with identical counts. Record the count — Task 4 must not change it.

- [ ] **Step 5: Format and commit**

```bash
~/.venvs/pre-commit/bin/pre-commit run --files \
  velox/experimental/cudf/exec/CudfOrderBy.h \
  velox/experimental/cudf/exec/CudfOrderBy.cpp
git add velox/experimental/cudf/exec/CudfOrderBy.h velox/experimental/cudf/exec/CudfOrderBy.cpp
git commit -s -m "refactor(cudf): Let CudfOrderBy take sort keys from a MergeExchangeNode"
```

---

## Task 4: Add and register `MergeExchangeAdapter`

**Files:**
- Modify: `velox/experimental/cudf/exec/OperatorAdapters.cpp` — new class beside `OrderByAdapter` (which ends at line 605), registration after line 1139
- Modify: `velox/experimental/cudf/tests/AdapterOperatorTest.cpp` — the selection test
- Test target: `velox_cudf_adapter_operator_test`

**Interfaces:**
- Consumes: `CudfOrderBy`'s `MergeExchangeNode` constructor from Task 3; `ucx_exchange::UcxExchange(int32_t, exec::DriverCtx*, const std::shared_ptr<const core::PlanNode>&, std::shared_ptr<UcxExchangeClient>, std::string_view operatorType = …)`; `OperatorAdapter`'s five pure virtuals; `OperatorAdapterRegistry::getInstance().getAdapters()` and `OperatorAdapter::name()`.
- Produces: an adapter named `"MergeExchange"` in the registry, which for a `kUcx` node replaces `exec::MergeExchange` with `{UcxExchange, CudfOrderBy}`.

- [ ] **Step 1: Write the failing test**

In `velox/experimental/cudf/tests/AdapterOperatorTest.cpp`, add after the existing
`adapterStatsMergedIntoPlanNode` case:

```cpp
namespace {
// Returns the registered adapter with 'name', or nullptr.
const cudf_velox::OperatorAdapter* findAdapterByName(const std::string& name) {
  for (const auto& adapter :
       cudf_velox::OperatorAdapterRegistry::getInstance().getAdapters()) {
    if (adapter->name() == name) {
      return adapter.get();
    }
  }
  return nullptr;
}
} // namespace

TEST_F(AdapterOperatorTest, mergeExchangeAdapterSelectsUcxOnly) {
  auto rowType = ROW({"c0"}, {BIGINT()});

  auto ucxNode = PlanBuilder()
                     .mergeExchange(
                         rowType,
                         {"c0"},
                         std::string(VectorSerde::kindName(
                             VectorSerde::Kind::kCompactRow)),
                         std::string{core::TransportKind::kUcx})
                     .planNode();
  auto inMemoryNode = PlanBuilder()
                          .mergeExchange(
                              rowType,
                              {"c0"},
                              std::string(VectorSerde::kindName(
                                  VectorSerde::Kind::kCompactRow)),
                              std::string{core::TransportKind::kInMemory})
                          .planNode();

  const auto* adapter = findAdapterByName("MergeExchange");
  ASSERT_NE(adapter, nullptr) << "MergeExchangeAdapter is not registered";

  // canRunOnGPU ignores its operator and DriverCtx arguments, so this exercises
  // the transport gate without building a Driver or moving any data.
  cudf_velox::CudfConfig::getInstance().exchange = true;
  EXPECT_TRUE(adapter->canRunOnGPU(nullptr, ucxNode, nullptr));
  EXPECT_FALSE(adapter->canRunOnGPU(nullptr, inMemoryNode, nullptr));

  // With cuDF exchange off, even a kUcx node keeps the CPU MergeExchange.
  cudf_velox::CudfConfig::getInstance().exchange = false;
  EXPECT_FALSE(adapter->canRunOnGPU(nullptr, ucxNode, nullptr));
}
```

Add these includes to the file:

```cpp
#include "velox/experimental/cudf/exec/OperatorAdapters.h"
#include "velox/vector/VectorStream.h"
```

`VectorSerde::kindName` is declared in `velox/vector/VectorStream.h:215`. No serde
needs registering: `PlanBuilder::mergeExchange` (`PlanBuilder.cpp:560-578`) only
stores the kind string on the node, and this test never constructs an operator from
it.

- [ ] **Step 2: Build and verify it fails**

```bash
VELOX_DIR=/gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side \
TARGETS="velox_cudf_adapter_operator_test" \
RUN_CMDS="velox/experimental/cudf/tests/velox_cudf_adapter_operator_test|*mergeExchangeAdapterSelectsUcxOnly*" \
NAME=merge_t4_red LOG=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/merge_t4_red.log \
bash /gpfs/zc2/u/dnb/velox_workspaces/tz_build/gpu_build_run.sh
```

Expected: `SOME_TESTS_FAILED` with the `ASSERT_NE(adapter, nullptr)` failing on
*"MergeExchangeAdapter is not registered"*. Confirm `RUN=1` — a filter that matches
nothing still exits 0 and would look green.

- [ ] **Step 3: Add the adapter**

In `OperatorAdapters.cpp`, after `OrderByAdapter` (ends line 605):

```cpp
/// MergeExchangeAdapter - Replaces MergeExchange with UcxExchange + CudfOrderBy
/// for the UCX transport. MergeExchange merges pre-sorted streams by comparing
/// rows on the host, which cannot work on device-resident CudfVectors, so the
/// UCX path receives everything and sorts once on the GPU instead.
class MergeExchangeAdapter : public OperatorAdapter {
 public:
  MergeExchangeAdapter() : OperatorAdapter("MergeExchange") {}

  bool canHandle(const exec::Operator* op) const override {
    return dynamic_cast<const exec::MergeExchange*>(op) != nullptr;
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* /*ctx*/) const override {
    if (!CudfConfig::getInstance().exchange) {
      return false;
    }
    auto mergeExchangeNode =
        std::dynamic_pointer_cast<const core::MergeExchangeNode>(planNode);
    return mergeExchangeNode != nullptr &&
        mergeExchangeNode->transportKind() == core::TransportKind::kUcx;
  }

  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& planNode,
      exec::DriverCtx* ctx,
      int32_t operatorId) const override {
    auto mergeExchangeNode =
        std::dynamic_pointer_cast<const core::MergeExchangeNode>(planNode);
    VELOX_CHECK_NOT_NULL(
        mergeExchangeNode,
        "MergeExchangeAdapter requires a MergeExchangeNode for plan node: {}",
        planNode->id());

    std::vector<std::unique_ptr<exec::Operator>> result;
    // A null client makes UcxExchange create its own single-consumer client and
    // process splits on driver 0 only, which serializes the receive the same way
    // MergeExchange::addMergeSources does.
    result.push_back(
        std::make_unique<ucx_exchange::UcxExchange>(
            operatorId, ctx, mergeExchangeNode, nullptr));
    result.push_back(
        std::make_unique<CudfOrderBy>(operatorId, ctx, mergeExchangeNode));
    return result;
  }
};
```

`keepOperator()` is deliberately not overridden: `createReplacements` is only
reached when `canRunOnGPU` is true (`ToCudf.cpp:179-183`), so the `canRunOnGPU`
gate alone leaves a non-UCX node with its stock operator.

Add the includes:

```cpp
#include "velox/exec/Merge.h"
#include "velox/experimental/ucx-exchange/UcxExchange.h"
```

- [ ] **Step 4: Register it**

In `registerAllOperatorAdapters()`, after the `OrderByAdapter` line (1139):

```cpp
  registry.registerAdapter(std::make_unique<MergeExchangeAdapter>());
```

- [ ] **Step 5: Build and verify the test passes**

Re-run the Step 2 command with `NAME=merge_t4 LOG=…/merge_t4.log`.
Expected: `BUILD_OK` + `ALL_TESTS_PASSED`, `RUN=1`, 0 failed.

- [ ] **Step 6: Run the full adapter and OrderBy suites for regressions**

```bash
VELOX_DIR=/gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side \
TARGETS="velox_cudf_adapter_operator_test velox_cudf_order_by_test" \
RUN_CMDS="velox/experimental/cudf/tests/velox_cudf_adapter_operator_test|*
velox/experimental/cudf/tests/velox_cudf_order_by_test|*" \
NAME=merge_t4_full LOG=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/merge_t4_full.log \
bash /gpfs/zc2/u/dnb/velox_workspaces/tz_build/gpu_build_run.sh
```

Expected: `ALL_TESTS_PASSED`, `RC ... = 0` for both, and the OrderBy count identical
to what Task 3 Step 4 recorded.

- [ ] **Step 7: Format and commit**

```bash
~/.venvs/pre-commit/bin/pre-commit run --files \
  velox/experimental/cudf/exec/OperatorAdapters.cpp \
  velox/experimental/cudf/tests/AdapterOperatorTest.cpp
git add velox/experimental/cudf/exec/OperatorAdapters.cpp \
        velox/experimental/cudf/tests/AdapterOperatorTest.cpp
git commit -s -m "feat(cudf): Replace MergeExchange with UcxExchange and CudfOrderBy for UCX"
```

---

## Task 5: Reconcile the plan and design documents

This is a fifth commit beyond the design doc's four; that plan covered code only.
Everything here is under `docs/`, which is category (c) — outside both experimental
modules — so it must not be combined with Tasks 3 or 4.

**Files:**
- Modify: `docs/superpowers/plans/2026-08-10-ucx-exchange-integration.md` (Task D1 section)
- Modify: `docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md` (commit plan ordering)

**Interfaces:**
- Consumes: nothing. Produces: documents that agree with the tree, so the final whole-branch review does not flag D1's reverted commits as an unexplained regression.

- [ ] **Step 1: Mark Task D1 superseded in the parent plan**

Replace the parent plan's `### Task D1: UCX merge-source adapter` heading and its
"Files"/"Interfaces" blocks with a pointer, keeping the original steps below a
clear marker so the history stays readable:

```markdown
### Task D1: UCX merge-source adapter — SUPERSEDED

**Superseded on 2026-08-12** by
`docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md` and
`docs/superpowers/plans/2026-08-12-merge-exchange-over-ucx.md`.

The merge-source approach below was implemented (`13aa7dbbd`, `b40b01ab6`) and then
reverted. It cannot work: `MergeExchange` compares rows host-side, and the UCX path
yields `CudfVector`, which keeps its columns on the device and passes an empty
children vector to its `RowVector` base — so `SourceStream::fetchMoreData` throws
"Trying to access non-existing child in RowVector". The replacement receives over
UCX and sorts on the GPU via a cuDF operator adapter. Do not implement the steps
below.
```

- [ ] **Step 2: Add the ordered-output requirement to Task F1**

In the parent plan's Task F1 section, add to its step list:

```markdown
- [ ] **Assert merge-over-UCX ordering.** Build a plan with a `MergeExchangeNode`
  whose `transportKind` is `kUcx`, fed by two or more UCX producer tasks, and
  assert the result is globally ordered. This is the end-to-end proof for
  `docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md`, whose own
  plan deliberately covers only adapter selection.
```

- [ ] **Step 3: Correct the revert ordering in the design doc**

In the design doc's "Commit plan" section, swap items 1 and 2 so the
`velox/experimental/ucx-exchange/` revert comes first, and add the reason:

```markdown
1. Revert `b40b01ab6` — `velox/experimental/ucx-exchange/` only, deleting
   `UcxMergeSource.{h,cpp}` and its `CMakeLists.txt` entry. This must come first:
   it assigns `entry->makeMergeSource`, which only exists because of `13aa7dbbd`.
2. Revert `13aa7dbbd` — `velox/exec/` only.
```

- [ ] **Step 4: Commit**

```bash
~/.venvs/pre-commit/bin/pre-commit run --files \
  docs/superpowers/plans/2026-08-10-ucx-exchange-integration.md \
  docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md
git add docs/superpowers/plans/2026-08-10-ucx-exchange-integration.md \
        docs/superpowers/specs/2026-08-12-merge-exchange-over-ucx-design.md
git commit -s -m "docs: Mark Task D1 superseded by the merge-over-UCX design"
```

---

## LOG HYGIENE (mandatory)

Three implementers on the parent plan were killed by context overflow from build
logs. Never `cat` or `tail -n 150` a log.

- Completion: `grep -Eo 'BUILD_OK|BUILD_FAILED|ALL_TESTS_PASSED|SOME_TESTS_FAILED' "$L" | tail -1`
- Failures: `grep -n -m 30 -E 'error:|Error [0-9]|undefined reference' "$L"`
- Results: `grep -E '^\[  (FAILED|PASSED|OK)' "$L" | tail -20`

Keep every command's output under ~40 lines. The same applies to source: read the
line ranges cited in each task, not whole files. `tests/UcxExchangeTest.cpp` is
~1600 lines and no task here needs to open it.

## gtest filter form

A filter with no trailing wildcard matches nothing, and gtest then prints
`Running 0 tests from 0 test suites` and **still exits 0**, so the wrapper stamps
`ALL_TESTS_PASSED` and a broken run looks green. Always confirm `Running N tests`
with N > 0, or count `^\[ *RUN`.

`AdapterOperatorTest` and `OrderByTest` are plain `testing::Test` fixtures, so
`TEST_F` and two-part names are correct. `MultiFragmentTest` is parameterized, so
its cases need `*caseName*`.

## Use one `RUN_CMDS` line per invocation for `ucx_exchange_test`

Its fixture binds port 21346 with no address reuse, so a second process in the same
list dies with `bind(0.0.0.0:21346) failed: Address already in use` →
`ucxx::BusyError` → RC 134. Tracked separately; not to be fixed or worked around
here. Other binaries may share one invocation.
