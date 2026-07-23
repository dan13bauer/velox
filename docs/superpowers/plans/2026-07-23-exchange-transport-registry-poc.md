# Exchange Transport Registry (Receive-Side PoC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a branch off merged `main` (which now contains PR #16980), build the receive-side mirror of the output-transport registry — `transportKind` on `ExchangeNode`, an `ExchangeTransportRegistry`, per-transport resolution in Task/LocalPlanner — and wire the UCX transport through it so `ucx_exchange_test` passes end-to-end.

**Architecture:** Faithful mirror of #16980's output side. An abstract `ExchangeClient` interface (control plane only) with a concrete `InMemoryExchangeClient` (today's `ExchangeClient`, renamed) and `UcxExchangeClient`. A query-scoped `ExchangeTransportRegistry` maps a transport id to a plain-aggregate entry `{ ExchangeClientFactory, ExchangeFactory }`. Task resolves the entry by the node's `transportKind` and builds the per-node client via the client factory (still Task-owned, shared across drivers); LocalPlanner builds the operator via the operator factory. The built-in in-memory transport is a seeded registry entry; an unregistered named transport fails fast.

**Tech Stack:** C++20, Velox `exec`, `core::TransportKind` (in `velox/core/PlanNode.h` as of #16980), `ScopedRegistry`, gtest/gmock, cuDF/UCX experimental adapter, docker `prestobuild/dependency:cio-new` (gcc-14, `--gpus all`).

## Global Constraints

- Base branch: `poc-exchange-transport-integration` off merged `main` = `dan13bauer/main` (`49e8aac49`), which contains PR #16980 as commit `b6040fcfc`. Upstream changed — 16980 is now merged and PR #18110 (the rename) rebases cleanly on top — so the branch must be **rebuilt** onto this base (Phase 0). Commit 1 (rename) is upstream PR #18110, applied cleanly; the design + plan doc commits are re-landed as the first commits, then all task commits go on top.
- `TransportKind` constants live in `velox/core/PlanNode.h`: `kInMemory{"in-memory"}` (default), `kHttp` = deprecated alias of `kInMemory`, `kUcx{"UCX"}`. Reuse them; never add a parallel enum.
- Naming: PascalCase types/files; camelCase functions/locals; camelCase_ private members; kPascalCase constants; snake_case namespaces/targets.
- Comments: `///` only for public API in headers; document every public class/method/member; explain *why*, not *what*; no comment that restates code. `//` everywhere else.
- `VELOX_CHECK_*` (two-arg forms) for internal errors, `VELOX_USER_CHECK_*` for user errors; runtime values at the END of messages.
- Non-trivial method bodies in `.cpp`, not headers. No `friend`/`FRIEND_TEST`. gtest container matchers for collections.
- TDD: write the failing test first, confirm it fails, implement minimally, confirm it passes, commit.
- NEVER commit the presto-native-execution/velox submodule pointer.
- Format via the repo pre-commit hook (pinned clang-format v21 / gersemi), NOT host `make format`.
- Build/verify in `prestobuild/dependency:cio-new` only (gcc/g++ 14; override the image's baked gcc-12 `CC`/`CXX`). Build and run in the SAME image.
- **Single build for everything.** There is ONE cuDF-enabled build (`make cmake-cudf VELOX_BUILD_TESTING=ON`, via `gpu_build_run.sh`) that serves both CPU and GPU test runs. Do NOT create a separate non-cuDF/CPU build — `ucx-exchange` is gated behind cuDF at build time, and a divergent build would not compile it. GPU vs CPU is a **runtime** distinction, not a build distinction: the ucx-exchange code paths are exercised only when cuDF is registered (`cudf_velox::registerCudf()`, config `cudf.enabled=true`) and the container has `--gpus`. So-called "CPU tests" below run from this same binary WITHOUT registering cuDF (cuDF stays unregistered → CPU path), needing no GPU. The GPU gate (`ucx_exchange_test`) runs the same binary WITH cuDF registered and `--gpus all`.
- Registry entry is a plain aggregate (no ctor guarding a null-factory case). Client stays Task-owned; registry never keyed by task.
- The in-memory transport is a real seeded entry, restored by `unregisterAll()`; a named transport with no entry FAILS FAST.

---

## File Structure

**Created:**
- `velox/exec/ExchangeClient.h` — NEW abstract control-plane interface (commit 3). Reuses the filename freed by the rename.
- `velox/exec/ExchangeFactory.h` — `ExchangeFactory` + `ExchangeClientFactory` + `ExchangeClientContext` typedefs (commit 3).
- `velox/exec/ExchangeTransportRegistry.h` / `.cpp` — registry (commit 3).
- `velox/exec/tests/ExchangeTransportRegistryTest.cpp` — standalone-binary registry test (commit 3).
- `velox/exec/tests/ExchangeTransportTest.cpp` — grouped-suite resolution test (commit 3).

**Renamed (commit 1):**
- `velox/exec/ExchangeClient.{h,cpp}` → `velox/exec/InMemoryExchangeClient.{h,cpp}` (class `ExchangeClient` → `InMemoryExchangeClient`).
- `velox/exec/tests/ExchangeClientTest.cpp` → `InMemoryExchangeClientTest.cpp`.

**Modified:**
- `velox/core/PlanNode.{h,cpp}` — `transportKind` on `ExchangeNode`/`MergeExchangeNode` (commit 2); delete `requiresExchangeClient()` overrides (commit 3).
- `velox/exec/Task.cpp` / `Task.h` — resolve client via registry; store abstract `ExchangeClient` (commit 3); merge client via registry (commit 4).
- `velox/exec/LocalPlanner.cpp` — build Exchange via operator factory + delete fallback branch (commit 3); MergeExchange (commit 4).
- `velox/exec/Operator.{h,cpp}` — delete `toOperator(...,exchangeClient)` / `fromPlanNode(...,exchangeClient)` (commit 3).
- `velox/exec/Driver.h` — convert `needsExchangeClient()` to `dynamic_pointer_cast<ExchangeNode>` (commit 3).
- `velox/exec/MergeSource.cpp` — merge client via registry (commit 4).
- `velox/experimental/ucx-exchange/UcxExchangeClient.{h,cpp}` — implement `exec::ExchangeClient` (commit 5).
- `velox/experimental/ucx-exchange/*` registration TU — register the UCX entry (commit 5).
- `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp` + `SinkDriverMock`/`SourceDriverMock` — resolve via registry (commit 5).
- `velox/exec/CMakeLists.txt`, `velox/exec/tests/CMakeLists.txt` — new sources/targets.

---

## Phase 0 — Rebuild the branch onto merged main

Upstream changed: PR #16980 is now merged into `main` (commit `b6040fcfc`, "feat: Select output transport per PartitionedOutput node via a pluggable registry"), and the receive-side rename PR #18110 rebases cleanly on top of it. The current branch sits on the **stale fetched** pre-merge 16980 (`356710339`) with a local rename cherry-pick (`ee3631849`); it must be rebuilt onto merged `main` and use upstream #18110 for the rename.

New base: `dan13bauer/main` (`49e8aac49`). Rename source: upstream PR #18110 = branch `dan13bauer/rename-exchange-client-to-default-exchange-client` (`dd00e11d5`); its net diff vs merged main is exactly the `ExchangeClient`→`InMemoryExchangeClient` rename (15 files) and applies cleanly.

- [ ] **Step 1: Fetch and back up the stale branch**

```bash
cd /gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side
git fetch dan13bauer
git branch -f backup/poc-stale-356 poc-exchange-transport-integration
```

- [ ] **Step 2: Reset the branch onto merged main**

```bash
git checkout poc-exchange-transport-integration
git reset --hard dan13bauer/main
```

- [ ] **Step 3: Re-land the design + plan doc commits with their updated content**

Restore this plan and `docs/designs/exchange-transport-registry-poc.md` (both revised for the merged-16980 base) onto the new base and commit them as the branch's first commit(s), e.g.:

```bash
git checkout backup/poc-stale-356 -- docs/designs/exchange-transport-registry-poc.md docs/superpowers/plans/2026-07-23-exchange-transport-registry-poc.md
git add docs && git commit -m "docs(exec): Design and implementation plan for exchange-transport registry PoC"
```

- [ ] **Step 4: Confirm base**

Run: `git cat-file -e dan13bauer/main:velox/exec/OutputTransportRegistry.h && echo OK16980`
Expected: `OK16980` (merged 16980 present). Also `git cat-file -e dan13bauer/main:velox/exec/InMemoryExchangeClient.h` FAILS (rename not yet in main — it is Commit 1 here).

---

## Phase 1 — Commit 1: Rename ExchangeClient → InMemoryExchangeClient

Reuses upstream PR #18110. On the merged-16980 base, its net diff (vs `dan13bauer/main` `49e8aac49`) is exactly the rename (15 files: `ExchangeClient.{h,cpp}`→`InMemoryExchangeClient.{h,cpp}`, `ExchangeClientTest.cpp`→`InMemoryExchangeClientTest.cpp`, plus edits to `Exchange.{h,cpp}`, `Task.{h,cpp}`, `Operator.{h,cpp}`, `Driver.h`, `LocalPlanner.cpp`, `MergeSource.cpp`, `MultiFragmentTest.cpp`, both `CMakeLists.txt`). It applies **cleanly** — 16980 is already in the base, so there is no conflict to resolve.

### Task 1.1: Apply the rename from upstream #18110

**Files:** the ~15 `velox/exec` files above.

**Interfaces:**
- Produces: concrete class `InMemoryExchangeClient` (was `ExchangeClient`); the name `ExchangeClient` is now free. `Exchange` operator ctor takes `std::shared_ptr<InMemoryExchangeClient>`. `Task::exchangeClients_` is `std::vector<std::shared_ptr<InMemoryExchangeClient>>`.

- [ ] **Step 1: Apply PR #18110's net rename diff and stage it**

```bash
cd /gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side
git checkout poc-exchange-transport-integration
git diff 49e8aac49..dd00e11d5 | git apply --index
```

(`dd00e11d5` = the rebased #18110 tip; diffing against `49e8aac49` = merged-main tip yields the rename only. Preserves git rename detection.)

- [ ] **Step 2: Verify no stray concrete-class references**

Run: `git grep -n "\bExchangeClient\b" velox/exec | grep -v InMemoryExchangeClient | grep -v "ExchangeClientTest\|ExchangeClientPool"`
Expected: no lines referring to the concrete class (the name is now free for the interface added in commit 3).

- [ ] **Step 3: Commit** (already staged by `--index`; the upstream diff is pre-formatted)

```bash
git commit -m "refactor(exec)!: Rename ExchangeClient to InMemoryExchangeClient

The concrete exchange client becomes InMemoryExchangeClient, freeing the
name ExchangeClient for the abstract transport interface. Mechanical
rename, no behavior change. Mirrors output-side rename #18034.

BREAKING CHANGE: exec::ExchangeClient is renamed to InMemoryExchangeClient
and its header moved to InMemoryExchangeClient.h. Downstream code that
constructs or references the concrete client, or overrides
Operator::PlanNodeTranslator::toOperator(..., std::shared_ptr<ExchangeClient>),
must update. Custom exchange transports will register an ExchangeFactory in
ExchangeTransportRegistry (added in a follow-up commit) instead."
```

### Task 1.2: Build-verify the rename (CPU path — no cuDF, no GPU)

- [ ] **Step 1: Build the exchange targets in docker**

Use the single cuDF-enabled build (`gpu_build_run.sh` / `make cmake-cudf VELOX_BUILD_TESTING=ON`; memory `velox-gpu-unit-tests-in-dep-image`), `VELOX_DIR=/gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side`, image `prestobuild/dependency:cio-new`. Build `TARGETS="velox_exec_infra_test $GRP"` where `$GRP` is the group binary carrying `InMemoryExchangeClientTest.cpp` (derive from `build.ninja`). This is a pure CPU-path test, so it does not need `--gpus` at run time and cuDF stays unregistered.
Expected marker: `BUILD_OK`.

- [ ] **Step 2: Run the renamed test**

Run (inside the image, no cuDF registration needed): `./"$GRP" --gtest_filter="*InMemoryExchangeClientTest*"`
Expected: all cases PASS (parameterized-suite filter form — see the memory's gotcha).

- [ ] **Step 3: No commit** (build-only verification; the code commit is Task 1.1).

---

## Phase 2 — Commit 2: transportKind on ExchangeNode

Reuses NODE FIELD commit `05439a07e` (touches only `core/PlanNode.{h,cpp}` + serde/builder/trace/toString tests; already references `TransportKind::kInMemory`).

### Task 2.1: Cherry-pick NODE FIELD

**Files:** Modify `velox/core/PlanNode.{h,cpp}`, `velox/core/tests/PlanNodeBuilderTest.cpp`, `velox/exec/tests/{MultiFragmentTest,PlanNodeSerdeTest,PlanNodeToStringTest}.cpp`, `velox/exec/tests/utils/PlanBuilder.{h,cpp}`, `velox/exec/trace/TraceUtil.cpp`.

**Interfaces:**
- Consumes: `core::TransportKind::kInMemory` (present in base).
- Produces: `ExchangeNode(id, type, serdeKind, transportKind)` 4-arg ctor; `ExchangeNode::transportKind()` accessor; `Builder::transportKind(...)` (required); `MergeExchangeNode` ctor gains `transportKind`. Serde `create()` defaults missing field to `kInMemory`. `addDetails` prints `[{serde} {transport}]`.

- [ ] **Step 1: Cherry-pick**

```bash
git cherry-pick --no-commit 05439a07e
```

- [ ] **Step 2: Resolve conflicts against the base (merged 16980 + rename)**

The base already carries 16980's `PlanNode.{h,cpp}` edits and serde/toString tests (for `PartitionedOutputNode.transportKind`), plus Commit 1's rename touching `MultiFragmentTest.cpp`. Resolve so all sets coexist; keep `ExchangeNode`'s new `transportKind_` member, 4-arg ctor, `Builder`, `serialize`/`create`, `addDetails`. Confirm `TransportKind::kInMemory` resolves (no duplicate definition — the base already defines it):

Run: `git grep -n "struct TransportKind" velox/core/PlanNode.h`
Expected: exactly one definition (from the base).

- [ ] **Step 3: Format, then commit**

```bash
git add -A && pre-commit run --files $(git diff --cached --name-only)
git commit -m "feat(core): Add transportKind to ExchangeNode

ExchangeNode, and by inheritance MergeExchangeNode, carry a transportKind
naming the receive transport, mirroring PartitionedOutputNode. Threaded
through the ctor, Builder, serialize/create, and addDetails; defaults to
the in-memory transport only when deserializing an older plan. Serialized
but not yet consumed at execution time."
```

### Task 2.2: Serde round-trip test for a non-default transport

**Files:** Test: `velox/exec/tests/PlanNodeSerdeTest.cpp`.

- [ ] **Step 1: Write the failing test**

Add next to the existing exchange serde test:

```cpp
TEST_F(PlanNodeSerdeTest, exchangeTransportKindRoundTrip) {
  auto plan = PlanBuilder()
                  .exchange(ROW({"a"}, {BIGINT()}), VectorSerde::Kind::kPresto)
                  .planNode();
  auto exchange = std::dynamic_pointer_cast<const core::ExchangeNode>(plan);
  ASSERT_NE(exchange, nullptr);
  // Rebuild with a non-default transport.
  auto ucxExchange = core::ExchangeNode::Builder(*exchange)
                         .transportKind(std::string{core::TransportKind::kUcx})
                         .build();
  testSerde(ucxExchange);
  EXPECT_EQ(ucxExchange->transportKind(), core::TransportKind::kUcx);
}
```

- [ ] **Step 2: Run to verify it fails (if `Builder`/round-trip incomplete)**

Run (docker, group binary for `PlanNodeSerdeTest`): `./"$GRP" --gtest_filter="*PlanNodeSerdeTest.exchangeTransportKindRoundTrip*"`
Expected: FAIL if serde doesn't preserve `kUcx`; PASS confirms NODE FIELD serde is complete. If it already passes because Task 2.1 was complete, keep it as a regression guard.

- [ ] **Step 3: Make it pass** (only if failing — fix `serialize`/`create` in `PlanNode.cpp`).

- [ ] **Step 4: Commit**

```bash
git add velox/exec/tests/PlanNodeSerdeTest.cpp
git commit -m "test(exec): Round-trip ExchangeNode transportKind with a non-default transport"
```

---

## Phase 3 — Commit 3: Exchange transport registry + resolution

The core mirror of `OutputTransportRegistry`. Multiple tasks; commit once at the end of Task 3.7 (the whole registry is one logical change), with intermediate red/green cycles.

### Task 3.1: Abstract ExchangeClient interface

**Files:** Create `velox/exec/ExchangeClient.h`. Modify `velox/exec/InMemoryExchangeClient.h` (add `: public ExchangeClient`, `override`).

**Interfaces:**
- Produces: `class ExchangeClient` with pure-virtual `addRemoteTaskId`, `noMoreRemoteTasks`, `close`, `stats`. `class InMemoryExchangeClient : public ExchangeClient`.

- [ ] **Step 1: Write the failing test**

Add to a new `velox/exec/tests/ExchangeTransportRegistryTest.cpp` (fleshed out in 3.5) a minimal compile/behaviour check first:

```cpp
#include "velox/exec/ExchangeClient.h"
#include "velox/exec/InMemoryExchangeClient.h"
#include <gtest/gtest.h>
namespace facebook::velox::exec {
TEST(ExchangeClientInterfaceTest, inMemoryIsExchangeClient) {
  static_assert(std::is_base_of_v<ExchangeClient, InMemoryExchangeClient>);
  SUCCEED();
}
} // namespace facebook::velox::exec
```

- [ ] **Step 2: Verify it fails to compile** (no `ExchangeClient.h` yet).

- [ ] **Step 3: Create the interface**

`velox/exec/ExchangeClient.h`:

```cpp
/* Apache 2.0 header */
#pragma once

#include <folly/container/F14Map.h>
#include <string>
#include "velox/common/statistics/RuntimeMetric.h" // wherever RuntimeMetric lives; match InMemoryExchangeClient.h's include

namespace facebook::velox::exec {

/// Control-plane handle for the set of upstream producers feeding one exchange
/// plan node. One instance per ExchangeNode, owned by Task and shared across
/// every driver thread running that node's exchange operator. Concrete
/// implementations (InMemoryExchangeClient, UcxExchangeClient) add the
/// transport-specific data plane (fetch / queue) and are paired with their
/// operator in ExchangeTransportRegistry.
///
/// Thread safety: one instance is driven concurrently from all of a node's
/// driver threads, so every method must be safe under concurrent calls.
///
/// Lifecycle: addRemoteTaskId() any number of times, then noMoreRemoteTasks()
/// once no more upstream tasks will arrive; close() exactly once at teardown
/// (idempotent). Completion is defined by noMoreRemoteTasks() + drained data,
/// surfaced through the concrete data plane, not by this interface.
class ExchangeClient {
 public:
  virtual ~ExchangeClient() = default;

  /// Starts fetching from upstream task 'remoteTaskId'. Idempotent: repeated
  /// calls with the same id are ignored. Safe to call after close() (the source
  /// is created and immediately closed to notify the producer).
  virtual void addRemoteTaskId(const std::string& remoteTaskId) = 0;

  /// Signals that no further addRemoteTaskId() calls will occur.
  virtual void noMoreRemoteTasks() = 0;

  /// Closes the client and its sources. Idempotent.
  virtual void close() = 0;

  /// Runtime statistics aggregated across sources, as a transport-neutral map.
  /// Implementations report background CPU time under
  /// Operator::kBackgroundCpuTimeNanos.
  virtual folly::F14FastMap<std::string, RuntimeMetric> stats() = 0;
};

} // namespace facebook::velox::exec
```

- [ ] **Step 4: Make InMemoryExchangeClient implement it**

In `InMemoryExchangeClient.h`: `#include "velox/exec/ExchangeClient.h"`; change the class head to `class InMemoryExchangeClient : public ExchangeClient, public std::enable_shared_from_this<InMemoryExchangeClient>`; add `override` to `addRemoteTaskId`, `noMoreRemoteTasks`, `close`, `stats`. Leave the data plane (`next`, `queue`, `pool`, `toString`, `toJson`, `requestDataSizesMaxWaitSec`) as concrete, non-virtual.

- [ ] **Step 5: Verify the static_assert compiles/passes** (built as part of 3.5's binary).

### Task 3.2: ExchangeFactory + client-context typedefs

**Files:** Create `velox/exec/ExchangeFactory.h`.

**Interfaces:**
- Produces: `ExchangeClientContext`, `ExchangeClientFactory`, `ExchangeFactory`.

- [ ] **Step 1: Create the header** (mirror of `PartitionedOutputFactory.h`)

```cpp
/* Apache 2.0 header */
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace facebook::velox::memory { class MemoryPool; }
namespace facebook::velox::core {
class ExchangeNode;
class QueryConfig;
} // namespace facebook::velox::core
namespace folly { class Executor; }

namespace facebook::velox::exec {

struct DriverCtx;
class Operator;
class ExchangeClient;

/// Task-supplied context for building a per-node exchange client. Transport
/// implementations read only the fields they need; the in-memory transport
/// reads the exchange knobs off 'queryConfig'.
struct ExchangeClientContext {
  std::string taskId;
  int destination;
  int32_t numberOfConsumers;
  memory::MemoryPool* pool;
  folly::Executor* executor;
  const core::QueryConfig& queryConfig;
};

/// Builds the per-node exchange client. Task calls this once per ExchangeNode
/// and owns the result (shared across drivers).
using ExchangeClientFactory =
    std::function<std::shared_ptr<ExchangeClient>(const ExchangeClientContext&)>;

/// Builds the exchange operator for a node, bound to the client Task created.
/// The two are registered together in ExchangeTransportRegistry so they cannot
/// diverge; the operator downcasts 'client' to its concrete type.
using ExchangeFactory = std::function<std::unique_ptr<Operator>(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::ExchangeNode>& node,
    std::shared_ptr<ExchangeClient> client)>;

} // namespace facebook::velox::exec
```

- [ ] **Step 2: No standalone test** (exercised via the registry in 3.5).

### Task 3.3: ExchangeTransportRegistry (.h/.cpp)

**Files:** Create `velox/exec/ExchangeTransportRegistry.{h,cpp}`. Modify `velox/exec/CMakeLists.txt` (add the `.cpp`).

**Interfaces:**
- Produces: `struct ExchangeTransportEntry { ExchangeClientFactory makeClient; ExchangeFactory makeOperator; }`; `class ExchangeTransportRegistry` with `Registry`, `kRegistryKey="exchangeTransports"`, `global()`, `create(parent)`, `tryGet(queryCtx,id)`, `tryGet(id)`, `getAll(queryCtx)`, `getAll()`, `unregisterAll(queryCtx)`, `unregisterAll()`.

- [ ] **Step 1: Create the header** — mirror `OutputTransportRegistry.h` with these substitutions: drop the `make()` weak-capture helper (the receive entry is a plain aggregate — the client is Task-created, not entry-held); `OutputTransportEntry`→`ExchangeTransportEntry`; fields become `ExchangeClientFactory makeClient` + `ExchangeFactory makeOperator`; `OutputTransportRegistry`→`ExchangeTransportRegistry`; `kRegistryKey`→`"exchangeTransports"`; include `ExchangeFactory.h` instead of `OutputBufferManager.h`/`PartitionedOutputFactory.h`.

```cpp
/* Apache 2.0 header */
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "velox/common/ScopedRegistry.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/ExchangeFactory.h"

namespace facebook::velox::core { class QueryCtx; }

namespace facebook::velox::exec {

/// Registry value pairing the factory that builds a transport's exchange client
/// with the factory that builds its matching exchange operator, keyed by
/// transport id. Registering the two together keeps a transport's client and
/// operator from diverging. Plain aggregate: unlike the output side there is no
/// long-lived manager instance to hold — the client is created per node by Task.
struct ExchangeTransportEntry {
  ExchangeClientFactory makeClient;
  ExchangeFactory makeOperator;
};

/// Manages exchange-transport registration and lookup, keyed by transport id.
/// Query-scoped APIs (QueryCtx& overloads) check per-query overrides before the
/// global registry; global APIs operate on the process-wide registry. All
/// methods are thread-safe.
class ExchangeTransportRegistry {
 public:
  using Registry = ScopedRegistry<std::string, ExchangeTransportEntry>;
  static constexpr std::string_view kRegistryKey = "exchangeTransports";

  static Registry& global();
  static std::shared_ptr<Registry> create(const Registry* parent = nullptr);
  static std::shared_ptr<ExchangeTransportEntry> tryGet(
      const core::QueryCtx& queryCtx, const std::string& id);
  static std::shared_ptr<ExchangeTransportEntry> tryGet(const std::string& id);
  static std::vector<std::pair<std::string, std::shared_ptr<ExchangeTransportEntry>>>
  getAll(const core::QueryCtx& queryCtx);
  static std::vector<std::pair<std::string, std::shared_ptr<ExchangeTransportEntry>>>
  getAll();
  static void unregisterAll(const core::QueryCtx& queryCtx);
  static void unregisterAll();

 private:
  static std::vector<std::pair<std::string, std::shared_ptr<ExchangeTransportEntry>>>
  snapshot(const core::QueryCtx& queryCtx);
};

} // namespace facebook::velox::exec
```

- [ ] **Step 2: Create the .cpp** — mirror `OutputTransportRegistry.cpp` verbatim with the same substitutions, and seed the in-memory default from `InMemoryExchangeClient::makeDefaultTransportEntry()` (added in 3.4). `#include "velox/exec/InMemoryExchangeClient.h"`, `velox/core/QueryCtx.h`, `velox/core/PlanNode.h`. `registerBuiltinDefault` inserts `core::TransportKind::kInMemory → InMemoryExchangeClient::makeDefaultTransportEntry()`. Keep the backward-compat-shim comment.

- [ ] **Step 3: Add to CMake** — append `ExchangeTransportRegistry.cpp` to the `velox_exec` sources in `velox/exec/CMakeLists.txt` (next to where `OutputTransportRegistry.cpp` is listed).

### Task 3.4: In-memory default transport entry

**Files:** Modify `velox/exec/InMemoryExchangeClient.{h,cpp}`.

**Interfaces:**
- Consumes: `ExchangeTransportEntry`, `ExchangeClientContext`, `Exchange` operator ctor `Exchange(id, ctx, node, shared_ptr<InMemoryExchangeClient>)`.
- Produces: `static std::shared_ptr<ExchangeTransportEntry> InMemoryExchangeClient::makeDefaultTransportEntry();`.

- [ ] **Step 1: Declare in header**

Add to `InMemoryExchangeClient.h`:

```cpp
  /// Builds the built-in in-memory transport entry: a client factory that
  /// constructs an InMemoryExchangeClient from query config, and an operator
  /// factory that builds the Exchange operator bound to it.
  static std::shared_ptr<ExchangeTransportEntry> makeDefaultTransportEntry();
```

- [ ] **Step 2: Implement in .cpp**

```cpp
// static
std::shared_ptr<ExchangeTransportEntry>
InMemoryExchangeClient::makeDefaultTransportEntry() {
  return std::make_shared<ExchangeTransportEntry>(ExchangeTransportEntry{
      // makeClient: reproduce Task::createExchangeClientLocked's construction.
      [](const ExchangeClientContext& c) -> std::shared_ptr<ExchangeClient> {
        return std::make_shared<InMemoryExchangeClient>(
            c.taskId,
            c.destination,
            c.queryConfig.maxExchangeBufferSize(),
            c.numberOfConsumers,
            c.queryConfig.minExchangeOutputBatchBytes(),
            c.pool,
            c.executor,
            c.queryConfig.requestDataSizesMaxWaitSec(),
            c.queryConfig.singleSourceExchangeOptimizationEnabled(),
            c.queryConfig.exchangeLazyFetchingEnabled());
      },
      // makeOperator: downcast to the concrete client and build Exchange.
      [](int32_t operatorId,
         DriverCtx* ctx,
         const std::shared_ptr<const core::ExchangeNode>& node,
         std::shared_ptr<ExchangeClient> client) -> std::unique_ptr<Operator> {
        auto inMemory =
            std::dynamic_pointer_cast<InMemoryExchangeClient>(std::move(client));
        VELOX_CHECK_NOT_NULL(
            inMemory,
            "In-memory exchange transport requires an InMemoryExchangeClient");
        return std::make_unique<Exchange>(
            operatorId, ctx, node, std::move(inMemory));
      }});
}
```

Add includes for `ExchangeTransportRegistry.h`, `Exchange.h`, `velox/core/QueryConfig.h` as needed.

### Task 3.5: Registry unit test (standalone binary)

**Files:** Create/flesh `velox/exec/tests/ExchangeTransportRegistryTest.cpp`. Modify `velox/exec/tests/CMakeLists.txt`.

**Interfaces:**
- Consumes: `ExchangeTransportRegistry`, `ExchangeTransportEntry`, `ExchangeClient`.

- [ ] **Step 1: Write the tests** — mirror `OutputTransportRegistryTest.cpp` with a `MockExchangeClient : public ExchangeClient` (empty overrides) and `makeEntry`:

```cpp
namespace facebook::velox::exec {
namespace {
using ::testing::SizeIs;

class MockExchangeClient : public ExchangeClient {
 public:
  void addRemoteTaskId(const std::string&) override {}
  void noMoreRemoteTasks() override {}
  void close() override {}
  folly::F14FastMap<std::string, RuntimeMetric> stats() override { return {}; }
};

std::shared_ptr<ExchangeTransportEntry> makeEntry() {
  return std::make_shared<ExchangeTransportEntry>(ExchangeTransportEntry{
      [](const ExchangeClientContext&) -> std::shared_ptr<ExchangeClient> {
        return std::make_shared<MockExchangeClient>();
      },
      [](int32_t, DriverCtx*, const std::shared_ptr<const core::ExchangeNode>&,
         std::shared_ptr<ExchangeClient>) -> std::unique_ptr<Operator> {
        return nullptr;
      }});
}

TEST(ExchangeTransportRegistryTest, registryOperations) {
  ExchangeTransportRegistry::unregisterAll();
  for (int i = 0; i < 5; ++i) {
    ExchangeTransportRegistry::global().insert(
        fmt::format("t-{}", i), makeEntry());
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(ExchangeTransportRegistry::tryGet(fmt::format("t-{}", i)), nullptr);
  }
  EXPECT_EQ(ExchangeTransportRegistry::tryGet("nonexistent"), nullptr);
  // getAll includes the always-available built-in in-memory default.
  EXPECT_THAT(ExchangeTransportRegistry::getAll(), SizeIs(5 + 1));
}

TEST(ExchangeTransportRegistryTest, builtinInMemoryAlwaysPresent) {
  ExchangeTransportRegistry::unregisterAll();
  EXPECT_NE(
      ExchangeTransportRegistry::tryGet(
          std::string{core::TransportKind::kInMemory}),
      nullptr);
}

// Add, mirroring the output-side test: per-query override vs global fallback
// (via ExchangeTransportRegistry::create(&global()) on a QueryCtx), isolation
// mode (create(nullptr)), and unregisterAll(queryCtx) clearing only overrides.
} // namespace
} // namespace facebook::velox::exec
```

- [ ] **Step 2: Add the standalone binary**

In `velox/exec/tests/CMakeLists.txt`, next to `add_executable(velox_output_transport_registry_test OutputTransportRegistryTest.cpp)`:

```cmake
add_executable(velox_exchange_transport_registry_test ExchangeTransportRegistryTest.cpp)
add_test(NAME velox_exchange_transport_registry_test COMMAND velox_exchange_transport_registry_test)
target_link_libraries(
  velox_exchange_transport_registry_test
  velox_exec velox_exec_test_lib GTest::gtest GTest::gtest_main)
```

Match the exact `target_link_libraries` list used by `velox_output_transport_registry_test`.

- [ ] **Step 3: Build + run (CPU path — same cuDF build, no cuDF registration, no GPU)**

Build `TARGETS="velox_exchange_transport_registry_test"` from the single cuDF build; run `./velox_exchange_transport_registry_test`.
Expected: all cases PASS.

### Task 3.6: Resolve client via registry in Task; store abstract client

**Files:** Modify `velox/exec/Task.cpp`, `velox/exec/Task.h`.

**Interfaces:**
- Consumes: `ExchangeTransportRegistry::tryGet`, `ExchangeTransportEntry::makeClient`, `ExchangeClientContext`.
- Produces: `exchangeClients_` / `exchangeClientByPlanNode_` now hold `std::shared_ptr<ExchangeClient>` (abstract). `createExchangeClientLocked` takes the node's `transportKind`.

- [ ] **Step 1: Change stored type to abstract**

In `Task.h`, change `exchangeClients_` to `std::vector<std::shared_ptr<ExchangeClient>>` and `exchangeClientByPlanNode_` to map to `std::shared_ptr<ExchangeClient>`; update `getExchangeClient*` return types. Add `#include "velox/exec/ExchangeClient.h"` (forward-declare where possible). Verify Task calls only control-plane methods on the client (`addRemoteTaskId`, `noMoreRemoteTasks`, `close`, `stats`) — grep confirms no `->next()`/`->queue()`/`->pool()` on a Task-held client.

- [ ] **Step 2: Rewrite `createExchangeClientLocked`**

```cpp
void Task::createExchangeClientLocked(
    int32_t pipelineId,
    const core::PlanNodeId& planNodeId,
    const std::string& transportKind,
    int32_t numberOfConsumers) {
  VELOX_CHECK_NULL(
      getExchangeClientLocked(pipelineId),
      "Exchange client has been created at pipeline: {} for planNode: {}",
      pipelineId, planNodeId);
  VELOX_CHECK_NULL(
      getExchangeClientLocked(planNodeId),
      "Exchange client has been created for planNode: {}", planNodeId);
  auto entry = ExchangeTransportRegistry::tryGet(*queryCtx(), transportKind);
  VELOX_USER_CHECK_NOT_NULL(
      entry, "No exchange transport registered for transport: {}", transportKind);
  const ExchangeClientContext context{
      taskId_,
      destination_,
      numberOfConsumers,
      addExchangeClientPool(planNodeId, pipelineId),
      queryCtx()->executor(),
      queryCtx()->queryConfig()};
  exchangeClients_[pipelineId] = entry->makeClient(context);
  exchangeClientByPlanNode_.emplace(planNodeId, exchangeClients_[pipelineId]);
}
```

Update the declaration in `Task.h` (add `const std::string& transportKind`).

- [ ] **Step 3: Update the caller** (~`Task.cpp:1315`)

The caller has the exchange plan node (or gets it from `needsExchangeClient()` — see Task 3.7 Step 3). Pass `exchangeNode->transportKind()` as the new argument.

### Task 3.7: Build Exchange via operator factory; delete the fallback path

**Files:** Modify `velox/exec/LocalPlanner.cpp`, `velox/exec/Operator.{h,cpp}`, `velox/core/PlanNode.h`, `velox/exec/Driver.h`.

**Interfaces:**
- Consumes: `ExchangeTransportRegistry::tryGet`, `ExchangeTransportEntry::makeOperator`.
- Produces: `toOperator(...,exchangeClient)` / `fromPlanNode(...,exchangeClient)` / `requiresExchangeClient()` removed. `Exchange` built via the entry's operator factory.

- [ ] **Step 1: Replace the Exchange branch in LocalPlanner** (`LocalPlanner.cpp:553-560`)

```cpp
    } else if (
        auto exchangeNode =
            std::dynamic_pointer_cast<const core::ExchangeNode>(planNode)) {
      // The exchange client is Task-owned and shared across this node's drivers.
      VELOX_CHECK_NOT_NULL(exchangeClient);
      auto entry = ExchangeTransportRegistry::tryGet(
          *ctx->task->queryCtx(), exchangeNode->transportKind());
      VELOX_USER_CHECK_NOT_NULL(
          entry,
          "No exchange transport registered for transport: {}",
          exchangeNode->transportKind());
      operators.push_back(entry->makeOperator(
          id, ctx.get(), exchangeNode, std::move(exchangeClient)));
    } else if (
```

Add `#include "velox/exec/ExchangeTransportRegistry.h"`. (The `MergeExchangeNode` branch above it is untouched here — commit 4.)

- [ ] **Step 2: Delete the translator fallback + overload**

- `LocalPlanner.cpp:717-722`: remove the `if (planNode->requiresExchangeClient()) { ... fromPlanNode(ctx, id, planNode, exchangeClient) ... }` branch (keep the non-client `fromPlanNode`).
- `Operator.cpp:176-190`: delete the `fromPlanNode(ctx, id, planNode, std::shared_ptr<ExchangeClient>)` overload entirely.
- `Operator.h:141`: delete its declaration and the `toOperator(..., std::shared_ptr<ExchangeClient>)` virtual on `PlanNodeTranslator` (and any default impl).
- `PlanNode.h:208` and `PlanNode.h:2235`: delete `requiresExchangeClient()` (base virtual + `ExchangeNode` override).

- [ ] **Step 3: Convert `Driver.h:877`**

```cpp
  std::optional<core::PlanNodeId> needsExchangeClient() const {
    VELOX_CHECK(!planNodes.empty());
    if (std::dynamic_pointer_cast<const core::ExchangeNode>(planNodes.front())) {
      return planNodes.front()->id();
    }
    return std::nullopt;
  }
```

(Optionally return `std::shared_ptr<const core::ExchangeNode>` so the caller reads `transportKind` without a re-lookup; then thread it to `createExchangeClientLocked`.)

- [ ] **Step 4: Build (single cuDF build)** — `TARGETS="velox_exec_infra_test"`; expect `BUILD_OK` (proves the deletions compile and nothing else referenced the overload).

### Task 3.8: Resolution behaviour test (grouped suite) + commit

**Files:** Create `velox/exec/tests/ExchangeTransportTest.cpp`. Modify `velox/exec/tests/CMakeLists.txt` (add to the grouped `SOURCES`, next to `OutputTransportTest.cpp` at line ~225).

**Interfaces:**
- Consumes: `ExchangeTransportRegistry`, a CPU test-transport double (a trivial `ExchangeClient` + a test `Exchange`-like operator, or reuse `InMemoryExchangeClient` under a test transport id).

- [ ] **Step 1: Write the tests** (mirror `OutputTransportTest.cpp`)

```cpp
// selectsOperatorByTransportKind: register a CPU test transport under a test id,
//   build a plan whose ExchangeNode names it, run via AssertQueryBuilder, and
//   assert the test transport's client factory + operator factory were invoked.
// usesInMemoryByDefault: a plan with the default transportKind resolves the
//   built-in in-memory entry (results match a normal exchange).
// errorsOnUnregisteredTransport: a plan naming an unregistered transport fails
//   with "No exchange transport registered for transport: <name>".
// usesDefaultAfterRegistryClear: unregisterAll() restores the in-memory default.
```

Write the full bodies mirroring `OutputTransportTest.cpp`'s structure (register test entry via `ExchangeTransportRegistry::global().insert(...)`, drive a real Task with `AssertQueryBuilder`, assert on invocation counters captured in the test entry's factories, `unregisterAll()` in `TearDown`).

- [ ] **Step 2: Add to grouped SOURCES** in `velox/exec/tests/CMakeLists.txt` (the `velox_exec_test` group list, alongside `OutputTransportTest.cpp`).

- [ ] **Step 3: Build + run (CPU path — same cuDF build, cuDF unregistered, no GPU)** — derive the group binary for `ExchangeTransportTest.cpp`; run `--gtest_filter="*ExchangeTransportTest*"`. Expect PASS.

- [ ] **Step 4: Format + commit the whole registry change**

```bash
git add -A && pre-commit run --files $(git diff --cached --name-only)
git commit -m "feat(exec): Exchange transport registry and per-transport resolution

Add an abstract ExchangeClient control-plane interface (InMemoryExchangeClient
implements it) and a query-scoped ExchangeTransportRegistry pairing a client
factory with an exchange-operator factory per transport, mirroring the output
side. Task resolves the entry by the node's transportKind and builds the
Task-owned client from the factory; LocalPlanner builds the operator from it.
The built-in in-memory transport is a seeded entry; an unregistered transport
fails fast. Remove the toOperator(...,exchangeClient) extensibility overload
and requiresExchangeClient(); custom transports register an ExchangeFactory."
```

---

## Phase 4 — Commit 4: MergeExchange resolution

Extend resolution to the merge path. The merge client is created in `MergeSource.cpp`, and the `MergeExchange` operator is built in LocalPlanner without a client (it self-manages merge sources).

### Task 4.1: Resolve the merge client via the registry

**Files:** Modify `velox/exec/MergeSource.cpp` (~line 224), `velox/exec/Task.cpp`/`Task.h` if merge client creation routes through a Task helper.

**Interfaces:**
- Consumes: `ExchangeTransportRegistry::tryGet`, `ExchangeTransportEntry::makeClient`.

- [ ] **Step 1: Write the failing test**

Add to `ExchangeTransportTest.cpp`: `mergeExchangeUsesTransportKind` — a `MergeExchangeNode` naming a CPU test transport resolves the merge client from that transport's `makeClient`. Assert the test factory's counter increments.

- [ ] **Step 2: Run to verify it fails** (merge path still hardcodes the in-memory client).

- [ ] **Step 3: Replace the hardcoded `make_shared<InMemoryExchangeClient>` at `MergeSource.cpp:224`** with a registry resolution: `ExchangeTransportRegistry::tryGet(*queryCtx, mergeExchangeNode->transportKind())` → `entry->makeClient(context)` (context built as in `createExchangeClientLocked`, `numberOfConsumers=1` per the current merge semantics). Fail fast with `VELOX_USER_CHECK_NOT_NULL`.

- [ ] **Step 4: Run to verify it passes** (CPU path — same cuDF build, cuDF unregistered, grouped binary).

- [ ] **Step 5: Commit**

```bash
git add -A && pre-commit run --files $(git diff --cached --name-only)
git commit -m "feat(exec): Per-transport resolution for MergeExchange

Resolve the MergeExchange client from ExchangeTransportRegistry by the
MergeExchangeNode's transportKind, mirroring the plain-exchange path."
```

---

## Phase 5 — Commit 5: Wire UCX into the registry

Make `UcxExchangeClient` an `exec::ExchangeClient`, register the UCX entry, and route `ucx_exchange_test` through the registry. GPU build.

### Task 5.1: UcxExchangeClient implements exec::ExchangeClient

**Files:** Modify `velox/experimental/ucx-exchange/UcxExchangeClient.{h,cpp}`.

**Interfaces:**
- Consumes: `exec::ExchangeClient`.
- Produces: `class UcxExchangeClient : public exec::ExchangeClient` with `addRemoteTaskId`/`noMoreRemoteTasks`/`close`/`stats` as overrides. Data plane (`next`/`queue`) stays concrete.

- [ ] **Step 1: Confirm the current control-plane surface**

Run: `git grep -n "addRemoteTaskId\|noMoreRemoteTasks\|void close\|stats()" velox/experimental/ucx-exchange/UcxExchangeClient.h`
Expected: methods with signatures compatible with the interface (adjust signatures to match exactly).

- [ ] **Step 2: Make it derive from the interface**

`#include "velox/exec/ExchangeClient.h"`; class head `class UcxExchangeClient : public exec::ExchangeClient`; add `override` to the four control-plane methods, aligning signatures (e.g. `folly::F14FastMap<std::string, RuntimeMetric> stats() override;` — the current empty `{}` body is acceptable per the PoC's bounded scope; leave a `// TODO:` noting the symmetry gap). Keep `next()`/`queue()` concrete.

- [ ] **Step 3: Build (single cuDF build)** — target `ucx_exchange_test`, don't run yet. Expect `BUILD_OK` (proves the interface fits).

### Task 5.2: Register the UCX transport entry

**Files:** Modify (or create a small registration TU in) `velox/experimental/ucx-exchange/` — add a `registerUcxExchangeTransport()` free function; call it where the UCX module initializes (mirror how the adapter registers its other hooks).

**Interfaces:**
- Produces: `void registerUcxExchangeTransport();` inserting `TransportKind::kUcx → { makeClient→UcxExchangeClient, makeOperator→UcxExchange }` into `ExchangeTransportRegistry::global()`.

- [ ] **Step 1: Implement the registration**

```cpp
void registerUcxExchangeTransport() {
  exec::ExchangeTransportRegistry::global().insert(
      std::string{core::TransportKind::kUcx},
      std::make_shared<exec::ExchangeTransportEntry>(exec::ExchangeTransportEntry{
          [](const exec::ExchangeClientContext& c)
              -> std::shared_ptr<exec::ExchangeClient> {
            return std::make_shared<UcxExchangeClient>(
                c.taskId, c.destination, c.numberOfConsumers /*, captured UCX infra */);
          },
          [](int32_t operatorId,
             exec::DriverCtx* ctx,
             const std::shared_ptr<const core::ExchangeNode>& node,
             std::shared_ptr<exec::ExchangeClient> client)
              -> std::unique_ptr<exec::Operator> {
            auto ucx = std::dynamic_pointer_cast<UcxExchangeClient>(std::move(client));
            VELOX_CHECK_NOT_NULL(ucx, "UCX transport requires a UcxExchangeClient");
            return std::make_unique<UcxExchange>(operatorId, ctx, node, std::move(ucx));
          }}),
      /*overwrite=*/true);
}
```

Match `UcxExchangeClient`'s real ctor params; capture any process-wide UCX infra the client needs in the `makeClient` closure.

- [ ] **Step 2: No standalone test** (exercised by Task 5.3).

### Task 5.3: Route ucx_exchange_test through the registry

**Files:** Modify `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`, `SinkDriverMock.{h,cpp}` / `SourceDriverMock.{h,cpp}`.

**Interfaces:**
- Consumes: `registerUcxExchangeTransport()`, `ExchangeTransportRegistry::tryGet`.

- [ ] **Step 1: Register in test setup**

In the test fixture `SetUp()` (or `main`), call `registerUcxExchangeTransport()`. In `TearDown()`, `ExchangeTransportRegistry::unregisterAll()`.

- [ ] **Step 2: Build the UcxExchange/UcxExchangeClient via the registry in the mock driver**

Where `SinkDriverMock` / the test currently constructs `UcxExchange` / `UcxExchangeClient` directly, replace with: build an `ExchangeNode` carrying `transportKind = kUcx`; resolve `entry = ExchangeTransportRegistry::tryGet(*queryCtx, node->transportKind())`; create the client via `entry->makeClient(context)` and the operator via `entry->makeOperator(id, ctx, node, client)`. Assert `entry != nullptr`.

- [ ] **Step 3: Build + run (docker GPU) via the wrapper**

Use `gpu_build_run.sh` (memory `velox-gpu-unit-tests-in-dep-image`):

```bash
VELOX_DIR=/gpfs/zc2/u/dnb/velox_workspaces/exchange-recv-side \
NAME=ucxpoc LOG=/gpfs/zc2/u/dnb/velox_workspaces/tz_build/ucx.log \
TARGETS="ucx_exchange_test" \
RUN_CMDS="velox/experimental/ucx-exchange/tests/ucx_exchange_test|*" \
  bash /gpfs/zc2/u/dnb/velox_workspaces/tz_build/gpu_build_run.sh
```

Then Monitor `LOG` for `BUILD_FAILED|CONFIGURE_FAILED|CACHE_NOT_GCC14_WIPE_NEEDED|ALL_TESTS_PASSED|SOME_TESTS_FAILED`.
Expected: `ALL_TESTS_PASSED`.

- [ ] **Step 4: Format + commit**

```bash
git add -A && pre-commit run --files $(git diff --cached --name-only)
git commit -m "feat(ucx): Wire UCX transport into ExchangeTransportRegistry

UcxExchangeClient implements the abstract exec::ExchangeClient interface and a
UCX entry (client + UcxExchange operator factory) is registered under
TransportKind::kUcx, so a plan naming the UCX transport resolves the UCX path
through the registry. ucx_exchange_test drives the operator/client via the
registry. Full UCX/HTTP client symmetry (stats, byte-budget backpressure,
memory-pool accounting) is intentionally out of scope for this PoC."
```

---

## Phase 6 — Final verification

- [ ] **Step 1: Whole-branch CPU-path tests green** (single cuDF build, cuDF unregistered, no GPU): `velox_exchange_transport_registry_test`, the `ExchangeTransportTest` group binary, `PlanNodeSerdeTest`/`PlanNodeToStringTest` group binaries, `InMemoryExchangeClientTest` group binary. All PASS.
- [ ] **Step 2: GPU gate green** (same cuDF build, cuDF registered + `--gpus all`): `ucx_exchange_test` → `ALL_TESTS_PASSED`.
- [ ] **Step 3: Confirm the commit stack on top of merged main**

Run: `git log --oneline dan13bauer/main..poc-exchange-transport-integration`
Expected (bottom→top): design+plan docs, rename (#18110), node field, (serde test), registry, mergeexchange, ucx.

---

## Self-Review Notes

- **Spec coverage:** rename (Phase 1), node field (Phase 2), registry + interface + delete-overload + Driver.h conversion (Phase 3), mergeexchange (Phase 4), UCX-via-registry + `ucx_exchange_test` (Phase 5), docker `cio-new`/gcc-14 verification (all build steps + Phase 6) — all present. Pre-applied review invariants encoded in Global Constraints + Task 3.1 interface contract.
- **Boundary:** Task 5.1 keeps `stats()` empty with a TODO; no pool/backpressure/queryConfig work — matches the approved "minimum UCX adaptation."
- **Type consistency:** `ExchangeClientContext` fields (Task 3.2) are consumed identically in Task 3.4, 3.6, 4.1, 5.2. Entry field names `makeClient`/`makeOperator` are consistent across 3.3/3.4/3.5/4.1/5.2. `ExchangeTransportRegistry` method names mirror `OutputTransportRegistry` exactly.
- **Known softness (resolve during implementation, not placeholders):** (a) exact `target_link_libraries` list for the new standalone test — copy `velox_output_transport_registry_test`'s; (b) `RuntimeMetric` include path in `ExchangeClient.h` — match `InMemoryExchangeClient.h`; (c) `UcxExchangeClient` ctor params + captured UCX infra in Task 5.2 — read from the current ctor; (d) how the UCX adapter's existing registration hook is invoked — mirror the adapter's pattern for `registerUcxExchangeTransport()`.
