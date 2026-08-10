# UCX Exchange Integration (PoC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `poc-exchange-transport-integration` branch compile, integrate, and run an end-to-end shuffle over **both** the default HTTP/in-memory transport and the **UCX/cuDF** transport, by wiring the existing `velox/experimental/ucx-exchange/` module into the PoC's own two registries.

**Architecture:** Keep the PoC design as-is — a receive-side `ExchangeTransportRegistry` (client + operator factories, abstract `exec::ExchangeClient`) and the output-side `OutputTransportRegistry` from upstream #16980 (abstract `exec::OutputBufferManager`). UCX plugs in as a second registered transport under key `core::TransportKind::kUcx` on both sides: `UcxExchangeClient`/`UcxExchange` on receive, `UcxOutputQueueManager`/`UcxPartitionedOutput` on output. Per-node selection is by the node's `transportKind`, resolved through the registries (unchanged from the PoC). The default HTTP/in-memory path stays the seeded default and is untouched.

**Tech Stack:** C++20, CMake, Velox exec, cuDF (libcudf 26.08), UCX 1.20.1 + ucxx 0.51, CUDA, GoogleTest. Build via `gpu_build_run.sh` in the cuDF-enabled image.

## Reference material (read before starting)

- Upstream PR **#17614** ("feat(cudf): Add exchange operator adapters with per-node transport selection") is an **outdated design** we are NOT adopting — it uses `OperatorAdapters` + `OutputBufferManagerRegistry`/`IOutputBufferManager`, not our two registries. We reuse it **only** for: (a) the build-layer CMake/docker deltas, and (b) as an authoritative reference for *which* `UcxOutputQueueManager` methods change and *how* to satisfy an output-buffer-manager interface. Re-fetch its diff when needed: `gh pr diff 17614 --repo facebookincubator/velox`.
- PoC README: `POC-README.md` (branch root) — the "Outlook" section is the prose version of parts A–D here.
- Interfaces the PoC already defines (keep, do not recreate):
  - Receive: `velox/exec/ExchangeClient.h` (abstract, 5 pure-virtuals), `velox/exec/ExchangeFactory.h` (`ExchangeClientContext`, `ExchangeClientFactory`, `ExchangeFactory`), `velox/exec/ExchangeTransportRegistry.{h,cpp}`.
  - Output: `velox/exec/OutputBufferManager.h` (abstract, 8 pure-virtuals), `velox/exec/PartitionedOutputFactory.h`, `velox/exec/OutputTransportRegistry.{h,cpp}`.

## Global Constraints

- **Do not change the PoC's registry design.** No `OperatorAdapters`, no `OutputBufferManagerRegistry`, no `IOutputBufferManager`. Both transports resolve through `ExchangeTransportRegistry` (receive) and `OutputTransportRegistry` (output).
- **libcudf pin is 26.08**; branch is rebased on latest `facebookincubator/velox` main. `UCX_FOUND`, ucxx-fetch (`CMake/resolve_dependency_modules/cudf.cmake`), and `install_ucx` (`scripts/setup-centos-adapters.sh`, UCX 1.20.1) are **already present** in the tree — do not re-add them.
- **The default HTTP/in-memory path must be byte-for-byte unchanged.** UCX is additive; `kInMemory` stays the seeded default and the only transport registered when cuDF exchange is off.
- **Transport-kind key** is `core::TransportKind::kUcx` (`"UCX"`); default `kInMemory` (`"in-memory"`). Reuse the existing constants — do not introduce new strings.
- **Registration is gated on `cudf.exchange`.** The `kUcx` entries are inserted only when the worker has cuDF exchange enabled (mirrors #17614's gating). When a plan node requests `kUcx` but no entry is registered, the existing PoC behavior applies: **fail fast** (`VELOX_USER_CHECK_NOT_NULL` in the registry lookup). Graceful degrade-to-HTTP is explicitly out of scope for this PoC.
- **Naming/comments:** follow `.claude/CLAUDE.md` — `///` only for public API in headers, `//` elsewhere; camelCase members with trailing `_`; no `*Utils`/`*Helpers` names; runtime values at the end of error messages.
- **Every code task is TDD:** write/adjust the failing test first, confirm it fails, implement, confirm it passes, commit. Build/infra tasks use "target compiles / test binary builds and runs" as the gate.
- **Build/run environment:** GPU image via `gpu_build_run.sh` (`--gpus all`, cuDF enabled). UCX tests are labeled `cuda_driver` and require a GPU + `cudf.enabled=true` + `cudf.exchange=true`.

## Design decisions to confirm at plan review

1. **MergeExchange over UCX (part D).** The abstract `exec::ExchangeClient` has no `next()`; `MergeExchangeSource::next()` needs a concrete data plane. Default in this plan: a **UCX merge-source adapter** that downcasts the resolved client to `UcxExchangeClient` (Task D1), least-invasive. Alternatives: (a) add a shared data-plane method to the abstract interface (touches in-memory too); (c) declare merge-over-UCX out of scope and keep merge in-memory-only. If (c) is acceptable for the PoC, skip Task D1 and only assert fail-fast.
2. **Registration site (part E).** Default: a `registerUcxExchange()` entry point in the ucx-exchange module, called from `ToCudf` when `CudfConfig::exchange` is set (reuses #17614's site and the `velox_cudf_exec → velox_ucx_exchange` link). If you prefer registration to live entirely inside the ucx-exchange module's own init, adjust Task E1 accordingly.

---

## File Structure

**Modified (build layer, from #17614):**
- `velox/CMakeLists.txt` — add `add_subdirectory(experimental/ucx-exchange)` gated on `UCX_FOUND` inside the `VELOX_ENABLE_CUDF` branch.
- `velox/experimental/ucx-exchange/CMakeLists.txt` — add `nvtx3::nvtx3-cpp` to PUBLIC link libs.
- `velox/experimental/cudf/exec/CMakeLists.txt` — add `velox_ucx_exchange` to `velox_cudf_exec` link libs.
- `scripts/docker/centos-multi.dockerfile` — add `install_ucx` step **only if** the build image lacks UCX (verify first).

**Modified (receive side):**
- `velox/experimental/ucx-exchange/UcxExchangeClient.{h,cpp}` — implement `exec::ExchangeClient`.
- New: `velox/experimental/ucx-exchange/UcxExchangeRegistration.{h,cpp}` — `registerUcxExchange()` inserting `kUcx` entries into both registries.
- `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`, `tests/SinkDriverMock.cpp` — route construction through the registry.

**Modified (output side):**
- `velox/experimental/ucx-exchange/UcxOutputQueueManager.{h,cpp}` — implement `exec::OutputBufferManager`.

**Modified (merge, if D1 in scope):**
- New: `velox/experimental/ucx-exchange/UcxMergeSource.{h,cpp}` — merge-source adapter over `UcxExchangeClient`.

**Modified (plan wiring):**
- `velox/experimental/cudf/exec/ToCudf.cpp`, `velox/experimental/cudf/CudfConfig.h` — parse `cudf.exchange`, call `registerUcxExchange()`.
- End-to-end test: `velox/experimental/cudf/tests/UcxExchangeEndToEndTest.cpp` (+ `tests/CMakeLists.txt`).

---

## Task Group A — Build layer

### Task A1: Put ucx-exchange in the build and compile it standalone

This is the prerequisite that has never been done: the module has never compiled in this configuration.

**Files:**
- Modify: `velox/CMakeLists.txt` (the `if(${VELOX_ENABLE_CUDF})` block, ~line 71-78)
- Modify: `velox/experimental/ucx-exchange/CMakeLists.txt` (link libs, ~line 52)

**Interfaces:**
- Produces: build target `velox_ucx_exchange` (OBJECT library) and, under `VELOX_BUILD_TESTING`, `ucx_exchange_test`.

- [ ] **Step 1: Add the subdirectory (reuse #17614 verbatim).** In `velox/CMakeLists.txt`, inside `if(${VELOX_ENABLE_CUDF})` right after `add_subdirectory(experimental/cudf)`:

```cmake
    # Build the experimental ucx-exchange library when a system UCX install
    # was found (UCX_FOUND is set while resolving the cudf dependency, which
    # also fetches ucxx).
    if(UCX_FOUND)
      add_subdirectory(experimental/ucx-exchange)
    endif()
```

- [ ] **Step 2: Add the nvtx3 link (reuse #17614).** In `velox/experimental/ucx-exchange/CMakeLists.txt`, change the PUBLIC link line:

```cmake
  PUBLIC ucxx::ucxx ucx::ucp nvtx3::nvtx3-cpp
```

(`UcxExchange`/`UcxPartitionedOutput` inherit `cudf_velox::NvtxHelper`, which needs the nvtx3 target.)

- [ ] **Step 3: Confirm UCX is present in the build image.** Run inside the build container: `ls /usr/include/ucp/api/ucp.h /usr/lib*/libucp.* 2>/dev/null; pkg-config --modversion ucx 2>/dev/null`. Expected: headers/libs present. If ABSENT, add to `scripts/docker/centos-multi.dockerfile` (reuse #17614) and rebuild the image:

```dockerfile
ARG UCX_VERSION
ENV UCX_VERSION=${UCX_VERSION:-1.20.1}

RUN bash /setup-centos-adapters.sh install_ucx && \
      dnf clean all
```

- [ ] **Step 4: Configure + build the module only.** Via `gpu_build_run.sh` with `TARGETS=velox_ucx_exchange`. Expected: `BUILD_OK`. This is the first real compile of the module against libcudf 26.08 / ucxx 0.51 — fix any header/API drift here (this is discovery work; record every fix in the report). Do NOT change the module's design, only what's needed to compile as-is.

- [ ] **Step 5: Build the module's own test.** `TARGETS=ucx_exchange_test`. Expected: `BUILD_OK`. (Running it is deferred to Task F; it needs a GPU + UCX runtime.)

- [ ] **Step 6: Commit.**

```bash
git add velox/CMakeLists.txt velox/experimental/ucx-exchange/CMakeLists.txt
git commit -m "build(ucx-exchange): Compile the ucx-exchange module under VELOX_ENABLE_CUDF"
```

---

## Task Group B — Receive side (UCX into ExchangeTransportRegistry)

### Task B1: `UcxExchangeClient` implements `exec::ExchangeClient`

**Files:**
- Modify: `velox/experimental/ucx-exchange/UcxExchangeClient.h` (class decl ~line 24, methods ~line 50-80)
- Modify: `velox/experimental/ucx-exchange/UcxExchangeClient.cpp` (matching definitions)

**Interfaces:**
- Consumes: `exec::ExchangeClient` (`velox/exec/ExchangeClient.h`) — pure-virtuals: `void addRemoteTaskId(const std::string&)`, `void noMoreRemoteTasks()`, `void close()`, `folly::F14FastMap<std::string, RuntimeMetric> stats()` (non-const), `folly::dynamic toJson() const`.
- Produces: `UcxExchangeClient` is-a `exec::ExchangeClient`; keeps its concrete data plane (`next()`, `queue()`) non-virtual for `UcxExchange`/merge to use after downcast.

- [ ] **Step 1: Write/extend the failing test.** In `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp`, add a test that holds a `UcxExchangeClient` through a base pointer and exercises the control plane:

```cpp
TEST_F(UcxExchangeTest, implementsAbstractExchangeClient) {
  std::shared_ptr<exec::ExchangeClient> client =
      std::make_shared<UcxExchangeClient>(
          "task.0", /*destination=*/0, /*numberOfConsumers=*/1);
  client->addRemoteTaskId("remote.0");
  client->noMoreRemoteTasks();
  EXPECT_NO_THROW(client->toJson());
  EXPECT_NO_THROW(client->stats());
  client->close();
}
```

- [ ] **Step 2: Build the test, verify it fails to compile.** `TARGETS=ucx_exchange_test`. Expected: compile error — `UcxExchangeClient` is not convertible to `exec::ExchangeClient` (no inheritance), and `addRemoteTaskId` takes `std::string_view`.

- [ ] **Step 3: Implement the interface.** In `UcxExchangeClient.h`:
  - `#include "velox/exec/ExchangeClient.h"`.
  - Change `class UcxExchangeClient : public std::enable_shared_from_this<UcxExchangeClient>` to also derive from `exec::ExchangeClient` (keep `enable_shared_from_this`): `class UcxExchangeClient : public exec::ExchangeClient, public std::enable_shared_from_this<UcxExchangeClient>`.
  - `addRemoteTaskId(std::string_view remoteTaskId)` → `addRemoteTaskId(const std::string& remoteTaskId) override`. Body unchanged (a `std::string` binds to any `string_view` use inside).
  - Add `override` to `noMoreRemoteTasks()`, `close()`, `toJson() const`.
  - `folly::F14FastMap<std::string, RuntimeMetric> stats() const` → drop `const`, add `override`: `folly::F14FastMap<std::string, RuntimeMetric> stats() override;`.
  - Keep `next()`, `queue()`, `requestDataSizesMaxWaitSec()`, `getRemoteTaskIdList()`, `toString()` exactly as-is (concrete data plane).
  In `UcxExchangeClient.cpp`, update the definitions to match the new signatures (parameter type, `const` removal).

- [ ] **Step 4: Build + run the test.** `TARGETS=ucx_exchange_test`, then run `--gtest_filter=UcxExchangeTest.implementsAbstractExchangeClient` on GPU. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add velox/experimental/ucx-exchange/UcxExchangeClient.h velox/experimental/ucx-exchange/UcxExchangeClient.cpp velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp
git commit -m "feat(ucx-exchange): UcxExchangeClient implements exec::ExchangeClient"
```

### Task B2: Register the `kUcx` receive-side transport entry

**Files:**
- Create: `velox/experimental/ucx-exchange/UcxExchangeRegistration.h`
- Create: `velox/experimental/ucx-exchange/UcxExchangeRegistration.cpp`
- Modify: `velox/experimental/ucx-exchange/CMakeLists.txt` (add the new `.cpp` to `UCX_EXCHANGE_SOURCES`)

**Interfaces:**
- Consumes: `ExchangeTransportRegistry::global()` and `ExchangeTransportEntry { makeClient; makeOperator }` (`velox/exec/ExchangeTransportRegistry.h`); `ExchangeClientContext` (`velox/exec/ExchangeFactory.h`); `UcxExchange` ctor `(int32_t, DriverCtx*, const std::shared_ptr<const core::PlanNode>&, std::shared_ptr<UcxExchangeClient>, std::string_view)`.
- Produces: `void registerUcxExchange();` — idempotent; inserts the `kUcx` entry into the receive registry (and, in Task C2, the output registry). Declared in `UcxExchangeRegistration.h`, namespace `facebook::velox::ucx_exchange`.

- [ ] **Step 1: Write the failing test.** In `UcxExchangeTest.cpp`:

```cpp
TEST_F(UcxExchangeTest, registersUcxReceiveTransport) {
  exec::ExchangeTransportRegistry::global().unregisterAll();
  EXPECT_EQ(
      exec::ExchangeTransportRegistry::global().tryGet(
          std::string{core::TransportKind::kUcx}),
      nullptr);
  registerUcxExchange();
  auto entry = exec::ExchangeTransportRegistry::global().tryGet(
      std::string{core::TransportKind::kUcx});
  ASSERT_NE(entry, nullptr);
  EXPECT_TRUE(static_cast<bool>(entry->makeClient));
  EXPECT_TRUE(static_cast<bool>(entry->makeOperator));
}
```

- [ ] **Step 2: Build, verify it fails.** Expected: `registerUcxExchange` undeclared.

- [ ] **Step 3: Implement `registerUcxExchange()`.** In `UcxExchangeRegistration.cpp`:

```cpp
#include "velox/experimental/ucx-exchange/UcxExchangeRegistration.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/experimental/ucx-exchange/UcxExchange.h"
#include "velox/experimental/ucx-exchange/UcxExchangeClient.h"

namespace facebook::velox::ucx_exchange {

void registerUcxExchange() {
  auto entry = std::make_shared<exec::ExchangeTransportEntry>();
  entry->makeClient =
      [](const exec::ExchangeClientContext& context)
      -> std::shared_ptr<exec::ExchangeClient> {
    return std::make_shared<UcxExchangeClient>(
        context.taskId, context.destination, context.numberOfConsumers);
  };
  entry->makeOperator =
      [](int32_t operatorId,
         exec::DriverCtx* ctx,
         const std::shared_ptr<const core::ExchangeNode>& node,
         std::shared_ptr<exec::ExchangeClient> client)
      -> std::unique_ptr<exec::Operator> {
    auto ucxClient = std::dynamic_pointer_cast<UcxExchangeClient>(client);
    VELOX_CHECK_NOT_NULL(
        ucxClient,
        "UCX exchange requires a UcxExchangeClient for transport: {}",
        core::TransportKind::kUcx);
    return std::make_unique<UcxExchange>(
        operatorId, ctx, node, std::move(ucxClient));
  };
  exec::ExchangeTransportRegistry::global().insert(
      std::string{core::TransportKind::kUcx}, std::move(entry));
}

} // namespace facebook::velox::ucx_exchange
```

Header `UcxExchangeRegistration.h` declares `/// Registers the UCX exchange (and output) transports under core::TransportKind::kUcx. Idempotent. void registerUcxExchange();`. Confirm the exact `ExchangeTransportEntry` field names and the `insert` signature against `ExchangeTransportRegistry.h` (mirror how `MultiFragmentTest.cpp:2140` already calls `insert`). `UcxExchange`'s ctor takes `const std::shared_ptr<const core::PlanNode>&`; a `core::ExchangeNode` converts implicitly.

- [ ] **Step 4: Add the source to CMake.** Add `UcxExchangeRegistration.cpp` to `UCX_EXCHANGE_SOURCES` in `velox/experimental/ucx-exchange/CMakeLists.txt`.

- [ ] **Step 5: Build + run.** `--gtest_filter=UcxExchangeTest.registersUcxReceiveTransport`. Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add velox/experimental/ucx-exchange/UcxExchangeRegistration.h velox/experimental/ucx-exchange/UcxExchangeRegistration.cpp velox/experimental/ucx-exchange/CMakeLists.txt velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp
git commit -m "feat(ucx-exchange): Register kUcx receive transport in ExchangeTransportRegistry"
```

### Task B3: Route `ucx_exchange_test` through the registry

**Files:**
- Modify: `velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp` (the cases that construct `UcxExchange`/`UcxExchangeClient` directly)
- Modify: `velox/experimental/ucx-exchange/tests/SinkDriverMock.cpp` (constructs client/operator directly)

**Interfaces:**
- Consumes: `registerUcxExchange()` (B2), `ExchangeTransportRegistry::global().tryGet(...)`.

- [ ] **Step 1: Register in fixture setup.** In `UcxExchangeTest`'s `SetUp()` call `registerUcxExchange();` and in `TearDown()` call `exec::ExchangeTransportRegistry::global().unregisterAll();`. (Do the same in `SinkDriverMock`'s setup if it owns its own harness.)

- [ ] **Step 2: Replace direct construction with resolution.** At each site that does `std::make_shared<UcxExchangeClient>(...)` / `std::make_unique<UcxExchange>(...)` for the path under test, resolve via the registry: `auto entry = ExchangeTransportRegistry::global().tryGet(std::string{core::TransportKind::kUcx});` then `entry->makeClient(context)` / `entry->makeOperator(...)`. Leave any case whose intent is to unit-test the concrete class directly (document which, if any, you keep direct and why in the report).

- [ ] **Step 3: Build + run the full UCX suite.** `TARGETS=ucx_exchange_test`, run all cases on GPU. Expected: PASS (same set as before, now through the registry).

- [ ] **Step 4: Commit.**

```bash
git add velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp velox/experimental/ucx-exchange/tests/SinkDriverMock.cpp
git commit -m "test(ucx-exchange): Resolve exchange client/operator via registry"
```

---

## Task Group C — Output side (UCX into OutputTransportRegistry)

### Task C1: `UcxOutputQueueManager` implements `exec::OutputBufferManager`

Reuse #17614's `UcxOutputQueueManager` diff as the reference for which methods change — but match the **PoC** `OutputBufferManager` signatures (from `velox/exec/OutputBufferManager.h`), which differ from #17614's `IOutputBufferManager` (PoC's `getUtilization`/`isOverutilized` return `std::optional<...>`; `initializeTask` takes a `std::shared_ptr<Task>` and a trailing `const std::string& transportOptions = {}`).

**Files:**
- Modify: `velox/experimental/ucx-exchange/UcxOutputQueueManager.h`
- Modify: `velox/experimental/ucx-exchange/UcxOutputQueueManager.cpp`

**Interfaces:**
- Consumes: `exec::OutputBufferManager` pure-virtuals (exact signatures):
  - `void initializeTask(std::shared_ptr<Task> task, core::PartitionedOutputNode::Kind kind, int numDestinations, int numDrivers, const std::string& transportOptions = {})`
  - `bool updateOutputBuffers(const std::string& taskId, int numDestinations, bool noMoreBuffers)`
  - `bool updateNumDrivers(const std::string& taskId, uint32_t newNumDrivers)`
  - `void removeTask(const std::string& taskId)`
  - `std::optional<OutputBufferStats> stats(const std::string& taskId)`
  - `std::optional<double> getUtilization(const std::string& taskId)`
  - `std::optional<bool> isOverutilized(const std::string& taskId)`
  - `std::string toString(const std::string& taskId)`
- Produces: `UcxOutputQueueManager` is-a `exec::OutputBufferManager`, registrable as the `manager` in an `OutputTransportEntry`.

- [ ] **Step 1: Write the failing test.** In `velox/experimental/ucx-exchange/tests/UcxOutputQueueManagerTest.cpp`, hold the manager through the base pointer and drive the lifecycle:

```cpp
TEST_F(UcxOutputQueueManagerTest, implementsOutputBufferManager) {
  std::shared_ptr<exec::OutputBufferManager> mgr =
      UcxOutputQueueManager::getInstanceRef();
  ASSERT_NE(mgr, nullptr);
  // Drive through the abstract surface; concrete assertions per method below.
  EXPECT_NO_THROW(mgr->toString("nonexistent.task"));
  EXPECT_FALSE(mgr->updateNumDrivers("nonexistent.task", 1));
}
```

- [ ] **Step 2: Build, verify failure.** Expected: `getInstanceRef()` return type not convertible to `exec::OutputBufferManager` (no inheritance); missing methods.

- [ ] **Step 3: Implement.** In `UcxOutputQueueManager.h`:
  - `#include "velox/exec/OutputBufferManager.h"`.
  - `class UcxOutputQueueManager : public exec::OutputBufferManager`.
  - Align existing methods to the PoC signatures (parameter `std::string_view` → `const std::string&`; add the `std::shared_ptr<Task> task` and trailing `transportOptions` params to `initializeTask` if not already present; `updateOutputBuffers` returns `bool`; `stats` returns `std::optional<OutputBufferStats>`), each `override`.
  - Add the four methods #17614 added — `updateNumDrivers`, `getUtilization`, `isOverutilized`, `toString` — but with the PoC return types (`bool`, `std::optional<double>`, `std::optional<bool>`, `std::string`).
  In `.cpp`, provide bodies: reuse #17614's implementations as the reference; for methods UCX has no meaningful notion of, return the neutral value consistent with the interface contract documented in `OutputBufferManager.h` (`getUtilization`/`isOverutilized` → `std::nullopt` when the transport has no bounded capacity/threshold; `toString` → a short state dump; `updateNumDrivers` → `false` for unknown task). Document each non-trivial mapping in the report.
  - Note the `OutputBufferStats` vs #17614's `OutputBuffer::Stats` type name — use whatever `OutputBufferManager.h` declares (`std::optional<OutputBufferStats>`); map UCX queue stats onto it.

- [ ] **Step 4: Build + run.** `TARGETS=ucx_exchange_test` (or the specific output-queue-manager test target), run on GPU. Expected: PASS. Also confirm the existing UcxOutputQueueManager tests still pass (signature changes are source-compatible for internal callers, or update them).

- [ ] **Step 5: Commit.**

```bash
git add velox/experimental/ucx-exchange/UcxOutputQueueManager.h velox/experimental/ucx-exchange/UcxOutputQueueManager.cpp velox/experimental/ucx-exchange/tests/UcxOutputQueueManagerTest.cpp
git commit -m "feat(ucx-exchange): UcxOutputQueueManager implements exec::OutputBufferManager"
```

### Task C2: Register the `kUcx` output-side transport entry

**Files:**
- Modify: `velox/experimental/ucx-exchange/UcxExchangeRegistration.cpp` (extend `registerUcxExchange()`)

**Interfaces:**
- Consumes: `OutputTransportRegistry::global()`, `OutputTransportEntry` and its `make(manager, PartitionedOutputFactory)` helper (`velox/exec/OutputTransportRegistry.h`); `PartitionedOutputFactory` `(int32_t, DriverCtx*, const std::shared_ptr<const core::PartitionedOutputNode>&, bool eagerFlush)`; `UcxPartitionedOutput` ctor `(int32_t, DriverCtx*, const std::shared_ptr<const core::PartitionedOutputNode>&, bool eagerFlush)` (already matches).
- Produces: `registerUcxExchange()` now also seeds the output registry.

- [ ] **Step 1: Write the failing test.** In `UcxOutputQueueManagerTest.cpp` (or a new registration test):

```cpp
TEST_F(UcxOutputQueueManagerTest, registersUcxOutputTransport) {
  exec::OutputTransportRegistry::global().unregisterAll();
  registerUcxExchange();
  auto entry = exec::OutputTransportRegistry::global().tryGet(
      std::string{core::TransportKind::kUcx});
  ASSERT_NE(entry, nullptr);
  EXPECT_NE(entry->manager, nullptr);
  EXPECT_TRUE(static_cast<bool>(entry->makeOutputOperator));
}
```

- [ ] **Step 2: Build, verify failure** (no `kUcx` output entry yet).

- [ ] **Step 3: Extend `registerUcxExchange()`** — append after the receive-registry insert:

```cpp
  auto manager = UcxOutputQueueManager::getInstanceRef();
  exec::OutputTransportRegistry::global().insert(
      std::string{core::TransportKind::kUcx},
      exec::OutputTransportEntry::make(
          manager,
          [](int32_t operatorId,
             exec::DriverCtx* ctx,
             const std::shared_ptr<const core::PartitionedOutputNode>& node,
             bool eagerFlush) -> std::unique_ptr<exec::Operator> {
            return std::make_unique<UcxPartitionedOutput>(
                operatorId, ctx, node, eagerFlush);
          }));
```

Verify `OutputTransportEntry::make`'s exact signature and the weak-capture semantics in `OutputTransportRegistry.h` (the manager is captured weakly; `getInstanceRef()` must keep a strong ref alive — it is a process singleton, so this holds). Add includes for `UcxPartitionedOutput.h`, `OutputTransportRegistry.h`, `UcxOutputQueueManager.h`.

- [ ] **Step 4: Build + run.** `--gtest_filter=*registersUcxOutputTransport`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add velox/experimental/ucx-exchange/UcxExchangeRegistration.cpp velox/experimental/ucx-exchange/tests/UcxOutputQueueManagerTest.cpp
git commit -m "feat(ucx-exchange): Register kUcx output transport in OutputTransportRegistry"
```

---

## Task Group D — MergeExchange over UCX

> If decision (c) from "Design decisions" is chosen (merge stays in-memory-only), skip D1 and instead add a test asserting a `kUcx` `MergeExchangeNode` fails fast with the existing downcast message; document the limitation in `POC-README.md`.

### Task D1: UCX merge-source adapter

**Files:**
- Create: `velox/experimental/ucx-exchange/UcxMergeSource.h`
- Create: `velox/experimental/ucx-exchange/UcxMergeSource.cpp`
- Modify: `velox/exec/Merge.cpp` (`addMergeSources`, ~line 838-865) — resolve the source by transport instead of unconditionally downcasting to `InMemoryExchangeClient`.

**Interfaces:**
- Consumes: `UcxExchangeClient::next(int consumerId, bool* atEnd, ContinueFuture*)`; the `MergeSource` interface that `MergeExchangeSource` implements (see `velox/exec/MergeSource.h`).
- Produces: a merge source that reads UCX packed tables and yields `RowVectorPtr`, selected in `Merge.cpp` when `transportKind_ == kUcx`.

- [ ] **Step 1: Write the failing test.** Add a merge-over-UCX case to `UcxExchangeTest.cpp` (or the merge test harness) that builds a `MergeExchangeNode` with `transportKind = kUcx`, feeds two ordered UCX sources, and asserts the merged output is globally ordered. (Model it on the existing in-memory `mergeExchange` test.)

- [ ] **Step 2: Build, verify failure** — current `Merge.cpp` downcasts to `InMemoryExchangeClient` and fails fast for `kUcx`.

- [ ] **Step 3: Implement the adapter + selection.** In `UcxMergeSource`, wrap a `std::shared_ptr<UcxExchangeClient>` and implement the `MergeSource` read path by converting `UcxExchangeClient::next()` packed tables to `RowVectorPtr` (reuse `UcxExchange::getOutputFromPackedTable`'s conversion — factor it into a shared helper if needed, without changing `UcxExchange`'s behavior). In `Merge.cpp`, after `ExchangeTransportRegistry::tryGet(queryCtx, transportKind_)` builds the client, branch on `transportKind_`: for `kInMemory` keep the existing `InMemoryExchangeClient` path; for `kUcx` downcast to `UcxExchangeClient` and build `UcxMergeSource`. Keep the fail-fast for any other kind.

- [ ] **Step 4: Build + run** the merge-over-UCX test on GPU, and re-run the in-memory merge regression (`mergeExchange`, `abortMergeExchange`, `mergeExchangeWithSpill`, `mergeExchangeOverEmptySources`) to prove the default path is unchanged. Expected: all PASS.

- [ ] **Step 5: Commit.**

```bash
git add velox/experimental/ucx-exchange/UcxMergeSource.h velox/experimental/ucx-exchange/UcxMergeSource.cpp velox/exec/Merge.cpp velox/experimental/ucx-exchange/tests/UcxExchangeTest.cpp
git commit -m "feat(ucx-exchange): MergeExchange over UCX via a merge-source adapter"
```

---

## Task Group E — Plan wiring and registration site

### Task E1: Enable UCX per query via `cudf.exchange` and select it on plan nodes

**Files:**
- Modify: `velox/experimental/cudf/exec/ToCudf.cpp` (call `registerUcxExchange()` when enabled; parse the session config)
- Modify: `velox/experimental/cudf/CudfConfig.h` (add the `exchange` flag if not already present — check first)
- Modify: `velox/experimental/cudf/exec/CMakeLists.txt` (already covered: `velox_ucx_exchange` link from Task A; confirm present)

**Interfaces:**
- Consumes: `registerUcxExchange()` (B2/C2); `CudfConfig::exchange`.
- Produces: on a worker with `cudf.exchange=true`, both `kUcx` entries are registered; plan nodes whose `transportKind == kUcx` resolve to UCX. `PlanBuilder::exchange(...)/mergeExchange(...)` already accept an optional `transportKind` (from the PoC) and serde is already backward-compatible — no plan-node code changes needed.

- [ ] **Step 1: Confirm the config knob.** Grep `velox/experimental/cudf/CudfConfig.h` for an `exchange` flag and the `cudf.exchange` session key (reuse #17614's parsing in `ToCudf.cpp` — `cudf.exchange`, `ucxx.*`, `cudf.exchange_log_level`). Add the flag only if absent, matching #17614's names.

- [ ] **Step 2: Write the failing test.** A cuDF-exec test that, with `cudf.exchange=true`, calls the ToCudf registration entry point and asserts `ExchangeTransportRegistry::global().tryGet(kUcx)` and `OutputTransportRegistry::global().tryGet(kUcx)` are both non-null; and with `cudf.exchange` unset, that only `kInMemory` is registered (UCX absent → a `kUcx` node would fail fast).

- [ ] **Step 3: Implement the call site.** In `ToCudf.cpp`, when `CudfConfig::exchange` is set on the worker, call `ucx_exchange::registerUcxExchange();` (once; idempotent). Do not register when it is unset — this preserves the pure-HTTP default.

- [ ] **Step 4: Build + run.** Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add velox/experimental/cudf/exec/ToCudf.cpp velox/experimental/cudf/CudfConfig.h
git commit -m "feat(cudf): Register UCX transports when cudf.exchange is enabled"
```

---

## Task Group F — End-to-end run

### Task F1: End-to-end UCX shuffle + HTTP regression

**Files:**
- Create: `velox/experimental/cudf/tests/UcxExchangeEndToEndTest.cpp`
- Modify: `velox/experimental/cudf/tests/CMakeLists.txt` (new `velox_add_cudf_test`, reuse #17614's block shape — `LIBS ... velox_ucx_exchange velox_exec_test_lib velox_test_util`, `TIMEOUT 3000`)

**Interfaces:**
- Consumes: everything above; runs a two-fragment plan (PartitionedOutput → Exchange) with `transportKind = kUcx` on both the producing `PartitionedOutputNode` and consuming `ExchangeNode`, and `cudf.exchange=true`.

- [ ] **Step 1: Write the end-to-end test.** Build a producer task with a `kUcx` `PartitionedOutputNode` and a consumer task with a `kUcx` `ExchangeNode` (set via `PlanBuilder`), run them on GPU with `cudf.enabled=true, cudf.exchange=true`, and assert the received rows equal the produced rows. Add a twin case with the default transport (no `transportKind`) asserting the HTTP/in-memory path still produces identical results.

- [ ] **Step 2: Add the test target** to `velox/experimental/cudf/tests/CMakeLists.txt`:

```cmake
velox_add_cudf_test(
  NAME velox_cudf_ucx_exchange_e2e_test
  SOURCES Main.cpp UcxExchangeEndToEndTest.cpp
  LIBS
    ${CUDF_TEST_DEFAULT_LIBS}
    velox_ucx_exchange
    velox_vector_fuzzer
    velox_exec_test_lib
    velox_test_util
  TIMEOUT 3000
)
```

- [ ] **Step 3: Build + run on GPU.** `gpu_build_run.sh` with `--gpus all`, `TARGETS=velox_cudf_ucx_exchange_e2e_test`, run with cuDF exchange enabled. Expected: both cases PASS.

- [ ] **Step 4: Full regression.** Rebuild and run the full `velox/exec` suite (`velox_exec_test_group0..group8`, `velox_exec_util_test_group0`, `velox_exec_infra_test`, `velox_exchange_transport_registry_test`) to prove the default path is unregressed. Expected: all PASS.

- [ ] **Step 5: Update `POC-README.md`.** Replace the "Outlook" section's future-tense items that are now done with a short "Integrated" summary; keep any remaining limitations (e.g. merge-over-UCX if deferred).

- [ ] **Step 6: Commit.**

```bash
git add velox/experimental/cudf/tests/UcxExchangeEndToEndTest.cpp velox/experimental/cudf/tests/CMakeLists.txt POC-README.md
git commit -m "test(cudf): End-to-end UCX exchange over the transport registry"
```

---

## Self-review notes

- **Spec coverage:** A (build) → Task Group A; B (receive) → B1-B3; C (output) → C1-C2; D (merge) → D1; E (plan wiring/selection) → E1; F (run) → F1. All six covered.
- **Type consistency:** `registerUcxExchange()` is defined once (B2), extended (C2), and called (E1) — single name throughout. `ExchangeTransportEntry {makeClient, makeOperator}`, `OutputTransportEntry {manager, makeOutputOperator}` + `make()`, and the two factory typedefs match the headers read during planning. Confirm the two `insert` signatures and `OutputTransportEntry::make` against the registry headers at implementation time (flagged in-task).
- **Known verification points deferred to implementers (flagged in-task, not placeholders):** exact `OutputBufferStats` type name; whether `CudfConfig::exchange` already exists; whether the build image already ships UCX; the precise `insert`/`make` signatures. Each task says what to confirm and where.
