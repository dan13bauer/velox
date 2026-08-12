/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "velox/common/ScopedRegistry.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/ExchangeFactory.h"

namespace facebook::velox::core {
class QueryCtx;
} // namespace facebook::velox::core

namespace facebook::velox::exec {

/// Registry value pairing the factory that builds a transport's exchange client
/// with the factory that builds its matching exchange operator, keyed by
/// transport id. Registering the two together keeps a transport's client and
/// operator from diverging. Plain aggregate: unlike the output side there is no
/// long-lived manager instance to hold -- the client is created per node by
/// Task.
struct ExchangeTransportEntry {
  ExchangeClientFactory makeClient;
  ExchangeFactory makeOperator;
};

/// Manages exchange-transport registration and lookup, keyed by transport id.
/// Each entry pairs the factory that builds a transport's exchange client with
/// the factory that builds its matching exchange operator. All methods are
/// thread-safe.
///
/// Two groups of APIs:
///
/// - Query-scoped APIs take a QueryCtx& and check for per-query registry
///   overrides before falling back to the global registry. Use these in
///   operator and task code where a QueryCtx is available.
///
/// - Global APIs operate directly on the global registry. Use these for
///   process-level operations: startup registration, shutdown cleanup, and
///   process-wide lookups.
class ExchangeTransportRegistry {
 public:
  using Registry = ScopedRegistry<std::string, ExchangeTransportEntry>;

  /// Registry key for per-query exchange transport overrides on QueryCtx.
  static constexpr std::string_view kRegistryKey = "exchangeTransports";

  /// Returns the global registry (root scope).
  static Registry& global();

  /// Creates a per-query registry. If 'parent' is provided, lookups fall back
  /// to it. Pass nullptr for isolation mode (no fallback).
  static std::shared_ptr<Registry> create(const Registry* parent = nullptr);

  /// Returns the transport entry registered under 'id' for 'queryCtx'
  /// (per-query override, then global registry), or nullptr.
  static std::shared_ptr<ExchangeTransportEntry> tryGet(
      const core::QueryCtx& queryCtx,
      const std::string& id);

  /// Returns the transport entry registered under 'id' in the global registry,
  /// or nullptr. Ignores per-query overrides; use the QueryCtx overload to
  /// honor them.
  static std::shared_ptr<ExchangeTransportEntry> tryGet(const std::string& id);

  /// Returns all transports visible to 'queryCtx' as (id, entry) pairs
  /// (per-query override merged over the global registry).
  static std::vector<
      std::pair<std::string, std::shared_ptr<ExchangeTransportEntry>>>
  getAll(const core::QueryCtx& queryCtx);

  /// Returns all registered transports from the global registry, as
  /// (id, entry) pairs.
  static std::vector<
      std::pair<std::string, std::shared_ptr<ExchangeTransportEntry>>>
  getAll();

  /// Clears the per-query transport overrides; global registrations remain.
  static void unregisterAll(const core::QueryCtx& queryCtx);

  /// Clears all registered transports, keeping the built-in in-memory default.
  static void unregisterAll();

 private:
  /// Returns the (id, entry) pairs visible to 'queryCtx' -- the per-query
  /// override merged with the global registry. Backs the QueryCtx-scoped
  /// getAll().
  static std::vector<
      std::pair<std::string, std::shared_ptr<ExchangeTransportEntry>>>
  snapshot(const core::QueryCtx& queryCtx);
};

} // namespace facebook::velox::exec
