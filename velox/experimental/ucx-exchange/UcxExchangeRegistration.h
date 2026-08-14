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

namespace facebook::velox::ucx_exchange {

/// Registers the UCX exchange (and output) transports under
/// core::TransportKind::kUcx. Idempotent.
void registerUcxExchange();

/// Removes both kUcx entries, undoing registerUcxExchange(). Idempotent, and a
/// no-op when they were never registered. Leaves other transports alone,
/// including the built-in in-memory default.
void unregisterUcxExchange();

} // namespace facebook::velox::ucx_exchange
