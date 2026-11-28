# UCX Exchange: One State Machine on Each Side of a Channel (Summary)

**Status:** proposal for discussion.
**Scope:** `velox/experimental/ucx-exchange`.
**Detailed version:** [ucx-exchange-channel-lifecycle.md](ucx-exchange-channel-lifecycle.md).

## Summary

Many fixes to the UCX exchange share one shape. A channel, the link from one
producer task's destination to one consumer, ends in a way that one component
handles, while another with its own copy of the channel's state never hears
about it. A consumer then waits until its query is aborted,
finishes with rows missing, or keeps GPU memory.

A channel spans two workers, so no structure on one worker can hold all of
it. This note proposes one design: each side of a channel gets one state
machine, kept in the object that already holds that side's task-wide state
under one lock. Protocol messages are the only transitions between the
two sides. The server and the source keep only their UCX progress. The protocol
gets a version and changes without backward compatibility; workers of
different versions never crash, and the query fails with a message instead.
The design carries the fixes of the open PRs #19047 and #19701, in one
change.

## Background

The **current design** is the upstream code at the time of writing. The
proposal starts from it.

### UCX in brief

UCXX is the C++ wrapper of UCX that the exchange uses. A **UCX worker**
progresses communication; an **endpoint** connects two UCX workers. Messages
carry a 64-bit **tag**, and a message that arrives before its receive is
posted waits in the UCX worker's queue of **unexpected messages**. UCX reports
a failed peer only with peer error handling (`ucxx.error_handling`, on by
default); some fabrics require it off.

### Terms

- A **worker** is a process that runs Velox tasks, such as a Prestissimo
  worker. The **coordinator** schedules tasks and aborts a query's tasks when
  it finishes or fails.
- A **producer task**'s `UcxPartitionedOutput` writes **chunks** (today cuDF
  packed tables) into its **output queue** (`UcxOutputQueue`), which holds
  one **destination** queue per consumer under one lock. The **end marker**
  ends a destination. The **output queue manager** (`UcxOutputQueueManager`)
  holds a worker's output queues and implements `exec::OutputBufferManager`.
- A **consumer task** owns a `UcxExchangeClient`, which owns one **source**
  (`UcxExchangeSource`) per producer and one **exchange queue**
  (`UcxExchangeQueue`) for the tables they receive.
- For each source, the producer's worker creates a **server**
  (`UcxExchangeServer`), which sends each chunk as a metadata and a data
  message.
- The **Communicator**, one per worker, runs the event loop that drives UCX
  and owns the endpoints, which channels between two workers share.
- A **placeholder** is an output queue created for a server that asks for a
  task before the task has initialized.
- A **channel** is one producer task and destination, with its server and
  source. Its **producer side** is the destination queue and the server; its
  **consumer side**, the source and its tables. Its **outcome** is a clean end
  or an error. Each destination has exactly one channel, and a source never
  reconnects.
### How a channel opens

The consumer opens the channel, which is identified by the producer's task ID
and destination. The source connects an endpoint to the producer's worker, or
reuses one, and sends the handshake as an active message: the task ID, the
destination and the worker ID, and in the design also the protocol version and
its channel ID. The producer's Acceptor binds a server to the destination,
creating a placeholder if needed, and responds.

### Who touches a channel's state

Eight components, the ones above and the intra-node registry, hold part of a
channel's state as queues, flags, state machines and side maps keyed by task
ID. Five kinds of threads change it: producer drivers, consumer drivers, the
Communicator thread, the thread that terminates a task, and the worker's
request threads.

## The problem

### The pattern in the fixes

Each fix found components with different views of one channel:

- #17752: closing one `UcxExchange` closed a client its siblings still used
  (the operator vs. the shared client).
- #17753 and #18921: a consumer that connected while or before its producer
  initialized missed the intra-node path (queue initialization, the Acceptor
  and the placeholder).
- #19047: a consumer that closed early left its producer `RUNNING` (the
  source, the server and the output queue).
- #19064: a broadcast reader that arrived before its destination was announced
  waited forever (the reader and the output queue).
- #19701: a failed producer ended its channels cleanly, so a query returned 0
  rows (the output queue, the server and the intra-node registry). Its review
  found three more races of this kind in its own drafts.
- #19859: producer and consumer backpressure counted different things (the
  queues, the server and the consumer).
- Found for this note: a table that arrives during a consumer's close stays
  in the closed queue, and a failed send leaves its producer `RUNNING`.

### Root causes

**No component owns a channel's ending.** A channel ends in at least six
ways: every producer driver calls `noMoreData()`; the producer task is
removed; the destination's results are deleted; the consumer closes; the
connection fails; or, on the intra-node path, a published chunk is never
read. Each event reaches some components and not others, and three on each
side decide part of the ending. The endings are not written down, so each new
one needs a fix at every boundary it crosses.

**Task lifetime clashes with UCX connection lifetime.** A consumer can
connect before its producer exists or long after it is gone, linked only by
the task ID, through placeholders and records of removed tasks in side maps.

**The protocol carries little of the lifecycle.** The current wire format
cannot say why a channel ended, has no message for a consumer that leaves
early, and aborts the worker on a version mismatch. State has to be inferred:
an unknown task ID means "not created yet", a silent consumer "still
reading".

**Two transports double the cases.** Two of the three races in #19701's
drafts exist only on the intra-node path.

**Endings are task-wide, but only the consumer decides them under one lock.**
Whether a removed producer ends cleanly depends on all its destinations; a
consumer fails when any source fails. With #19701, the producer reads its
output queue, releases the lock and retries under the manager's lock, and two
races came from those gaps.

## Goals

- Every reader gets exactly one outcome: a chunk, a clean end, or an error that
  says why the channel ended.
- Each side of a channel decides its outcome once, in one place, and the
  protocol carries that decision to the other side.
- Every way a channel can end is listed in one table per side, with one test
  each.
- No state outlives what needs it: a removed task's output queue is dropped
  once its `Task` is gone, an unclaimed Pending one after a timeout, and a
  channel's state ends with its last participant.
- Each side can be tested on its own, without UCX.
- A worker never crashes on a peer of another protocol version; the query
  fails with a message that names both versions.

## Requirements

- No change to the Velox core interfaces (`exec::OutputBufferManager`,
  `exec::ExchangeClient`).
- No change to the data encoding: `cudf::packed_columns` and its cuDF
  metadata stay as they are. Only the exchange's own messages change.
- No backward compatibility: the exchange's messages may change in the most
  efficient way, provided that workers of another version fail gracefully
  (see Goals). A cluster runs one version.
- UCX and UCXX remain the transport.
- The data path stays zero-copy, with no new copies or synchronization per
  chunk.
- No thread blocks on a peer. The one exception: with `ucxx.error_handling`
  off, closing an endpoint flushes it on the Communicator thread and can wait
  on an unresponsive peer without limit. A close that does not block is
  follow-up work, together with a per-channel timeout.

The proposal inherits one limit of UCX. With `ucxx.error_handling` off, a
failed peer is never reported, so a channel to that peer ends only when one of
its tasks ends. Error handling off is not a supported configuration but a
workaround for fabrics where peer error handling does not work today; the
design keeps working with it.

## Proposal

### Starting point

The proposal goes from the current design to the design in one change, which
replaces #19047 and #19701 and takes their fixes:

- from #19047: a producer finishes only after the send of its last message
  completed; a close message for a consumer that leaves early; each server
  bound to its output queue once;
- from #19701: the producer's error in the last message of a channel, and a
  consumer that fails when its endpoint closes or a message cannot be decoded.

Both PRs had to keep the current wire format, so #19701 adds a capability flag
for the close message and older consumers still cannot see the producer's
error. The design versions the protocol instead. Where the two PRs disagree,
it follows #19701 on removal, so a removed producer with undelivered output
sends its error, and #19047 on a consumer that closes before its producer
initialized, so the release is remembered and the producer can finish. The
intra-node path is out of scope and is to be removed separately. The design
takes fixes and tests from #19047 at `5dd89b66a5` and from #19701 at
`09c4707e92`.

### Where each side's state lives

One state per destination goes inside the output queue, and one state per
source inside the exchange queue. Every decision that ends a channel is taken
under that object's lock, so the producer decides removal in one step instead
of reading, releasing and retrying. Velox's CPU exchange makes the same
choice: one lock per task's output buffer, over its destination buffers.

### The producer side

The output queue manager keeps one map from task ID to `UcxOutputQueue`,
whose state is Pending (a server or a close message named the task before it
initialized), Active or Removed. It replaces the current design's `queues_`
and `removedTasks_`, whose late readers get a clean end, and the
`failedInitializations_` that #19701 would add. The task's
output queue stays the same object from Pending to Removed: removal frees its
chunks and keeps only a `std::weak_ptr` to the `Task` and its outcome. The
manager's lock only adds and erases output queues. Each destination is
Unpublished, Open, Ended(outcome) or Released.

A close for a task the producer's worker does not know creates a Pending
output queue with that destination Released; a server whose handshake names a
Released destination gets `kStop` and ends without sending.
A Pending output queue that no task claims needs a timeout: in Prestissimo, a
DELETE that arrives before the CREATE means that `initializeTask()` and
`removeTask()` are never called for the task. One sweep, in `initializeTask()`
only, drops the output queues with no bound server that are Removed with
their `Task` gone, or Pending for more than 60 seconds, but never the output
queue of the task being initialized. The timeout is a constructor parameter
of `UcxOutputQueueManager`.

The producer's outcome table gives, for every pair of state and event, the
next state and the outcome. The first decision for a destination wins. The
endings fall into four groups:

- **Clean end:** the server takes the end marker, or the task is removed after
  all drivers finished with nothing left to deliver.
- **Producer error:** the task is removed with output undelivered, never
  initialized, or discarded its output after running out of GPU memory. The
  server sends the error in the end message, also to a consumer that connects
  to the Removed task.
- **Consumer gone:** the close message arrives or the endpoint closes. The
  destination is Released and its results are deleted, so the producer can
  finish.
- **Producer cannot deliver:** a send fails while the endpoint stays open, or
  the server fails. The producer task fails, the coordinator aborts the
  query, and removal ends the remaining consumers with that error.

A late event, such as a removal after the consumer closed, changes nothing:
the first decision stands. An impossible pair, such as publishing a
destination twice, changes nothing either, and a debug check (`VELOX_DCHECK`)
names it. A chunk enqueued into a destination that is not Open is freed at
once and not counted toward backpressure, so a producer after `LIMIT` does
not block on a gone consumer.

Removal counts the chunks on the intra-node path in the output queue, under
its lock, and never reads the registry; the count can only turn a clean end
into an error.

The server keeps its UCX progress and reports three events to the output
queue: the close message, endpoint closure, and a failed send or server.

### The consumer side

The exchange queue keeps one state per source: Registered, Open,
Ended(outcome) or Released. Its outcome table has the same form and covers
every pair; every pair it does not decide is a late event. The endings fall
into three groups, and again the first decision wins:

- **Clean end:** the end message, without an error.
- **Error:** the end message carries the producer's error; the handshake
  response has another protocol version; a connect, handshake, receive,
  decode or allocation fails; or the endpoint closes. The consumer fails.
- **Closed:** the client closes. The source is Released and, if the channel
  has not ended, sends the close message at once, also during the handshake.
  A source added after the close still connects to tell its server.

A source with no handshake response after
`cudf.exchange_handshake_timeout_ms` (30 s) fails and sends its close. A close
to a producer of version 0 stays unmatched in that producer's UCX worker; it
happens only during a rolling upgrade, whose spanning queries fail anyway.

The consumer has finished when every source has Ended or been Released, and
fails when any source ends with an error. Once it has failed or closed, the
exchange queue frees any table that still arrives. A close and an end that
race are settled by the source's state: only one is counted.

### Between the sides: the protocol

| Message | Producer side | Consumer side |
|---|---|---|
| handshake, with version and channel ID | binds a server to a Pending, Active or Removed output queue, if the versions match | Registered |
| handshake response, with version | — | Open if the versions match; otherwise Ended with a version error |
| metadata and data messages | Open | Open |
| end message, with the outcome and the producer's error, if any | Ended | Ended, with the same outcome |
| close message | Released | Released |
| endpoint closed | Released | Ended with an error, unless already Ended or Released |

- **One protocol version, no flags.** The handshake and its response carry a
  version, which every wire-format change increments; each side accepts only
  its own. The current design is version 0; the design defines version 1.
- **Versions 0 and 1 never exchange handshakes**, since the active message
  that sets up a channel is where the incompatibility is detected. Version 1
  uses a new active-message ID, which a current producer drops; the new source
  fails after the handshake timeout. Under the old ID, version 1 keeps only a
  handler that rejects: it creates no server, fails the producer task if
  Active, and its 16-byte response fails the current source's 8-byte receive
  with a truncation error.
- **A fixed prefix from version 1 on:** the active-message ID, the version at
  offset 0 and the channel ID at offset 4 of the handshake, and the 16-byte
  response on the channel-ID tag with the version at offset 8. Two versions
  from 1 on reject each other at once, naming both.
- **Tags unique per consumer worker.** UCX matches tagged receives per UCX
  worker, so the consumer's Communicator gives each source a 32-bit channel ID
  unique among its live sources. The server's tags use it instead of the
  current design's hash.
- **An end message** replaces the metadata message with `atEnd`.
- **One close message and one close receive per worker.** The Communicator
  hands each close to the output queue of the task it names, which releases
  the destination even if its server has ended or does not exist yet. With a
  receive per server, as in #19047 and #19701, such a close stays in UCX for
  the life of the worker.
- **Control messages.** The close is the first control message: a header
  with type and length, then a 256-byte task ID and a 4-byte destination,
  tagged with the operation byte `0x05`, which the per-worker receive matches
  alone. Later types, such as a credit grant, come with a new protocol
  version. A message of unknown type or wrong length is logged and dropped.
- **No check that aborts the worker.** A message that cannot be decoded fails
  its channel, instead of failing a `VELOX_CHECK` inside a UCX callback.

### Rules for both sides

- Callbacks run, and freed chunks and tables are released, after the queue's
  lock is released.
- No `Task` method runs, and no `Task` lock is taken, while the output queue's
  lock is held: removal reads the task's final state and error first.
- `Task::setError()` never runs on the Communicator thread, whose teardown
  work would stall every exchange on the worker. It is posted to the task's
  executor, `task->queryCtx()->executor()`.
- The output queue and the exchange queue treat chunks and tables as opaque,
  keep no memory accounting of their own (#18590 charges chunks to Velox's
  memory pools), and free chunks when a channel is released or fails. No state
  that outlives a channel holds a chunk.
- Each table is one function from state and event to the next state and
  outcome, tested per row and per pair.

### What changes where

| Component | What changes | What it fixes |
|---|---|---|
| `UcxOutputQueueManager` | one map of output queues; the sweep | side maps that outlive their task; a placeholder created during removal |
| `UcxOutputQueue` | destination states; removal in one step | the races of removal in steps |
| `UcxExchangeServer` | UCX progress only; the end message | a failed producer's clean end; a producer left `RUNNING` after a failed send |
| `UcxExchangeQueue` | source states; frees late tables | tables that hold GPU memory after a close or failure |
| `UcxExchangeSource`, `UcxExchangeClient` | one `end(outcome)`; the close sent at once; closed state as Released sources | the close and end races; a consumer that leaves silently |
| `Communicator` | one control-message receive; channel IDs | a close lost after its server ended; shared tag hashes |
| `Acceptor` | version check; reject-only old handler | a version mismatch that aborts the worker |
| `UcxExchangeProtocol` | the versioned handshake, end and control messages | a bad message that aborts the worker |

## How the design relates to existing fixes

The tests of #17752, #19064, #19047, #19701 and #19859 are the design's
contract, adapted, most only to the new message format. Some change their
expected outcome on purpose:
- a clean end after a failed producer, or after its output was deleted,
  becomes the producer's error;
- #19047's endpoint closed during the last send follows the design's rule;
- #19701's two placeholder tests keep the release, as #19047 does;
- #19701's `olderServerGetsNoCloseMessage` becomes tests of the three version
  cases.

New tests cover the sweep and malformed control messages. With #19701, one
removal takes five locks one after another, and four paths and eleven flags
decide a consumer's end; in the design, each is one decision under one lock.

## Comparison with the HTTP exchange

The HTTP exchange, in Velox and Prestissimo, keeps its state in the same two
places: one `OutputBuffer` per task and one `ExchangeQueue` per client, each
under one lock. `UcxExchangeQueue` is modeled on `ExchangeQueue`.

It differs in three ways:

- **Producer failure.** HTTP never reports it on the data path. A failed
  producer answers with empty pages that are never marked complete, until the
  coordinator aborts the consumer. The UCX exchange sends the producer's error
  in-band, so the consumer fails at once.
- **Acknowledgments.** HTTP retries requests for up to 3 minutes, so the
  producer keeps pages until the consumer acknowledges them. The UCX exchange
  has reliable connections and no retries. A source accepts chunks only in
  sequence and ends only after the end message, so a lost message fails or
  stalls the channel but never ends it cleanly. Chunks are freed as soon as
  their send completes.
- **Bounded waits.** Every HTTP request expires, so a consumer of a dead
  producer always fails eventually. The UCX exchange detects a lost peer only
  with `ucxx.error_handling` on.

## References

- Velox pull requests:
  - landed: #17752, #17753 (as commit d66364f743), #19064;
  - closed without landing: #18921;
  - open: #18590, #19859;
  - open, replaced by the proposal: #19047, #19701.
