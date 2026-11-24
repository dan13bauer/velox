# UCX Exchange: One State Machine for Each Half of a Channel (Summary)

**Status:** proposal for discussion.
**Scope:** `velox/experimental/ucx-exchange`.
**Detailed version:** [ucx-exchange-channel-lifecycle.md](ucx-exchange-channel-lifecycle.md).

## Summary

Many fixes to the UCX exchange share one shape. A channel, the link from one
producer task's destination to one consumer, ends in a way that one component
handles, while another component with its own copy of the channel's state
never hears about it. A consumer then waits until its query is aborted,
finishes with rows missing, or keeps GPU memory.

A channel spans two workers, so no structure on one worker can hold all of
it. This note proposes one design: each half of a channel gets one state
machine, kept in the object that already holds that side's task-wide state
under one lock. Protocol messages are the only transitions between the
halves. The server and the source keep only their UCX progress.

## Background

The **current design** is the upstream code at the time of writing. The
**baseline** is the current design with the open PRs #19701 and #19047
landed; the proposal starts from the baseline.

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
- On a **producer task**'s worker, `UcxPartitionedOutput` writes **chunks**
  (today cuDF packed tables) into the task's **output queue**
  (`UcxOutputQueue`), which holds one **destination** queue per consumer under
  one lock. The **end marker** ends a destination. The **output queue
  manager** (`UcxOutputQueueManager`) holds the output queues of the worker's
  tasks and implements Velox's `exec::OutputBufferManager`.
- A **consumer task** owns a `UcxExchangeClient`, which owns one **source**
  (`UcxExchangeSource`) per producer and one **exchange queue**
  (`UcxExchangeQueue`) for the tables they receive.
- When a source connects, the producer's worker creates a **server**
  (`UcxExchangeServer`) for its destination. The server sends each chunk as a
  metadata message and a data message; the last metadata message ends the
  channel.
- The **Communicator** is one per worker. It runs the event loop that drives
  UCX for all servers and sources, and owns the endpoints, which all channels
  between two workers share.
- A **placeholder** is an output queue created for a server that asks for a
  task before the task has initialized.
- A **channel** is one producer task and destination, with its server and its
  source. Its **producer half** is the destination queue and the server; its
  **consumer half** is the source and its tables in the exchange queue. Its
  **outcome** is how it ended: cleanly, or with an error.

### Who touches a channel's state

Eight components hold part of a channel's state: the output queue manager,
the output queue, the intra-node registry, the server, the source, the
exchange queue, the client and the Communicator. That state is queues, flags,
state machines and side maps keyed by task ID. Five kinds of threads change
it: producer drivers, consumer drivers, the Communicator thread, the thread
that terminates a task, and the worker's request threads.

## The problem

### The pattern in the fixes

- #17752: closing one `UcxExchange` closed a client that its siblings still
  used. The client's closed state was in dispute between the operator and the
  shared client.
- #17753 and #18921: a consumer that connected while or before its producer
  initialized missed the intra-node path. Whether the producer's queue
  existed was in dispute between queue initialization, the Acceptor and the
  placeholder.
- #19047: a consumer that closed early left its producer `RUNNING`. Whether
  the consumer was still reading was in dispute between the source, the
  server and the output queue.
- #19064: a broadcast reader that arrived before its destination was
  announced waited forever. Publication was in dispute between the reader and
  the output queue.
- #19701: a failed producer ended its channels cleanly, so a query returned
  0 rows. The producer's outcome was in dispute between the output queue, the
  server and the intra-node registry. Its review found three more races of the
  same kind in its own drafts.
- #19859: producer and consumer backpressure counted different things.
  Memory in use was in dispute between the queues, the server and the
  consumer.
- Found while preparing this note: a table that arrives during a consumer's
  close stays in the closed queue, and a failed send leaves its producer
  `RUNNING`.

### Root causes

**No component owns a channel's ending.** A channel can end in at least six
ways: every producer driver calls `noMoreData()`; the producer task is
removed; the destination's results are deleted; the consumer closes; the
connection fails; or, on the intra-node path, a published chunk is never
read. Each event reaches some components and not others. On the producer side,
the output queue, the manager and the server each decide part of the ending;
on the consumer side, the source, the exchange queue and the client do. The
endings are not written down, so each new one needs a fix at every boundary it
crosses.

**Task lifetime clashes with UCX connection lifetime.** A `Task` lives as long
as the worker keeps it; a connection lives as long as the network and the peer
allow. A consumer can connect before its producer exists or long after it is
gone, linked only by the task ID. Placeholders and records of removed tasks
bridge the gap in side maps whose lifetime is defined only indirectly.

**The protocol carries little of the lifecycle.** In the current design, the
wire format cannot say why a channel ended, has no message for a consumer
that leaves early, and aborts the worker on a version mismatch. State has to
be inferred: an unknown task ID means "not created yet", a silent consumer
"still reading".

**Two transports double the cases.** The intra-node path has its own registry
and handoff, and two of the three races in #19701's drafts exist only there.

**Endings are task-wide, but only the consumer decides them under one lock.**
Removing a producer ends all its destinations, and whether that end is clean
depends on all of them. A consumer finishes when every source has ended and
fails when any fails. The consumer keeps these under the exchange queue's
lock. With #19701, the producer reads its output queue, releases the lock and
retries under the manager's lock, and two races came from those gaps.

## Goals

- Every reader gets exactly one outcome: a chunk, a clean end, or an error that
  says why the channel ended.
- Each half of a channel decides its outcome once, in one place, and the
  protocol carries that decision to the other half.
- Every way a half can end is listed in one table, with one test each.
- No state outlives what needs it: a removed task's entry is dropped once its
  `Task` is gone, and a channel's state ends with its last participant.
- Each half can be changed and landed on its own.

## Requirements

- No change to the Velox core interfaces (`exec::OutputBufferManager`,
  `exec::ExchangeClient`).
- No change to the data encoding: `cudf::packed_columns` and its metadata stay
  as they are.
- UCX and UCXX remain the transport.
- The data path stays zero-copy, with no new copies or synchronization per
  chunk.

The proposal inherits one limit of UCX. With `ucxx.error_handling` off, a
failed peer is never reported, so a channel to that peer ends only when one of
its tasks ends.

## Proposal

### Baseline

The proposal starts from the baseline, with one close message shared by #19701
and #19047; #19047 binds each server to its output queue once. Where the two
PRs disagree, the proposal follows #19701 on removal: a removed producer with
undelivered output sends its error, never a clean end. It follows #19047 on a
consumer that closes before its producer initialized: the release is
remembered, so the producer can finish. The intra-node path is out of scope and
is to be removed separately.

### Where each half's state lives

Each half's state machine goes into the object that holds its side's task-wide
state: one entry per destination inside the output queue, and one entry per
source inside the exchange queue. Every decision that ends a channel is taken
under that object's lock. The consumer keeps the lock that already decides its
endings; the producer decides removal in one step under the task entry's lock
instead of reading, releasing and retrying. Velox's CPU exchange makes the same
choice: one lock per task's output buffer, over its destination buffers.

### The producer half

The output queue manager keeps one map of task entries: Pending (a server asked
before the task initialized), Active, and Removed. It replaces the baseline's
`queues_.byTaskId` (output queues and placeholders), `removedTasks_` and
`failedInitializations_`. Each entry is the task's output queue and stays the
same object from Pending to Removed: removal frees its chunks and keeps only a
`std::weak_ptr` to the `Task` and its outcome, so a Removed entry lives as long
as the worker keeps the `Task`; a Pending entry is dropped with its last
waiting server, unless a consumer closed, in which case it keeps that release
until the task initializes. Every state change happens under the entry's lock;
the manager's lock only adds and erases entries. Each destination is
Unpublished, Open, Ended(outcome) or Released.

The endings fall into four groups. The first decision for a destination wins.

- **Clean end:** the server takes the end marker, or the task is removed after
  all drivers finished with nothing left to deliver.
- **Producer error:** the task is removed with output still undelivered, never
  initialized, or discarded its output after running out of GPU memory. The
  server sends the error in the last metadata message. A consumer that
  connects to a Removed task gets the same outcome.
- **Consumer gone:** the close message arrives or the endpoint closes. The
  destination is Released and its results are deleted, so the producer can
  finish.
- **Producer cannot deliver:** a send fails while the endpoint stays open. The
  producer task fails, so the coordinator aborts the query.

The server keeps its UCX progress and reports three events to the output
queue: the close message, endpoint closure and a failed send.

### The consumer half

The exchange queue keeps one entry per source: Registered, Open, Ended(outcome)
or Released. The endings fall into three groups, and again the first decision
wins:

- **Clean end:** the last metadata message arrives without an error.
- **Error:** the last metadata message carries the producer's error, or a
  connect, handshake, receive, decode or allocation fails, or the endpoint
  closes. The consumer fails.
- **Closed:** the client closes. The source is Released and tells its server
  with the close message if the channel has not ended. A source added after the
  close still connects to tell its server.

The consumer has finished when every source has Ended or been Released, and
fails when any source ends with an error. Once it has failed or closed, the
exchange queue frees any table that still arrives. The source keeps its UCX
progress and reports every end through one call, `end(outcome)`. A close and
an end that race are settled by the entry's state: only one is counted.

### Between the halves: the protocol

| Message | Producer half | Consumer half |
|---|---|---|
| handshake | binds a server to a Pending, Active or Removed task entry | Registered |
| handshake response, with capability flags | — | Open |
| metadata and data messages | Open | Open |
| last metadata message, with `atEnd` and the producer's error, if any | Ended | Ended, with the same outcome |
| close message | Released | Released |
| endpoint closed | Released | Ended with an error, unless already Ended or Released |

- **Capability flags.** The handshake response has padding bytes that older
  builds send as zero; a new message or field gets a flag there, so old and new
  builds work together. #19701 uses the first flag, `acceptsClose`. The
  handshake could get flags of its own the same way, since older servers
  ignore a longer handshake.
- **One close message.** #19047 and #19701 add the same message. One PR should
  carry it and the other build on it, sent only if the server set
  `acceptsClose`. A source that closes during the handshake waits, with a
  timeout, for the response before deciding.
- **One close receive per worker.** The Communicator keeps one receive for all
  close messages on its worker and hands each to its server, or drops it if the
  server has ended. In the baseline, such a message stays in UCX for the life
  of the worker.
- **Why the channel ended.** #19701 adds the producer's error to the last
  metadata message, without a flag. An older consumer ignores it and can still
  miss rows; later format changes should use a flag.

### Rules for both halves

- Every ending goes through its half's entry, under the lock that already
  guards it. Callbacks run, and freed chunks and tables are released, after
  that lock is released.
- Every decision for a task is made under its entry's lock; the manager's
  lock only adds and erases entries. No `Task` lock is taken while the entry's
  lock is held: removal reads the task's final state and error first.
- Entries treat chunks and tables as opaque, keep no memory accounting of their
  own (#18590 charges chunks to Velox's memory pools), and free chunks when a
  channel is released or fails. No state that outlives a channel holds a
  chunk.
- Each table is one function from state and event to the next state and
  outcome, with one test per row, run without UCX.

### What changes where

- **`UcxOutputQueueManager`** keeps one map of task entries, Pending, Active or
  Removed, instead of `queues_.byTaskId`, `removedTasks_` and
  `failedInitializations_`; each entry stays one object from Pending to
  Removed, under one lock, and is erased once its `Task` is gone. This fixes
  side maps that had to agree and could outlive their task, and a placeholder
  created in the middle of a removal.
- **`UcxOutputQueue`** gives each destination a state and implements the
  producer's outcome table; removal decides all destinations in one step under
  its lock. This removes the read, release and retry of removal, where races
  came from, and makes a new ending one new table row.
- **`UcxExchangeServer`** keeps only its UCX progress and reports the close
  message, endpoint closure and failed sends. A failed send fails the producer
  task. This fixes producers left `RUNNING` after a failed send.
- **`UcxExchangeQueue`** keeps one entry per source and implements the
  consumer's outcome table; once the consumer has failed or closed, it frees
  every table that still arrives. This fixes tables that hold GPU memory after
  a close or a failure.
- **`UcxExchangeSource`** keeps only its UCX progress and reports every end
  through `end(outcome)`, which replaces four paths with flags of their own. A
  close during the handshake waits for the response, so it reaches servers
  that accept it. This removes the close and end races and fixes a close that
  never reached its server.
- **`UcxExchangeClient`** releases its sources' entries when it closes, so the
  closed state lives in one place.
- **`Communicator`** keeps one receive for all close messages on its worker
  and drops those whose server has ended. This fixes close messages that stay
  in UCX for the life of the worker.

The other components still hold state, but only transport state: UCX
requests, the chunk in flight, endpoints and the work queue. None of it
decides how a channel ends. On the producer side, that decision lives only in
the manager's task entry and the states of its destinations:

- The calls from Velox and `UcxPartitionedOutput`, `initializeTask()`,
  `removeTask()`, `noMoreData()` and `deleteResults()`, reach the entry
  through the manager.
- Each server is bound to its entry once, asks it for the next step, and
  reports the close message, endpoint closure and failed sends to it.

The consumer side works the same way through the exchange queue's source
entries.

## How the design relates to existing fixes

The tests that #17752, #19064, #19047, #19701 and #19859 add are the design's
contract; the detailed version maps each of them to a row of the outcome
tables. Every lifecycle test maps to a row, and four change their expectations
on purpose. Two are #19047's tests of a failed last send, which expect the
producer to stay `RUNNING`; the design fails it. Two are #19701's placeholder
tests: the design keeps the release of a consumer that closed before its
producer initialized, as #19047 does. In the baseline, one removal takes five
locks one after another, and a consumer's end is decided by four paths and
eleven flags; in the design, each is one decision under one lock.

## Comparison with the HTTP exchange

The HTTP exchange, in Velox and Prestissimo, keeps its state in the same two
places: one `OutputBuffer` per task and one `ExchangeQueue` per client, each
under one lock. `UcxExchangeQueue` is modeled on `ExchangeQueue`.

It differs in three ways:

- **Producer failure.** HTTP never reports it on the data path. A failed
  producer answers with empty pages that are never marked complete, until the
  coordinator aborts the consumer. The UCX exchange sends the producer's error
  in-band, so the consumer fails at once with the producer's message.
- **Acknowledgments.** HTTP retries requests for up to 3 minutes, so the
  producer keeps pages until the consumer acknowledges them. The UCX exchange
  sends over reliable connections without retries. A source accepts chunks
  only in sequence and ends only after the last metadata message, so a lost
  message fails or stalls the channel but never ends it cleanly. No
  acknowledgment is needed, and chunks are freed as soon as their send
  completes.
- **Bounded waits.** Every HTTP request expires, so a consumer of a dead
  producer always fails eventually. In the UCX exchange, failure detection
  depends on `ucxx.error_handling`; with it off, a lost peer is never reported.

In short, the UCX exchange gives stronger delivery guarantees and earlier
error reports, and the HTTP exchange gives bounded waits.

## References

- Velox pull requests:
  - landed: #17752, #17753 (as commit d66364f743), #19064;
  - closed without landing: #18921;
  - open: #18590, #19047, #19701, #19859.
