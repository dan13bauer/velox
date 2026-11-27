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
halves. The server and the source keep only their UCX progress. The protocol
gets a version and changes without backward compatibility; workers of
different versions never crash, and the query fails with a message instead.
The design carries the fixes of the open PRs #19047 and #19701, in one
change.

## Background

The **current design** is the upstream code at the time of writing. The
proposal starts from it, and takes the fixes of #19047 and #19701 into the
design instead of landing them first.

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
  metadata message and a data message; a final message ends the
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
- Each half can be tested on its own, without UCX.
- A worker never crashes on a peer of another protocol version; the query
  fails with a message that names both versions.

## Requirements

- No change to the Velox core interfaces (`exec::OutputBufferManager`,
  `exec::ExchangeClient`).
- No change to the data encoding: `cudf::packed_columns` and its cuDF
  metadata stay as they are. Only the exchange's own messages change.
- No backward compatibility. The exchange's messages may change in the most
  efficient way, provided that workers with an older or newer version fail
  gracefully: neither crashes, and the query fails with a message that says
  why. A cluster runs one version.
- UCX and UCXX remain the transport.
- The data path stays zero-copy, with no new copies or synchronization per
  chunk.

The proposal inherits one limit of UCX. With `ucxx.error_handling` off, a
failed peer is never reported, so a channel to that peer ends only when one of
its tasks ends.

## Proposal

### Starting point

The proposal goes from the current design to the design in one change, which
replaces #19047 and #19701 and takes their fixes:

- from #19047: a producer finishes only after the send of its last message
  completed; a close message for a consumer that leaves early; each server
  bound to its output queue once; the release remembered for a consumer that
  closed before its producer initialized;
- from #19701: the producer's error in the last message of a channel, kept for
  consumers that connect after the producer was removed; and a consumer that
  fails when its endpoint closes or a message cannot be decoded.

Both PRs had to keep the current wire format, so #19701 adds a capability flag
for the close message and older consumers still cannot see the producer's
error. The design versions the protocol instead. Where the two PRs disagree,
it follows #19701 on removal: a removed producer with undelivered output sends
its error, never a clean end. It follows #19047 on a consumer that closes
before its producer initialized: the release is remembered, so the producer
can finish. The intra-node path is out of scope and is to be removed
separately.

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
before the task initialized), Active, and Removed. It replaces the current design's
`queues_` (output queues and placeholders) and `removedTasks_`, whose late
readers get a clean end, and the `failedInitializations_` that #19701 would
add. Each entry is the task's output queue and stays the
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
  server sends the error in the end message. A consumer that
  connects to a Removed task gets the same outcome.
- **Consumer gone:** the close message arrives or the endpoint closes. The
  destination is Released and its results are deleted, so the producer can
  finish.
- **Producer cannot deliver:** a send fails while the endpoint stays open, or
  the server fails on its own worker. The producer task fails, so the
  coordinator aborts the query, and a later removal ends the remaining
  consumers with that error.

The server keeps its UCX progress and reports three events to the output
queue: the close message, endpoint closure, and a failed send or server.

### The consumer half

The exchange queue keeps one entry per source: Registered, Open, Ended(outcome)
or Released. The endings fall into three groups, and again the first decision
wins:

- **Clean end:** the end message arrives without an error.
- **Error:** the end message carries the producer's error; the handshake
  response has another protocol version; a connect, handshake, receive,
  decode or allocation fails; or the endpoint closes. The consumer fails.
- **Closed:** the client closes. The source is Released and tells its server
  with the close message if the channel has not ended. A source that closes
  during the handshake waits, with a timeout, for the response. It sends the
  close even without a response, once its handshake was sent, since the close
  reaches the producer task's output queue whether or not the server exists. A
  source added after the close still connects to tell its server.

The consumer has finished when every source has Ended or been Released, and
fails when any source ends with an error. Once it has failed or closed, the
exchange queue frees any table that still arrives. The source keeps its UCX
progress and reports every end through one call, `end(outcome)`. A close and
an end that race are settled by the entry's state: only one is counted.

### Between the halves: the protocol

| Message | Producer half | Consumer half |
|---|---|---|
| handshake, with the protocol version | binds a server to a Pending, Active or Removed task entry, if the versions match | Registered |
| handshake response, with the protocol version | — | Open if the versions match; otherwise Ended with a version error |
| metadata and data messages | Open | Open |
| end message, with the outcome and the producer's error, if any | Ended | Ended, with the same outcome |
| close message | Released | Released |
| endpoint closed | Released | Ended with an error, unless already Ended or Released |

- **One protocol version, no flags.** The handshake and its response each
  carry a version, which every wire-format change increments. A server
  accepts only a handshake of its own version, and a source only a response
  of its own version.
- **A fixed prefix.** The handshake keeps the current design's fields at their
  current offsets, with the version after them, sent as the same active
  message; the response is 16 bytes in every version, with the version beyond
  the current design's 8 bytes, on the same tag. So any
  two versions, including the current design, tell each other apart:
  - a current producer accepts the longer handshake and sends its 8-byte
    response; the new source finds no version and fails the query;
  - a new producer finds no version in a current handshake, creates no
    server, and fails the producer task if it is Active; its 16-byte response
    makes the current source's 8-byte receive fail with a truncation error;
  - two different new versions: the response rejects the handshake, and both
    sides fail with both versions in the message.
  No worker crashes; a rolling upgrade fails the queries that span versions.
- **An end message.** A channel ends with an end message, which carries the
  outcome and the producer's error, instead of a metadata message with
  `atEnd`. A clean end is a few bytes.
- **One close message**, naming the channel by task ID and destination, and
  **one close receive per worker**: the Communicator hands each close message
  to the output queue of the task it names, which releases the destination
  even if its server has ended or does not exist yet. With a receive per
  server, as in #19047 and #19701, such a message stays in UCX for the life of
  the worker.
- **No check that aborts the worker.** A message that cannot be decoded fails
  its channel, instead of failing a `VELOX_CHECK` inside a UCX callback as
  the current design does.

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
  message, endpoint closure and failed sends. It ends a channel with the end
  message, which carries the producer's error. A failed send fails the
  producer task. This fixes the clean end of a failed producer, which loses
  rows, and producers left `RUNNING` after a failed send.
- **`UcxExchangeQueue`** keeps one entry per source and implements the
  consumer's outcome table; once the consumer has failed or closed, it frees
  every table that still arrives. This fixes tables that hold GPU memory after
  a close or a failure.
- **`UcxExchangeSource`** keeps only its UCX progress, checks the response's
  version, and reports every end through `end(outcome)`, which replaces the
  paths that each decide part of the end with flags of their own. A close
  during the handshake waits for the response, so it reaches its server. This
  removes the close and end races and fixes a consumer that leaves early
  without telling its producer.
- **`UcxExchangeClient`** releases its sources' entries when it closes, so the
  closed state lives in one place.
- **`Communicator`** keeps one receive for all close messages on its worker
  and hands each to the task's output queue, and tells every server and source
  on a closed endpoint, as #19701 does.
- **`Acceptor`** checks the handshake's version and rejects a mismatch with a
  response and an error, and drops a malformed handshake instead of failing a
  `VELOX_CHECK`. A response that cannot be sent releases the destination.
  This fixes a protocol mismatch that aborts the worker.
- **`UcxExchangeProtocol`** gets the versioned handshake and response, the end
  message and the close message.

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
tables. All tests are retained and adapted to the new design; most need only
the new message format. Some change their expected outcome on purpose:
- tests that expect a clean end after a failed producer, or after its output
  was deleted, now expect the producer's error;
- #19047's tests of an endpoint closed during the last send, and of a close
  without a handshake response, follow the design's rules for these cases;
- #19701's two placeholder tests keep the release of a consumer that closed
  before its producer initialized, as #19047 does;
- #19701's `olderServerGetsNoCloseMessage` becomes tests of the three version
  cases.
With #19701, one removal takes five locks one after another, and a consumer's
end is decided by four paths and eleven flags; in the design, each is one
decision under one lock.

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
  only in sequence and ends only after the end message, so a lost
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
  - open: #18590, #19859;
  - open, replaced by the proposal: #19047, #19701.
