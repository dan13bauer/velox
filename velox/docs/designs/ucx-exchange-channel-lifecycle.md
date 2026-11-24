# UCX Exchange: One State Machine for Each Half of a Channel

**Status:** proposal for discussion.
**Scope:** `velox/experimental/ucx-exchange`.

## Summary

The UCX exchange moves GPU column buffers between Velox tasks over UCX; today
these are the packed tables of the cuDF backend. Many of the fixes it has
received over the last six months share one shape. A channel, the
point-to-point link from one producer task's destination to one consumer, ends
in a way that one component handles. Another component, holding its own copy
of the channel's state, never hears about it. A consumer then waits until its
query is aborted, finishes with rows missing, or leaves memory behind.

A channel spans two workers. Its producer half runs on the producer's worker,
and its consumer half on the consumer's worker, so no structure on one worker
can hold a whole channel. This note proposes an extention of the current
design:

- Each half of a channel gets one state machine, with a written table of the
  events that end it and the outcome of each.
- Each state machine lives in the object that holds that side's task-wide
  state under one lock: the producer task's output queue, and the consumer's
  exchange queue.
- Only protocol messages carry an outcome from one half to the other: the
  handshake, the last metadata message with the producer's error, and the
  close message.
- The server and the source keep only their UCX progress. They report what
  happened on the wire to their half's state machine and act on its decisions.

The design adds no singleton, moves no chunks between components, and changes
no Velox interface.

## Background

This note uses two reference points. The **current design** is the upstream
code at the time of writing. The **baseline** is the current design with the
open PRs #19701 and #19047 landed; the proposal starts from the baseline.

### UCX in brief

UCX is a communication library; UCXX is its C++ wrapper, which this exchange
uses. A **UCX worker** is the UCX object that progresses communication; it is
unrelated to the worker processes defined below. An **endpoint** is a
connection from one UCX worker to another. Messages are sent with a 64-bit
**tag**, and a receive posted for a tag completes when a message with that tag
arrives from any endpoint of the UCX worker. A message that arrives before its
receive is posted waits in the UCX worker's queue of **unexpected messages**
until one is. Large messages use a **rendezvous** protocol: the data stays on
the sender until the receiver posts the matching receive. Rendezvous can use a
fast local copy (`cuda_copy`) between GPU buffers in one process.

UCX detects a failed peer only with **peer error handling**, which this
exchange enables through `ucxx.error_handling` (default `true`). Some fabrics,
such as AWS SRD, do not support it; there the setting must be off, and a
failed peer is never reported.

### Terms

The diagram shows each term below in place. Producer task X on worker A sends
its destination 1 to consumer task Y on worker B over one channel, outlined in
red. The lower part shows the messages that channel carries, in time order.

![Components of a UCX exchange channel and the messages it carries](images/ucx-exchange-channel-terms.drawio.svg)

- A **worker** is a process that runs Velox tasks, such as a Prestissimo
  worker, the C++ worker of Presto. The **coordinator**, a separate process,
  schedules tasks on the workers and aborts a query's remaining tasks when the
  query finishes or fails. The worker decides how long a finished `exec::Task`
  object stays alive. A query's exchange connects a producer task on one worker
  with consumer tasks on the same or other workers.
- A **producer task** is a task whose plan ends in a `PartitionedOutputNode`
  that selects the UCX transport. Its drivers run `UcxPartitionedOutput`
  operators, which write **chunks** into the task's **output queue**
  (`UcxOutputQueue`). A chunk is the unit a server sends as one metadata
  message and one data message; today it is a cuDF table packed into
  `cudf::packed_columns`.
- An output queue holds one **destination** queue (`UcxDestinationQueue`) per
  consumer that reads from the producer task: one per partition for a
  partitioned output, one per consumer for a broadcast. One lock in the output
  queue guards all of its destination queues. A destination is **published**
  once its queue exists in the output queue: at `initializeTask()`, or, for
  broadcast, when `updateOutputBuffers()` announces it. A broadcast output
  queue keeps its chunks for destinations that arrive late. The chunks
  a destination queue holds are its **results**. The **end marker** is a null
  chunk that ends a destination's channel; the output queue queues it once
  every driver of the producer task called `noMoreData()`.
- The **output queue manager** (`UcxOutputQueueManager`) is a singleton on each
  worker. It holds the output queues of the producer tasks running on that
  worker, keyed by task ID, and implements Velox's `exec::OutputBufferManager`
  interface for the UCX transport.
- A **consumer task** is a task with an `ExchangeNode` that selects the UCX
  transport. The task owns one `UcxExchangeClient` per such node, shared by the
  node's `UcxExchange` operators. The client owns one **source**
  (`UcxExchangeSource`) per producer task it reads from, and one **exchange
  queue** (`UcxExchangeQueue`), into which all its sources put the tables they
  receive.
- A source connects to the worker of its producer task. On that worker, the
  `Acceptor` creates a **server** (`UcxExchangeServer`) when the source's
  handshake arrives. A server serves one destination of one producer task: it
  waits for that destination's next chunk, which makes it the destination's
  **reader**, and sends the chunk to its source.
- A **placeholder** is an output queue that the output queue manager on the
  producer's worker creates when a server asks it for a task that has not
  called `initializeTask()` yet. The server's request waits in the
  placeholder, and when the task calls `initializeTask()`, the placeholder
  becomes the task's output queue.
- The **Communicator** is a singleton on each worker that serves both its
  producer and its consumer tasks. It owns the worker's UCX worker, a work
  queue, and the endpoints to and from the worker's peers. All channels
  between two workers share these endpoints. The worker process runs the
  Communicator's event loop, `Communicator::run()`, on one thread of its own.
- The **intra-node path** hands chunks between a server and a source in the
  same worker through the **intra-node registry**
  (`IntraNodeTransferRegistry`) instead of UCX transfers. It is off by default
  (`cudf.intra_node_exchange`), and it is not used for broadcast or for
  placeholders.
- A **channel** is one producer task and destination together with the server,
  on the producer's worker, and the source, on the consumer's worker, that move
  its chunks. Its **producer half** is the destination queue and its server;
  its **consumer half** is the source and the tables it has put into the
  exchange queue. A channel is neither the UCX endpoint, which all channels to
  one peer share, nor a consumer's set of sources.
- A channel's **outcome** is how it ended: cleanly, or with an error and the
  error's message.

### How a chunk travels

1. A producer driver calls `UcxOutputQueueManager::enqueue()`. If a server is
   waiting on that destination, its callback runs on the producer's driver
   thread, stores the chunk, and puts the server on the Communicator's work
   queue.
2. On the Communicator thread, the server sends a metadata message and then
   the data. Each message has a tag built from a hash of the task ID and
   destination, an operation byte and a sequence number.
3. The source receives both on the Communicator thread and puts the table into
   the exchange queue, where the consumer's drivers read it.
4. A channel ends with a metadata message that has `atEnd` set. Right after
   posting that send, the server deletes the destination's results.

### Who touches a channel's state

In the current design, the channel state is distributed across multiple
components, as shown in the table below. #19701 adds the items marked (†).

| Component | Channel state |
|---|---|
| `UcxOutputQueueManager` | output queues and placeholders by task ID; IDs of removed tasks; the producer's error for each removed task (†); tasks whose initialization failed (†); readers arriving at placeholders (†) |
| `UcxOutputQueue`, `UcxDestinationQueue` | chunks, the waiting reader's callback, readers of unpublished destinations (`pendingQueues_`), end marker, `terminated_` |
| `IntraNodeTransferRegistry` | published intra-node chunks, IDs of cancelled tasks; tasks held still during removal (†) |
| `UcxExchangeServer` | a 7-state machine, the chunk in flight, UCX requests, a closed flag; whether its consumer is gone, and whether its source sent the close message (†) |
| `UcxExchangeSource` | an 8-state machine; end-of-stream, closed, end-marker-delivered and registered flags; a backpressure flag; UCX requests; whether its server accepts the close message, and whether its endpoint closed (†) |
| `UcxExchangeQueue` | received tables, the number of sources that ended, `atEnd_`, one error for the whole consumer |
| `UcxExchangeClient` | its sources, `closed_` |
| `Communicator`, `EndpointRef` | registered sources and servers, endpoints, deferred cleanup |

These components are called from at least five kinds of threads:

- producer drivers, through `enqueue()`, which fires reader callbacks;
- consumer drivers, which read tables and release backpressure;
- the Communicator thread, for UCX callbacks and the servers' and sources'
  `process()`;
- the thread that runs `Task::terminate()`, which calls `removeTask()` for a
  producer and closes the client of a consumer;
- the worker's other threads, such as those that handle the coordinator's
  requests, through `stats()` and `updateOutputBuffers()`.

In the current design, the headers in `velox/experimental/ucx-exchange` declare
11 locks and 10 atomic members, in about 7,500 lines of code without tests. The
components refer to each other by task-ID strings and tag hashes.

## The problem

### The pattern in the fixes

| PR | What went wrong | Where the state diverged |
|---|---|---|
| #17752 | closing one `UcxExchange` closed a client that its siblings still used | operator vs. shared client |
| #17753 | a consumer whose handshake arrived while its producer was initializing missed the intra-node path | queue initialization vs. the Acceptor |
| #18921 (closed after openucx/ucx#11319) | a consumer that connected before its producer initialized never got the intra-node path | placeholder vs. initialized queue |
| #19047 (open) | with `UCX_RNDV_THRESH=0`, which makes every message use rendezvous, a consumer that closed before posting its last receive left the producer task `RUNNING` | source vs. server vs. output queue |
| #19064 | a broadcast reader that connected before its destination was announced waited forever when data or the end marker was already queued | reader vs. destination publication |
| #19701 (open) | a producer that failed or was aborted ended its channels cleanly, so a TPC-DS query returned 0 rows | output queue vs. server vs. registry |
| #19859 (open) | producer backpressure counted queued bytes, consumer backpressure counted queued tables, and neither counted GPU memory still in use after dequeue | queue vs. server vs. consumer |

#19701 shows the pattern from the inside. Review of its first drafts found
three races of this kind in its own new code:

- A chunk that a source had already read from the intra-node registry, but
  whose read the server had not processed yet, was counted as lost when the
  producer was removed in between
  ([comment](https://github.com/facebookincubator/velox/pull/19701#discussion_r4378663516)).
- A server could create a placeholder between the two lookups that one
  `removeTask()` call makes, and that placeholder then ended cleanly instead of
  with the producer's error
  ([comment](https://github.com/facebookincubator/velox/pull/19701#discussion_r4378734102)).
- The record of a removal in the output queue manager and the one in the
  intra-node registry could carry different reasons
  ([comment](https://github.com/facebookincubator/velox/pull/19701#discussion_r4554522671)).

It also found older cases in the current design, which #19701 fixes:

- Records of removed tasks are freed only when a task ID is initialized or
  removed again, which Presto never does, so they stay for the life of the
  worker ([comment](https://github.com/facebookincubator/velox/pull/19701#discussion_r4554522689)).
- A placeholder for a producer that never arrives outlives the server that
  waited on it, and stays for the life of the worker.
- A server whose consumer closed early keeps waiting, with the results kept
  for that consumer, until the producer is removed.

The consumer half has cases of its own, found while preparing this note. A
source checks its closed flag when a data message arrives, then puts the table
into the exchange queue. If the client's close empties the queue between the
check and the put, the late table stays in the closed queue, holding GPU
memory until the consumer's `Task` is destroyed. After one source fails, the
other sources keep putting tables into the queue until the client closes,
although no driver reads them any more.

The producer half has one more, also found while preparing this note. When a
server's send fails, the server stops without deleting its destination's
results or releasing its reader, so the producer task stays `RUNNING` until it
is removed.

Each fix so far has handled one of these cases where it was found.

### Root causes

**No component owns a channel's ending.** A channel can end in at least six
ways:

- every producer driver calls `noMoreData()`;
- the producer task is removed (`removeTask()`), which `Task::terminate()`
  does when the task finishes, fails, or is aborted or cancelled;
- the destination's results are deleted (`deleteResults()`), by the server
  after the end marker, or by the producer itself when it runs out of GPU
  memory;
- the consumer closes;
- the connection fails;
- on the intra-node path, a published chunk is never read.

Each event reaches some of the components above and not others. On the
producer side, the output queue, the output queue manager and the server each
decide part of the ending. On the consumer side, the source, the exchange queue
and the client do. The endings are not written down anywhere, so every new
way to end a channel needs a fix at every boundary it crosses.

**Task lifetime clashes with UCX connection lifetime.** A Velox `Task` lives
as long as the worker keeps it. A UCX connection lives as long as the network
and the peer allow. A consumer can connect, and so create a server, before its
producer task exists or long after it is gone. The only link between the two is
the task ID. The code bridges the gap with placeholders and records of removed
tasks, both kept in side maps keyed by task ID. Their lifetime follows from the
`Task`'s lifetime only indirectly, and the cases above show both staying for
the life of the worker.

**The protocol carries little of the lifecycle.** In the current design, the
wire format has no way to say why a channel ended and no message for a consumer
that leaves early. Its only version check is a magic number that the receiver
verifies with `VELOX_CHECK` inside a UCX callback, which the Communicator loop
does not catch, so a mismatch aborts the worker. State therefore has to be
inferred: an unknown task ID means "not created yet", and a silent consumer
means "still reading".

**Two transports double the cases.** The intra-node path has its own registry
and its own handoff. With #19701, removing a task also has to coordinate with
the registry and count the chunks in flight there. Two of the three races in
#19701's drafts exist only on that path.

**Endings are task-wide, but only the consumer decides them under one lock.**
The decisions that end a channel concern the whole task on both sides:

- On the producer side, removing a task ends all of its destinations at once,
  and whether that end is clean depends on what all of them still hold. The
  count of drivers that called `noMoreData()`, and a broadcast's chunks for
  later destinations, are per task as well.
- On the consumer side, the consumer has finished when every source has ended,
  and it fails when any source fails.

The consumer side keeps these under one lock: the exchange queue's lock guards
the count of sources that ended and the one error, and the client's closed
flag is changed under the same lock. The producer side does not. With #19701,
removal reads the output queue under its lock, releases it, and then retries
under the manager's lock until the queue stops changing; on the intra-node
path it also holds the registry still. Two of the races in #19701's drafts
came from the gaps between those steps.

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
its tasks ends, for example when the coordinator aborts the query.

## Proposal

### Baseline

The proposal starts from the baseline, the current design with #19701 and
#19047 landed, with one close message shared by the two. #19047 binds each
server to the output queue it started with, so a server looks its task ID up
only once. The two PRs disagree on one point. On removal, #19047 deletes the
queued output and ends each destination with a clean end marker, while #19701
sends the producer's error. The tables below follow #19701, because a clean end
after lost output is the row loss #19701 fixes.

The intra-node path is out of scope; its removal is a separate change (see
Related change). Two of the three races in #19701's drafts go away with it,
not with this design.

### Where each half's state lives

Each half's state machine goes into the object that holds its side's
task-wide state: one entry per destination inside the output queue, and one
entry per source inside the exchange queue. Every decision that ends a channel
is taken under that object's lock. On the consumer side, this keeps the lock
that already decides endings. On the producer side, removal is decided under
the output queue's lock in one step, instead of the read, release and retry
described under Root causes. Velox's CPU exchange makes the same choice: one
lock per task's output buffer, over its destination buffers.

### The producer half

`UcxOutputQueueManager` keeps one map, with one entry per task ID. That map
replaces the map of output queues, the placeholders, the records of removed
tasks and the tasks whose initialization failed:

| Task state | Entered when | Left when |
|---|---|---|
| Pending | a server asks for a task the output queue manager has not seen | `initializeTask()`, `removeTask()`, or its last waiting server closes |
| Active | `initializeTask()` | `removeTask()` |
| Removed | `removeTask()` | the next removal after the producer's `Task` is destroyed drops it, or the task ID is initialized again |

A Pending or Active entry is the task's output queue. A Removed entry keeps
only a `std::weak_ptr` to the producer's `Task` and the outcome, and holds no
chunks. It must not hold a `std::shared_ptr`, or the `Task` would never be
destroyed. For a task whose `initializeTask()` threw, the output queue manager
keeps the `Task` from that call. A server whose handshake names a Removed task
is bound to the Removed entry and sends the last metadata message with the
stored outcome. `updateOutputBuffers()` for a Removed task does nothing.

A Removed entry lives long enough for the consumers that need it. A producer
that succeeds cannot be removed before its consumers connect: `Task` reports
itself finished only after each destination's end marker was sent or its
consumer left. Only a failed or aborted producer is removed early, and a
consumer that connects while its `Task` exists gets the producer's error from
the Removed entry. This relies on the worker keeping a finished `Task` until
the coordinator has seen it end; the coordinator then aborts the query's
consumers. Prestissimo keeps a terminated `Task` for `old-task-cleanup-ms`, 60
seconds by default (see Open questions for other workers). A consumer that
connects after the `Task` is gone creates a Pending entry, which ends when that
consumer is aborted and its server closes.

When the last server of a Pending entry closes, the entry is dropped with
everything it recorded. If the task initializes later, its destinations wait
for readers that never come, so the task does not finish until it is removed.
The coordinator removes it when it ends the query, which it does once the
consumers have closed. Keeping the entry instead would bring back the
placeholders that stay for the life of the worker.

Inside the output queue, each destination has one state:

| Destination state | Meaning |
|---|---|
| Unpublished | a reader waits; the destination is not announced yet |
| Open | chunks flow to its reader |
| Ended(outcome) | the end marker, or the error, is decided and queued |
| Released | its results are deleted; nothing more is sent |

Removal decides for all destinations of the task at once; the other events
decide for one destination. The first decision for a destination wins:

| Event | Outcome |
|---|---|
| every driver called `noMoreData()` | none yet; the end marker is queued behind the remaining chunks |
| the server takes the end marker | clean |
| `removeTask()`, after every driver called `noMoreData()`, with no chunk left in any destination | clean, even if the task is aborted at that point |
| `removeTask()` with chunks left in any destination, in a broadcast's chunks kept for later destinations, or in flight on the intra-node path; or before every driver called `noMoreData()` | producer error, with the task's state and error message |
| `removeTask()` without a successful `initializeTask()` | error: the producer never initialized its output |
| `deleteResults()` while the destination is Open, as the producer does when it runs out of GPU memory | error: the producer discarded its output; the results are deleted, and the server sends the error |
| `deleteResults()` after Ended, as the server does once the send of the last metadata message completed | none; the destination becomes Released |
| a reader for a destination beyond the final count | clean, at once, as in the current design: no rows are ever produced for such a destination |
| the close message arrives, or the endpoint closes | none for the consumer, which is gone or sees its endpoint close; the destination becomes Released and its results are deleted |
| a send fails while the endpoint stays open | error: the output queue fails the producer task with the send's error (`Task::setError()`), so the coordinator aborts the query; the consumer, whose receive may never fail, is aborted with it |
| the close message for an Unpublished destination of an Active task | none; the destination is Released before it is published, and publishing it later keeps it Released, as in the baseline |

Releasing the destination when the consumer is gone matches Presto's HTTP
exchange, whose consumer deletes its results on the producer when it aborts.
A producer that is aborted later, as after a satisfied `LIMIT`, then finds the
destination already Released. The destination's state tells the two uses of
`deleteResults()` apart, so its signature does not change. When the producer
runs out of GPU memory, the "discarded" error comes first and the later
`removeTask()` keeps it, so the consumer fails either way.

The server keeps its UCX progress: waiting for a chunk, sending, waiting for
the send to complete. It reads its destination's state through the output
queue it is bound to and reports three events: the close message arrived, its
endpoint closed, and a send failed. In the baseline, the Communicator loop
already tells every element on a closed endpoint through `onEndpointClosed()`,
which #19701 adds; UCX reports a closed endpoint only with
`ucxx.error_handling` on.

### The consumer half

`UcxExchangeQueue` keeps one entry per source:

| Source state | Entered when |
|---|---|
| Registered | the client adds the source |
| Open | the handshake response arrives |
| Ended(outcome) | the source reports an end: clean, the producer's error, or a transport error |
| Released | the client closes |

The first decision for a source wins:

| Event | Outcome |
|---|---|
| the last metadata message, without an error | clean |
| the last metadata message, with the producer's error | producer error; the consumer fails |
| a failed connect, handshake or receive, an undecodable metadata message, or a failed allocation | transport error; the consumer fails |
| the endpoint closes | transport error, unless the source is already Ended or Released |
| the client closes | none; the source becomes Released, and its server is told with the close message if the channel has not ended |
| the client adds a source after it closed | none; the source starts Released |

The consumer has finished when the client called `noMoreRemoteTasks()` and
every entry is Ended or Released; with no sources, that is at once. It fails
when any entry ends with an error before the client closes. In the current
design, these are the exchange queue's count of sources that ended and its one
error; both become views of the entries. Once the consumer has failed or
closed, the exchange queue frees every table a source puts into it, from any
source. It collects them under its lock and frees them after the lock is
released.

The source keeps its UCX progress: connecting, the handshake, waiting for
metadata, waiting for data, and done. It reports every end through one call,
`end(outcome)`. That call replaces four paths that each decide part of the end
in the baseline: delivering the end marker, failing the source, closing it, and
handling a closed endpoint. They rely on a compare-and-swap on the end-marker
flag and on the exchange queue keeping only its first error.

A source that closes before the handshake response arrives does not know yet
whether its server accepts the close message. It waits for the response, which
the server sends right after the handshake, and then sends the close message
only if the server set `acceptsClose`. The wait ends when the endpoint closes
or after a timeout; with `ucxx.error_handling` off, only the timeout ends it.
A source that gets no response sends nothing.

A consumer close and the end of a channel can race in four ways:

- **Clean end and close at the same time.** Only one of them is counted. In the
  current design, the compare-and-swap on the end-marker flag ensures that; in
  the design, the entry's state does.
- **The producer's error and close at the same time.** In the current design,
  the error and the client's closed flag are both set under the queue's lock,
  and a read checks the closed flag first. A driver that reads before the close
  sees the error and fails; after the close, reads end without it. The client
  closes only when its task ends, so an error dropped this way belongs to a
  task that is ending anyway, for example after a satisfied `LIMIT`. The design
  keeps this order.
- **The close message and the end at the same time.** A source sends the close
  message only if it has not yet seen the end. If the server ended first, the
  producer's Communicator still receives the message and drops it (see the
  protocol rules).
- **A table in flight at close.** In the design, the table is freed, as
  described above. In the current design, it stays in the closed
  queue.

### Between the halves: the protocol

The protocol messages are the only transitions that cross workers:

| Message | Producer half | Consumer half |
|---|---|---|
| handshake | binds a server to a Pending, Active or Removed task entry | Registered, waiting for the response |
| handshake response, with capability flags | — | Open |
| metadata and data messages | Open | Open |
| last metadata message, with `atEnd` and the producer's error, if any | Ended | Ended, with the same outcome |
| close message | Released | Released |
| endpoint closed | Released | Ended with a transport error, unless already Ended or Released |

Rules for the messages:

- **Capability flags in the handshake response.** `HandshakeResponse` is 8
  bytes. In the current design, 7 of them are padding that older builds send
  as zero; #19701 uses one of them. A new message or
  field gets a flag there, and a source uses it only if its server set the
  flag, so builds with and without it still work together. #19701 uses the
  first flag, `acceptsClose`, for the close message. Flags flow only from the
  server to the source: the handshake itself has no field for them yet, so a
  server cannot learn what its source understands. The Acceptor checks only
  that a handshake is at least as long as it expects, so a flags field
  appended to the handshake would be accepted and ignored by older servers.
  That would let a server send a new field only to sources that understand
  it.
- **One close message.** #19047 and #19701 each add this message, with the
  same tag and a one-byte payload, so they match on the wire. #19047 sends it
  without checking `acceptsClose`. One PR should carry the message and the
  other build on it, with the check.
- **One close receive per worker.** Instead of one close receive per server,
  the Communicator keeps one receive that matches every close message on its
  worker. It hands each message to the server it names, or drops it if that
  server has already ended. In the baseline, a close message that arrives after
  its server ended is never received: it stays in UCX's queue of unexpected
  messages for the life of the UCX worker, and with `UCX_RNDV_THRESH=0` the
  consumer's send never completes either.
- **Why the channel ended.** #19701 adds the producer's error to the last
  metadata message. That field is not behind a flag: a consumer reading from
  an older producer fails its query. An older consumer reading from a newer
  producer ignores the error, so it still reads a failed producer's end as a
  clean end and can miss rows, as before #19701. Changing the magic number
  would abort older workers, for the reason given under Root causes.
- **Further control messages.** The same flags let later changes add
  messages, for example for flow control, which #19859 approaches today by
  counting on each side.

### Rules for both halves

- Every event in the tables above goes through its half's entry. State
  changes happen under the lock that guards it: the output queue's lock on the
  producer side, the exchange queue's lock on the consumer side.
- Reader callbacks and promises run after that lock is released, as the code
  already does. Chunks and tables that an entry frees are freed after the lock
  is released, too.
- The lock order is the output queue manager's map, then the output queue. No
  lock of the `Task` is taken while either is held. Removal reads the task's
  state and error message before it takes the output queue's lock; they are
  final by then, because `Task::terminate()` sets the task's final state before
  it calls `removeTask()`. #19047 reads `Task::isRunning()` under the output
  queue's lock; under this rule, that read moves before the lock.
- `initializeTask()` for a task ID in Removed starts a new entry. Servers that
  still hold the old entry see it as Removed. Presto does not reuse task IDs,
  but the tests do.
- Entries treat chunks and tables as opaque handles and never look inside
  them, so the lifecycle does not depend on the backend that produced them.
  That keeps the transport engine-agnostic, as Velox's GPU acceleration guide
  describes it.
- Entries keep no memory accounting of their own. #18590 (open) charges the
  chunks that `UcxPartitionedOutput` queues to a per-query exchange pool in
  Velox's memory system, and a chunk releases its charge when it is freed. An
  entry's job is to free its chunks when its channel is released or ends with
  an error. No state that outlives a channel, such as a task entry in Removed,
  may hold a chunk.
- Each table is implemented as one function from the current state and an
  event to the next state and its outcome. Each row gets one test, run against
  the output queue or the exchange queue alone, without UCX.

### What changes where

| Class | Change |
|---|---|
| `UcxOutputQueueManager` | one map of task entries replaces four side maps |
| `UcxOutputQueue` | one state per destination; the outcome table; removal decided under its lock |
| `UcxExchangeServer` | reads its destination's state; reports the close message, endpoint closure and failed sends; a failed send fails the producer task; deletes results when the last send completes |
| `UcxExchangeQueue` | one entry per source; the outcome table; frees tables once the consumer failed or closed |
| `UcxExchangeSource` | reports every end through `end(outcome)`; drops its end-marker-delivered and registered flags; waits, with a timeout, for the handshake response before deciding on the close message |
| `UcxExchangeClient` | `close()` releases the entries |
| `Communicator` | keeps one close receive for the worker and hands each close message to its server |
| `Acceptor`, `EndpointRef` | unchanged |

Against the baseline, the design changes five behaviors, each one a fix:

- a send that fails while its endpoint stays open fails the producer task,
  instead of leaving it `RUNNING` until it is removed;
- the server deletes its destination's results when the send of the last
  metadata message completes, not when it is posted, so that a failed last
  send also fails the producer task instead of letting it finish;
- tables that arrive after the consumer failed or closed are freed;
- a close message that arrives after its server ended is received and
  dropped, instead of staying in UCX for the life of the worker;
- a source that closes during the handshake tells its server, if the server
  accepts the close message. With #19701 alone it never tells the server, and
  #19047 sends the message whether or not the server accepts it.

## Comparison with the HTTP exchange

The HTTP exchange is implemented across Velox and
Prestissimo (Presto's C++ worker) together. In Velox, the producer
side is `OutputBuffer` and the consumer side is `ExchangeClient`,
`ExchangeQueue` and `ExchangeSource`. Prestissimo adds `PrestoExchangeSource`
and the results endpoints of its `TaskManager`.

This section compares the HTTP exchange with the UCX exchange as this note
proposes it. Most of the UCX behavior described here already holds in the
baseline; the table marks what the proposal adds.

**The state lives in the same places.** The HTTP exchange keeps the producer's
state in one `OutputBuffer` per task, under one lock, over its destination
buffers. It keeps the consumer's state in one `ExchangeQueue` per client,
under one lock, with one source per producer. `UcxExchangeQueue` is modeled on
`ExchangeQueue`. This design puts each half's state machine into exactly
these two places.

**The way a producer failure reaches the consumer differs.** In the HTTP
exchange, the data path never reports it:

- When the producer task has failed or was aborted, `TaskManager::getResults()`
  waits for the request's maximum wait and returns an empty page that is not
  marked complete. The consumer keeps asking.
- A page is marked complete only when the producer's buffer delivers its end
  marker or the producer `Task` has finished. After a failure, neither
  happens, so a consumer never ends cleanly after lost output.
- The coordinator sees the failure in the producer's task status and aborts
  the query, including the waiting consumers.

The UCX exchange, from #19701 on, sends the producer's error in the last
metadata message, and the consumer fails at once. This avoids polling over a
UCX endpoint while the coordinator catches up, and it gives the consumer the
producer's message. The consumer's error is derived from the producer's, so
the query should still report the producer's error as its cause (see Open
questions).

**UCX gives stronger delivery guarantees, and they are enforced by the
receiver.** UCX promises only that a completed send's buffer may be reused.
For a message sent with rendezvous, that implies the receiver has matched it
and fetched the data. For a small message sent eagerly, such as every metadata
message, it implies nothing about the receiver. The design does not rely on
either:

- A source posts receives for exact, sequence-numbered tags, one chunk after
  another, and ends cleanly only after it has received the last metadata
  message. It cannot skip a chunk.
- If any message is lost, the source never sees the end. With
  `ucxx.error_handling` on, it fails on its endpoint's error. Tag receives
  belong to the UCX worker, not to an endpoint, so this goes through the
  endpoint's error callback (`onEndpointClosed()`). With error handling off,
  the source waits.
- On the producer side, a send's completion only decides when a chunk may be
  freed. That is always safe, because the design never retransmits: a failed
  transfer fails the channel.

UCX's synchronous tag send, which completes only after a remote match, cannot
be used here: it is not supported when peer error handling is enabled.

**No acknowledgment is needed.** The HTTP exchange retries every request for
up to `exchange.max-error-duration` (3 minutes by default). A response can be
lost after the producer sent it, so the consumer asks for the same sequence
number again, and the producer keeps the pages until the consumer acknowledges
them or deletes the results. The acknowledgment exists because of those
retries. The UCX exchange sends over reliable connections, has no retries,
and frees a chunk as soon as its send completes, which also keeps the data
path zero-copy.

**HTTP bounds every wait, and UCX does not.** Every HTTP request expires, and
the retries end after `exchange.max-error-duration`, so a consumer of a dead
producer always fails eventually. In the UCX exchange, failure detection
depends on `ucxx.error_handling`. With it off, a lost peer is never reported,
and an acknowledgment would not help without a timeout either. So the UCX
exchange gives stronger delivery guarantees, and the HTTP exchange gives
bounded waits (see Open questions).

| | HTTP exchange | UCX exchange, as proposed |
|---|---|---|
| producer failure | `getResults()` waits for the request's maximum wait and returns an empty page that is not marked complete. The consumer keeps asking until the coordinator sees the failure in the producer's task status and aborts the consumer | the server sends the producer's error in the last metadata message, and the consumer fails at once with that error |
| clean end | a page is marked complete when the producer's buffer delivers its end marker, or when the producer `Task` has finished. The consumer then deletes its results on the producer with `DELETE` | the server sends the last metadata message with `atEnd` set, and deletes the destination's results when that send completes (proposed; the baseline deletes them when the send is posted). The consumer ends after it has received that message |
| producer memory freed | when the consumer acknowledges a sequence number or deletes its results. Until then the producer keeps the pages, so that the consumer can ask for them again after an error | when the send of a chunk completes. The design never retransmits, so nothing is kept for a second attempt |
| transport errors | every request is retried for up to `exchange.max-error-duration`, 3 minutes by default. Only then does the consumer's queue get the error, and the consumer fails | a failed receive, an undecodable message or a failed allocation fails the source at once, and with it the consumer. A send that fails while its endpoint stays open fails the producer task (proposed) |
| dead peer | always detected in the end: every request expires, and the retries stop after `exchange.max-error-duration` | detected only with `ucxx.error_handling` on. UCX then reports it on the endpoint, and every server and source on that endpoint ends. With it off, a dead peer is never reported, and its channels wait until the coordinator aborts their tasks |
| consumer closes early | the consumer sends `DELETE` for its results, which calls `deleteResults()` on the producer. The request is retried, and a `DELETE` that keeps failing fails the consumer | the source sends the close message once, if its server set `acceptsClose`, and the server then deletes the destination's results. If the server ended first, the producer's Communicator receives the message and drops it (proposed; in the baseline, the message stays in UCX) |
| consumer before its producer exists | the request waits in a `PrestoTask` that has no Velox `Task` yet. It expires after its maximum wait, and the consumer asks again. A `PrestoTask` that never gets its Velox `Task` is dropped after 60 seconds without a heartbeat from the coordinator | the server's request waits in a Pending entry, without a timeout. `initializeTask()` turns the entry into the task's output queue, which then serves the request. `removeTask()` ends it with the error that the producer never initialized its output. If neither comes, the entry is dropped when its last waiting server closes, for example after the coordinator aborts the consumer |
| consumer after its producer was removed | the `PrestoTask` stays for `old-task-cleanup-ms`, 60 seconds by default, after the task terminated, and answers with empty pages. After that, a request creates a new `PrestoTask` without a Velox `Task`, which also answers with empty pages, until the coordinator aborts the consumer | while the producer's `Task` exists, the server is bound to the Removed entry and sends the last metadata message with the stored outcome, usually the producer's error. Once the `Task` is gone, the server's request waits in a new Pending entry, as in the row above |

## Alternatives considered

**A per-worker channel registry in the Communicator.** One registry per worker
would hold task entries and both halves of the channels on that worker, and the
output queue manager and the client would forward into it. Its main argument,
symmetry, does not hold: a channel's two halves run on different workers, so a
registry never holds both halves of one channel. The producer's decisions are
task-wide, so a lock per channel entry would need a task lock above it, which
is the output queue's lock again. It would also make the transport own Velox
chunks, unlike Velox's CPU exchange. It would not have prevented #19047 or the
missing producer error of #19701, which are protocol work either way.

**One umbrella singleton over the Communicator and the output queue
manager.** A per-worker runtime would own the transport, a channel registry and
the Velox adapters, wire them without global lookups, and define one start-up
and shutdown order. Its lifecycle design is the registry's, so the objections
above apply to it too. It adds three risks of its own:
- the worker, not the exchange, owns the Communicator's thread;
- GPU buffers must not be freed while rendezvous requests are in flight, which
  constrains the shutdown order;
- tasks hold only a `std::weak_ptr` to the output queue manager and skip
  `removeTask()` once it has expired, so the runtime would have to outlive
  every task.

**Single-threaded ownership on the Communicator's event loop.** All channel
state would live on the loop, and other threads would post events to it. The
event queue would then be the only place where threads meet. Velox calls the
output queue manager synchronously (`stats()`, `getUtilization()`,
`isOverutilized()`, `updateOutputBuffers()`), and producers call `enqueue()`
from their drivers, so these calls would have to read snapshots, and
backpressure futures would be fulfilled from the loop. One thread would also
be a CPU ceiling for all channels on a worker. The proposal keeps this option
open: each half's table is a function of state and event, which a loop could
run as well.

## Related change: retiring the intra-node path

Most of the intra-node path's advantage over the UCX path came from UCX being
unable to use `cuda_copy` for rendezvous while peer error handling was on.
openucx/ucx#11319, merged into UCX `master` on 2026-10-05, lets endpoints on
the same UCX worker (a UCP worker, in UCX's terms) use `cuda_copy` with peer
error handling. That covers the case the intra-node path serves: both tasks in
one process. After it merged, the author of #18921 no longer observed a
reliable speed-up from intra-node handshake improvements and closed that PR.
The same author notes that small messages can still go over PCIe unless the
rendezvous scheme is forced with `UCX_RNDV_SCHEME=get_zcopy`; in their TPC-H
benchmarks, that effect was within noise.

The fix is not in a UCX release yet; 1.23.0 does not include it. Once a
release with it is the supported baseline, the UCX path with default settings
(`ucxx.error_handling` on, `cudf.intra_node_exchange` off) is expected to
match the intra-node path, which should be measured before the removal. Until
then, the intra-node path should receive only fixes, and the new state
machines should keep its behavior: removing a task cancels its registry
entries, and a chunk published but not read counts as not delivered.
Removing the path deletes the registry and its handoff, together with the
races found in them.

## Migration

1. Agree on the order of the in-flight PRs that change channel endings:
   #19701, #19047 and #19859, with one close message and #19701's outcome on
   removal.
2. The producer half: fold the side maps into task entries, then give each
   destination its state and the outcome table, with one test per row.
3. The consumer half: give the exchange queue its source entries, then move
   the source to `end(outcome)`, with one test per row and one for each of the
   four close races.
4. Later protocol changes, each behind its own flag.

Steps 2 and 3 are independent of each other.

## Open questions

- #19859 counts reservations, queued chunks and transfers in flight per
  destination and task, to adapt flow control. #18590 charges the same producer
  memory to Velox's memory pools. Should flow control read those pools instead
  of keeping its own counts, and where should #19859's accounting of transfers
  in flight and of memory held downstream live?
- Receive buffers are not charged to Velox's memory system. #18590 leaves them
  out, because the source allocates them on the Communicator thread, while
  Velox expects memory to be allocated on driver threads (see the async RPC
  design note). The metadata message announces each chunk's size before the
  source posts the data receive, so the consumer's driver could reserve the
  buffer there. How would that work with backpressure?
- How long do workers other than Prestissimo keep a finished `Task`? A
  Removed entry lives exactly that long. A worker that drops a failed `Task`
  before the coordinator has aborted its consumers leaves late consumers
  waiting until the abort.
- When a consumer fails with the producer's error from the last metadata
  message, does the query report the producer's own error as its cause, or can
  the consumer's derived error be reported first?
- Does the UCX exchange need a bound on how long a channel may wait, for
  example an idle timeout per channel, when `ucxx.error_handling` is off?
- Is a consumer that connects after its producer's `Task` was destroyed worth
  handling beyond waiting until the coordinator aborts the query, as Presto's
  HTTP exchange does?
- Should the handshake get a flags field of its own, appended at its end, so
  that a server can learn what its source understands? Older servers would
  accept it and ignore it. #19701's producer error in the last metadata
  message could then have been sent only to sources that understand it.

## References

- Velox pull requests:
  - landed: #17752, #17753 (as commit d66364f743), #19064;
  - closed without landing: #18921;
  - open: #18590, #19047, #19701, #19859.
- UCX: openucx/ucx#11319, "UCT/CUDA/COPY: Qualify cuda_copy for RNDV with
  peer-failure EH".
