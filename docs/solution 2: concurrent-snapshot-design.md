# Solution 2: Concurrent Snapshot Design

## Status

This document describes the experimental snapshot alternative implemented by
`SnapshotPriceIngestionEngine`. It is not the selected baseline: the baseline
still synchronizes reads and writes on each `TickerState`, while the experiment
is retained for correctness and performance comparison.

## Goal

Allow ingestion and average queries for the same ticker to make progress
independently while giving every query a consistent view.

The intended behavior is:

1. A query starts when a ticker has five accepted quotes and captures snapshot
   `V5`.
2. A writer accepts quote six and publishes snapshot `V6`.
3. The in-flight query finishes using `V5`, so quote six is not part of that
   result.
4. A later query captures `V6` and includes quote six.

```text
Reader:  capture V5 ───────── calculate from V5 ───── return
Writer:          ingest quote 6 ─ publish V6
```

The reader does not observe a partially updated deque, sum, or count. It sees one
complete version even when a writer publishes a newer version concurrently.

## Define the consistency boundary

The design needs an explicit rule for deciding which quotes belong to a query:

- If a query captures `V5` before `V6` is published, it uses `V5`.
- If `V6` is published before the query captures a snapshot, a request for the
  latest value uses `V6`.

Concurrency alone cannot make a query ignore a quote that was already published
before the query began. Supporting that behavior requires an API boundary such
as a maximum sequence number, timestamp, or snapshot version:

```java
getTwoMinuteAverage(String ticker, long currentTimestamp, long maxSequenceNumber)
```

The initial proposal uses snapshot-at-query-start semantics because it keeps the
existing latest-average API simple.

## Proposed architecture

Each ticker would contain two forms of state:

1. **Writer-owned mutable state:** queues or a circular buffer, running sums,
   sequence metadata, and expiry bookkeeping.
2. **Published immutable snapshot:** a coherent version of the sums and counts
   that readers can safely retain.

```text
synchronized writers
        │
        ├── update mutable quote history and aggregates
        │
        └── publish immutable snapshot through AtomicReference
                                      │
                                      └── lock-free readers
```

Writers may remain synchronized with one another. The goal is to remove reader
participation from the writer lock, not necessarily to make same-ticker writes
lock-free.

## Snapshot model

A compact published view could be represented as an immutable record:

```java
record AverageSnapshot(
        long version,
        long lastSequenceNumber,
        long effectiveTimestampMillis,
        BigInteger twoMinuteSum,
        int twoMinuteCount,
        BigInteger fiveMinuteSum,
        int fiveMinuteCount) {
}
```

`TickerState` could publish it through an `AtomicReference`:

```java
private final AtomicReference<AverageSnapshot> publishedSnapshot;
```

`AtomicReference` updates and retrieves the snapshot reference atomically. The
snapshot must remain immutable after publication; otherwise an older reader could
still observe later mutations through the retained reference.

## Writer path

The writer would continue to own mutation of quote history:

```java
synchronized boolean ingest(Quote quote) {
    validateSequenceAndTimestamp(quote);
    expire(quote.timestampMillis());
    addToWindows(quote);
    updateOrderingMetadata(quote);

    publishedSnapshot.set(createSnapshot());
    return true;
}
```

Publication occurs only after every field needed by the new snapshot has been
calculated. Readers therefore receive either the preceding complete snapshot or
the new complete snapshot, never an intermediate combination.

## Reader path

A reader captures the reference once and uses only that local snapshot:

```java
Optional<PriceAverage> average(Window window) {
    AverageSnapshot snapshot = publishedSnapshot.get();
    return toAverage(snapshot, window);
}
```

If a writer publishes `V6` after the `get()`, the reader's local variable still
refers to immutable `V5`. The writer does not wait for that reader, and the reader
does not retry merely because a newer version exists.

## Time-based expiry

Snapshot publication during ingestion is insufficient by itself. A rolling
average changes when quotes expire even if no new quote arrives.

The current implementation handles this by expiring quotes inside a synchronized
average query. A non-mutating reader cannot do that without either scanning or
changing shared state. Possible writer-side solutions include:

- A scheduled expiry task that advances ticker windows and publishes new
  snapshots
- A single-owner event loop with quote and timer events
- Fixed time buckets with a background cursor that expires completed buckets
- A serving contract that permits snapshots to remain unchanged until the next
  ingestion event, if that reduced freshness is explicitly acceptable

The selected design must define expiry precision, scheduler delays, idle ticker
handling, and snapshot freshness. Without that work, lock-free reads would return
stale aggregates when time advances without new quotes.

## Why other locks do not provide the same semantics

### Read-write lock

A `ReadWriteLock` allows multiple readers, but its write lock remains exclusive.
Readers and a writer cannot operate on the protected mutable state at the same
time. It may improve read concurrency, but it does not let an in-flight reader
retain an old version while a writer publishes a new one.

It is also a poor fit while average queries call `expire()`, because expiry is a
mutation and would require the write lock.

### Stamped lock

A `StampedLock` supports optimistic reads that initially run without a read lock.
If a writer obtains the write lock during that read, validation fails and the
reader normally retries. A retry may observe the new six-quote state rather than
finish from the previous five-quote state.

An optimistic lock therefore reduces locking in some workloads, but it does not
preserve an old view of mutable objects. Immutable or versioned state is still
needed for the snapshot semantics described here.

## Correctness requirements

Any implementation should preserve these invariants:

- A snapshot's sum and count come from the same state version.
- Published snapshots are never mutated.
- Snapshot versions and accepted sequence numbers only increase.
- A failed or rejected ingestion does not publish a new version.
- Two-minute and five-minute values are published together.
- Exact window boundaries retain the existing
  `(currentTimestamp - duration, currentTimestamp]` semantics.
- Expiry can publish a new version even when no quote arrives.

## Tests needed before adoption

1. A reader capturing `V5` still returns `V5` after a writer publishes `V6`.
2. A reader starting after `V6` publication returns `V6`.
3. No query observes mismatched sums and counts under sustained concurrency.
4. Duplicate or stale sequences do not change the published version.
5. Expiry publishes fresh snapshots for idle tickers.
6. Exact two-minute and five-minute boundaries match the baseline.
7. Multiple same-ticker writers cannot lose an accepted quote.
8. Load tests compare latency, allocation, memory, and throughput with the
   synchronized baseline.

## Trade-offs

This design can make reads short and independent from same-ticker writes, but it
adds versioning, publication, background expiry, and more concurrency tests.
Publishing compact sum/count records is inexpensive relative to copying complete
quote deques, but the allocation rate still needs measurement at the target load.

The design should remain an experiment until profiling demonstrates that
same-ticker reader/writer contention is a real bottleneck.

## References

- [Java 17 `AtomicReference`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/atomic/AtomicReference.html)
- [Java 17 `StampedLock`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/locks/StampedLock.html)
- [Java 17 `ReadWriteLock`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/locks/ReadWriteLock.html)
