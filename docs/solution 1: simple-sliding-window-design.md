# Solution 1: Simple Sliding Window Design

This document summarizes the design choices made for the
[Price Ingestion Engine exercise starting points](https://transferwise.atlassian.net/wiki/spaces/APE/pages/4235501356/Exercise+Price+Ingestion+Engine#Starting-points),
particularly the alternatives suggested under **Starting points**.

## Selected design

The current implementation is a single-JVM, in-memory engine built around
independent per-ticker state:

```text
ConcurrentHashMap<ticker, TickerState>
                         |
                         +-- synchronized ingest and average operations
                                  |
                                  +-- two-minute ArrayDeque + running sum
                                  +-- five-minute ArrayDeque + running sum
```

`ConcurrentHashMap` provides concurrent access to the ticker registry. Each
ticker has a separate `TickerState`, and its mutable queues, sums, counts, and
ordering metadata are protected by synchronized methods. Operations for AAPL do
not take the lock used for MSFT, while readers and writers for AAPL share the
same per-ticker monitor.

## Starting-point decisions

| Area | Options suggested by the exercise | Selected approach |
| --- | --- | --- |
| History representation | Circular buffer, time buckets, sliding windows | Two sliding `ArrayDeque` windows per ticker |
| Average calculation | Retain and scan history, or maintain aggregates | Running sum and count for each window |
| Lock granularity | Global lock, per-ticker lock, lock-free state, thread affinity | Per-ticker synchronization |
| Ticker registry | Core Java concurrent collections | `ConcurrentHashMap` |
| Reader/writer interaction | Shared lock, read-write lock, lock-free publication | Shared per-ticker monitor |
| Reception and processing | Direct calls or a separate receiver/worker queue | Direct synchronous method calls |
| Build tooling | Plain `javac`, Maven, or Gradle | Gradle for tests; plain `javac` remains available for the load harness |
| Latency measurement | `System.nanoTime()` | `System.nanoTime()` around sampled operations |
| Memory measurement | Runtime/JVM facilities | `MemoryMXBean`, GC beans, and thread-allocation measurements |
| Concurrency testing | Multiple producers and readers | Load harness plus Lincheck model-checking and stress tests |

### Sliding-window storage

Each accepted quote is appended to both the two-minute and five-minute queues.
The price is added to the corresponding running sums. When time advances,
expired quotes are removed from the front of each queue and subtracted from its
sum.

Producing an average from the maintained sum and count does not scan all active
quotes. Expiring `k` quotes is O(k) for that operation, but amortized O(1) per
quote because each quote is appended and removed once from each queue.

Prices are represented as fixed-point millionths in a `long`. Sum updates use
exact arithmetic so overflow throws rather than silently corrupting an average.
Duplicate and stale sequence numbers are rejected per ticker.

### Synchronization

The implementation chose the simplest isolated locking model:

- There is no global engine lock.
- Every ticker owns an independent state object.
- Both ingestion and average queries synchronize on that ticker's state.
- Different tickers can progress concurrently.
- A reader and writer for the same ticker cannot be inside `TickerState` at the
  same time.

Thread affinity is used by the load generator, where each ticker belongs to one
producer to preserve ordered timestamps and sequences. It is not part of the
engine itself; callers may still invoke the engine from multiple threads.

### Direct processing

The public API invokes the in-memory engine directly. There is no receiver
queue, worker queue, queue capacity, backpressure policy, or queue-depth metric.
This keeps the baseline small and means measured operation latency includes the
engine call itself rather than time spent waiting in an application queue.

## Alternatives not implemented

The current engine does not use:

- A global lock
- A circular buffer or fixed-size ring
- One-second time buckets
- `ReentrantLock`, `ReadWriteLock`, or `StampedLock`
- Lock-free state or CAS loops for ticker aggregates
- Immutable snapshots published through `AtomicReference`
- LMAX Disruptor
- A receiver/worker ingestion pipeline
- Flink or other distributed state

The [concurrent snapshot design](<solution 2: concurrent-snapshot-design.md>) describes a
possible `AtomicReference` publication model for lock-free readers. The
[Flink architecture](flink-architecture.md) describes a production-oriented
distributed alternative. Both are design explorations, not current behavior.

## Requirement alignment and gaps

| Requirement | Current status |
| --- | --- |
| Precise averages | Fixed-point prices and exact running sums avoid floating-point accumulation errors |
| Duplicate detection | Duplicate and stale sequence numbers are rejected per ticker |
| Independent ticker concurrency | Per-ticker state prevents unrelated tickers from sharing one lock |
| Avoid read-time history scans | Reads use maintained sums and counts |
| Strict worst-case O(1) reads | Not fully met: a read may expire `k` old quotes |
| Reads never block ingestion | Not fully met: same-ticker reads and writes share a monitor |
| 50,000 quotes per second | Demonstrated by a local load run, not established as a universal capacity guarantee |
| Bounded memory | Windows expire old quotes, but the recorded 35-second run was shorter than the five-minute retention period and does not establish steady-state heap usage |

The two read-path gaps are deliberate properties of the simple baseline. A more
complex snapshot, bucketed, or single-owner design should replace it only when
measurements show that same-ticker contention or expiry latency is a material
bottleneck.

## Testing and measurement

The test strategy has two complementary layers:

1. **Lincheck correctness tests** execute same-ticker writes and reads under
   model checking and stress execution, verifying that observed results remain
   linearizable.
2. **The performance harness** runs multiple producer and reader threads over a
   hot/cold ticker distribution. It reports throughput, sampled latency
   percentiles, quote rejection, heap usage, allocation, garbage collection, and
   a post-phase comparison against an independent scan-based reference history.

The harness defaults to a five-second warm-up and a 30-second measurement at
50,000 quote writes and 50,000 reads per second across 10,000 tickers. Each
ticker has one producer in this workload, so the harness measures concurrent
cross-ticker writes and reads but does not create same-ticker writer contention.
Lincheck covers that correctness scenario separately.

### How the concurrent load is scheduled

With the default configuration, the harness creates a fixed pool of eight
workers: four producer threads and four reader threads. All workers first signal
that they are ready and then wait on the same start latch, so the measured phase
releases them together.

The workers are rate-paced rather than allowed to run as fast as possible. The
50,000 writes-per-second target is divided across the four producers, and the
50,000 reads-per-second target is divided across the four readers. This gives
each default worker a target of 12,500 operations per second, or approximately
one operation every 80 microseconds. A delayed worker may run immediately to
catch up with its schedule. The operating system and JVM still decide exactly
when each thread executes, so the harness does not know or retain a complete
per-operation timeline showing the exact ordering and overlap of every read and
write.

During the phase:

- Producers choose a ticker from the configured hot/cold distribution and call
  `ingest` directly.
- Readers independently choose a ticker from the same distribution, randomly
  select the two-minute or five-minute window, and query the engine directly.
- The operations therefore overlap naturally and create same-ticker
  reader/writer contention as well as concurrency across different tickers.
- Concurrent reads contribute throughput, latency, empty-result, retry, and
  failure measurements. Their individual returned averages are not recorded and
  checked against a point-in-time reference result.

After the timed workers have stopped, the accuracy audit queries both windows
for every ticker and compares the engine with the independent reference history.
It therefore verifies that the concurrent workload produced the correct final
state; it does not prove that every individual read during the workload returned
the correct average at its precise concurrency point. Lincheck provides the
complementary linearizability check for deliberately explored read/write
interleavings.

The report shows the completed read count and achieved read rate, so a run that
falls behind its configured read schedule is visible. However, the current
`SUSTAINED` verdict only requires at least 99% of the target write rate, zero
rejected writes, and zero final-state accuracy mismatches. It does not currently
fail a run for missing the target read rate. There is also no queue in which
individual read requests could be lost: each counted read is a synchronous
engine call completed by a reader worker.

For the accuracy audit, expected-empty ticker/window results are excluded from
numeric-average comparisons. Unexpected empty or non-empty results remain
mismatches. Because the audit runs after workers stop, it validates the final
state created by the concurrent workload without guessing how an overlapping
individual read should be linearized.

### How Lincheck exercises concurrency

`@Operation` marks a test-state method that Lincheck may select when generating
an execution scenario. It does not run the method itself or assign it a
wall-clock time. In this suite, the available operations are:

- Ingest `AAPL` at timestamp `1000` with sequence `1` and price `100000000`.
- Ingest `AAPL` at timestamp `1000` with sequence `2` and price `300000000`.
- Read the two-minute `AAPL` average at timestamp `1000`, returning either
  `empty` or a snapshot containing the total price and quote count.
- Read the five-minute `AAPL` average at timestamp `1000`, returning the same
  result representation.

Both quotes deliberately have the same business timestamp, but that does not
mean the calls are scheduled at a particular real-world second. Lincheck builds
small histories from these operations and executes their parallel section
across three threads. It records every operation result and checks whether the
whole concurrent history can be explained by a legal sequential ordering. A
read may therefore legitimately observe the state before either write, after
one accepted write, or after both accepted writes, provided its result fits a
valid point between its invocation and completion.

The generated scenarios also use Lincheck 3.7's defaults of five sequential
operations before the parallel section and five sequential operations after it.
Those sections may contain average reads, but the test does not declare a
guaranteed, dedicated final-average assertion.

There are currently two Lincheck tests over these same operations:

1. **Model checking:** 10 generated scenarios, each attempted up to 100 times,
   with three threads and two parallel operations per thread. Lincheck controls
   scheduling to explore different interleavings and may stop invoking a
   scenario early once it has explored the relevant possibilities.
2. **Stress testing:** 20 generated scenarios, each executed 100 times, with
   three threads and three parallel operations per thread. It relies on repeated
   execution under JVM thread scheduling to expose concurrency failures.

The current Lincheck suite covers concurrent reads through both public average
windows, but it does not cover window expiry, advancing timestamps, multiple
tickers, or a specifically asserted terminal average. Those behaviors are
covered elsewhere only to the extent described by the functional tests and
performance harness.

For bottleneck investigation, `scripts/profile-load-test.sh` runs the load
harness with Java Flight Recorder configured for method sampling, allocation,
garbage collection, and monitor contention. This can distinguish CPU or memory
pressure from threads waiting on the per-ticker monitor. The direct-processing
baseline has no receiver/worker queue to profile.

An experimental `SnapshotPriceIngestionEngine` can be selected in the harness
with `--engine=snapshot`. It retains synchronized writes but publishes immutable
sum/count snapshots for lock-free reads while the snapshot is valid. A stale
snapshot uses a synchronized expiry-and-republish slow path. This alternative is
not the selected implementation; its first comparison is recorded in the
[current performance snapshot](solution-results-current-performance-snapshot.md).

See [Performance Harness](performance-harness.md) for configuration and result
interpretation. The dated [current performance snapshot](solution-results-current-performance-snapshot.md)
preserves the functional, Lincheck, default-load, and two-times-rate JFR results
for comparison with future implementations.
