# Price Ingestion Engine

An in-memory Java implementation of rolling two-minute and five-minute ticker
price averages.

## Baseline design

- A `ConcurrentHashMap` isolates state by ticker.
- Each ticker has its own lock, so unrelated tickers do not block each other.
- Two timestamp-ordered queues retain only the entries required by each window.
- Running integer sums and counts make average lookup constant-time after expired
  entries have been removed.
- Individual prices use fixed-point millionths in a `long`; running sums use
  `BigInteger` so aggregate values cannot overflow.
- Sequence numbers must increase for each ticker. A quote whose sequence number
  is not greater than the last accepted value is rejected as a duplicate or
  stale quote.

The baseline deliberately keeps synchronization simple. Reads currently acquire
the ticker lock because advancing time can expire entries. An experimental
snapshot implementation separates writer-owned state from immutable published
snapshots to make the normal read path lock-free.

## How the rolling average works

The engine does not recalculate an average by scanning every quote in a window.
Each ticker instead maintains two `ArrayDeque` instances, one for the two-minute
window and one for the five-minute window, together with a running sum for each
deque.

For every accepted quote, the ticker state:

1. Removes expired quotes from the front of each deque and subtracts their prices
   from the corresponding running sum.
2. Appends the new quote to the back of both deques.
3. Adds the new price to both running sums.

An average query first performs the same expiry step and then returns the selected
window's running sum and deque size. `PriceAverage` divides that sum by the count
only when a decimal representation is requested.

The deque's front and back play the same conceptual roles as the left and right
pointers in a two-pointer sliding-window algorithm. There are no explicit pointer
variables because `ArrayDeque` manages its internal head and tail positions.

For example, if the active prices are `100`, `200`, and `300`, the state already
contains `sum = 600` and `count = 3`, so the average is derived as `600 / 3`.
When `100` expires, it is removed from the front and the state becomes `sum = 500`
and `count = 2`; no scan of `200` and `300` is needed.

### Complexity

- Appending a quote and reading the maintained sum and count use amortized O(1)
  deque operations.
- Producing an average after expiry is O(1).
- Expiring `k` old quotes is O(k) for that operation. Across the lifetime of the
  engine, expiry is amortized O(1) per quote because each quote is appended once
  and removed once from each deque.
- A read is therefore not strict worst-case O(1) today: it may remove expired
  quotes and it acquires the ticker's monitor while doing so.

`ArrayDeque` is not thread-safe on its own. The two deques and their sums are
accessed only from synchronized methods on a single ticker state. This keeps each
sum, count, and deque consistent while allowing unrelated tickers to proceed
independently through the outer `ConcurrentHashMap`.

## Implementation status and future experiments

Each item states its current status. The harness and expanded correctness
coverage are established tooling, while the snapshot engine remains experimental.
The other architectures are possible future iterations, not claims about the
current implementation. An alternative should replace the simpler baseline only
when realistic measurements show a worthwhile improvement.

1. **Build a performance harness (implemented).** The configurable harness runs
   concurrent producers and readers for 30 seconds at a target 50,000 quotes per
   second, models hot and cold tickers, and reports throughput, sampled latency
   percentiles, heap, allocation, GC, and rejection metrics. See the
   [performance harness guide](docs/performance-harness.md).
2. **Publish immutable snapshots for lock-free reads (experimental).** The
   [`SnapshotPriceIngestionEngine`](src/main/java/prices/engine/SnapshotPriceIngestionEngine.java)
   keeps writer-owned quote history and publishes coherent sum/count snapshots.
   Valid snapshots make reads lock-free; an idle ticker whose snapshot reaches an
   expiry boundary uses a synchronized refresh path. Profiling showed better
   normal read latency and no ticker-monitor contention, but slower writes and
   higher allocation, so it has not replaced the baseline. See
   [Solution 2](<docs/solution 2: concurrent-snapshot-design.md>) and the
   [measured comparison](docs/solution-results-current-performance-snapshot.md#snapshot-fast-read-experiment).
3. **Evaluate an exact per-quote circular buffer.** Retain every accepted quote
   once and maintain separate two-minute and five-minute head positions, sums,
   and counts. Expiring `k` quotes is still O(k) for that operation and amortized
   O(1) per quote. Unlike the current two-deque design, each quote is stored once;
   a fixed-capacity buffer also requires a maximum-rate assumption and an explicit
   overflow policy.
4. **Evaluate aggregate time buckets in a fixed ring.** Store one sum/count pair
   per time slice and expire a completed bucket by subtracting its aggregate.
   This gives bounded state and O(1) bucket rotation, but it no longer preserves
   individual quotes. Bucket width controls boundary precision: one-minute
   buckets can be off by almost a minute, while smaller buckets use more slots;
   exact semantics require special handling for the partially overlapping
   boundary bucket.
5. **Shard tickers by thread affinity.** Route each ticker to one owning worker so
   writes for that ticker are serialized without a per-ticker lock. This introduces
   queues, so capacity, backpressure, overload, shutdown, and failure behavior must
   be designed explicitly. Current hot/cold and Zipf profiles do not show enough
   ticker-lock contention to justify the additional queue hop as a performance
   optimization; reconsider it if asynchronous ingestion or explicit backpressure
   becomes a requirement.
6. **Compare synchronization strategies after profiling.** `ReadWriteLock`,
   atomic snapshots, concurrent queues, CAS-based state, or LMAX Disruptor are
   candidates only when measurements identify the bottleneck. In particular, a
   read-write lock offers little benefit while reads still mutate state during
   expiry, and independent atomic fields do not automatically provide a consistent
   sum/count snapshot.
7. **Extend correctness coverage (implemented).** Lincheck now combines
   same-ticker writes with reads at normal and exact expiry timestamps. Seeded
   differential tests compare both engines with a scan-based reference across
   duplicates and repeated time jumps; deterministic tests cover sums beyond the
   `long` range, large expiry batches, and a concurrent snapshot refresh/write race.
   The load harness covers concurrent accuracy, while
   [`expiry-soak-test.sh`](scripts/expiry-soak-test.sh) runs both engines with
   hot/cold and Zipf traffic beyond the five-minute boundary. The soak is kept
   separate from the normal fast test suite. Out-of-order quote handling remains
   deferred by the exercise.

## Project structure

- `src/main/java/prices`: public engine interface
- `src/main/java/prices/engine`: in-memory engine implementation and per-ticker state
- `src/main/java/prices/model`: quote and average records
- `src/main/java/prices/enums`: rolling-window definitions
- `src/main/java/prices/load`: dependency-free performance harness and metrics

## Further documentation

- [Solution 1: Simple Sliding Window Design](<docs/solution 1: simple-sliding-window-design.md>): selected
  starting-point options, current architecture, alternatives not implemented,
  and known constraint gaps.
- [Using Apache Flink for Price Averages](docs/flink-architecture.md): how the
  exercise maps to a continuous production stream, keyed ticker state, multiple
  time intervals, and a materialized query model.
- [Solution 2: Concurrent Snapshot Design](<docs/solution 2: concurrent-snapshot-design.md>):
  a proposed immutable-snapshot model that allows same-ticker ingestion and
  average reads to proceed independently while each reader remains pinned to one
  state version.
- [Performance Harness](docs/performance-harness.md): workload model, metrics,
  configuration, commands, and guidance for interpreting results.
- [Solution Results: Current Performance Snapshot](docs/solution-results-current-performance-snapshot.md):
  dated correctness, load, latency, memory, GC, and JFR baselines for comparing
  future implementation experiments.

## Assumptions

- Quote timestamps and sequence numbers are non-decreasing per ticker.
- Windows use `(current time - duration, current time]`; a quote exactly on the
  lower boundary is expired.
- Historical queries are unsupported. Query timestamps cannot precede the most
  recently accepted quote for that ticker.
- A missing ticker or an empty window returns `Optional.empty()`.

## Run the tests

```bash
./scripts/test.sh
```

The project requires Java 17 or newer. The Gradle test suite includes the
functional tests, load-harness support tests, and
[Lincheck](https://github.com/JetBrains/lincheck) concurrency correctness tests.

To run only the Lincheck tests, use:

```bash
./scripts/lincheck-test.sh
```

The Lincheck suite explores concurrent same-ticker ingestion and average reads
using both bounded model checking and stress execution.

Run the default five-second warm-up and 30-second load measurement with:

```bash
./scripts/load-test.sh
```

Capture the same workload with Java Flight Recorder for CPU, allocation, GC,
and monitor-contention analysis:

```bash
./scripts/profile-load-test.sh
```

Run the experimental immutable-snapshot read path against the same harness with:

```bash
./scripts/profile-load-test.sh --engine=snapshot
```

## References

- [Price Ingestion Engine exercise](https://transferwise.atlassian.net/wiki/spaces/APE/pages/4235501356/Exercise+Price+Ingestion+Engine)
- [Java 17 `ArrayDeque` documentation](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ArrayDeque.html)
- [Java 17 concurrency package documentation](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/package-summary.html)
