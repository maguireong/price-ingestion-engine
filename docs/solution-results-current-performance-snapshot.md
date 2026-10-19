# Solution Results: Current Implementation Performance Snapshot

## Purpose

This document preserves a dated performance and correctness baseline for the
current implementation. Future lock-free, thread-affinity, snapshot, or queue
experiments should run the same commands on the same environment and compare
their results with this snapshot before replacing the simpler design.

Snapshot date: **2026-09-12**

> **Historical aggregate representation:** These measurements were captured
> before running sums and `PriceAverage.totalPriceMicros` changed from `long` to
> `BigInteger`. They remain useful as the former fixed-width baseline, but their
> allocation, GC, heap, and latency values should not be treated as measurements
> of the current implementation. Re-profile before making a performance decision.

## Implementation under test

The baseline uses:

- A `ConcurrentHashMap` ticker registry
- One mutable `TickerState` per ticker
- `synchronized` ingestion and average operations on each `TickerState`
- Fixed-width `long` running sums, which have since been replaced by `BigInteger`
- Separate two-minute and five-minute `ArrayDeque` histories
- Running sum and count for each window
- Direct synchronous engine calls with no receiver or worker queue

The load generator assigns each ticker to one producer, so the load test has no
same-ticker writer contention. Readers still contend with writers when they
select the same ticker. Lincheck separately exercises same-ticker writes and
reads.

## Environment

| Item | Value |
| --- | --- |
| Operating system | macOS 26.6.2, Darwin 25.6.0 |
| Architecture | arm64 |
| JVM | OpenJDK 17.0.14, Homebrew build 17.0.14+0 |
| JFR format | 2.1 |

These are developer-machine observations, not production capacity guarantees.

## Complete Gradle test suite

Command:

```bash
./scripts/test.sh
```

| Test group | Tests | Failures | Recorded test-body time |
| --- | ---: | ---: | ---: |
| Baseline engine functional tests | 6 | 0 | 0.008 s |
| Snapshot engine functional tests | 4 | 0 | 0.001 s |
| Load-harness support tests | 11 | 0 | 0.010 s |
| Baseline Lincheck model checking and stress | 2 | 0 | 3.813 s |
| Snapshot Lincheck model checking and stress | 2 | 0 | 2.326 s |
| **Total** | **25** | **0** | **6.158 s** |

Gradle's JUnit XML reports test-body time rather than the complete Gradle process
time, so configuration, compilation, and daemon startup are excluded.

The Lincheck tests use four available operations:

- Lower-sequence same-ticker ingestion
- Higher-sequence same-ticker ingestion
- Two-minute same-ticker average read
- Five-minute same-ticker average read

Model checking ran 10 generated scenarios with up to 100 executions per
scenario, three threads, and two parallel operations per thread. Stress testing
ran 20 generated scenarios with 100 executions per scenario, three threads, and
three parallel operations per thread.

## Default unprofiled load baseline

Command:

```bash
./scripts/load-test.sh
```

Configuration: five-second warm-up, 30-second measurement, 50,000 writes/s,
50,000 reads/s, 10,000 tickers, four producers, four readers, and one latency
sample per 100 operations.

| Metric | Observed result |
| --- | ---: |
| Writes | 1,500,000 attempted; 1,500,000 accepted; 0 rejected |
| Write rate | 50,000/s |
| Reads | 1,500,000 completed; 112 empty; 0 timestamp retries |
| Read rate | 50,000/s |
| Final-state accuracy | 20,000 non-empty averages checked; 0 mismatches |
| Write latency | p50 0.333 us; p95 0.708 us; p99 1.833 us; max 70.833 us |
| Read latency | p50 0.500 us; p95 0.916 us; p99 2.417 us; max 11,268.791 us |
| Heap | 32.3 MiB start; 150.3 MiB peak; 149.5 MiB end |
| Worker allocation | 186.2 MiB; 6.2 MiB/s; 65.1 bytes/operation |
| Measurement-phase GC | 8 collections; 91 ms collection time |
| Queue | Not applicable |
| Verdict | SUSTAINED |

The single read-latency maximum is an outlier and should not be treated as a
stable characteristic without repeated runs. The p99 values are more useful for
baseline comparison.

## Two-times-rate JFR profile

Command:

```bash
./scripts/profile-load-test.sh \
  --target-quotes-per-second=100000 \
  --target-reads-per-second=100000
```

This run used the same five-second warm-up, 30-second measurement, ticker count,
worker count, traffic distribution, and sampling interval as the default run.

Doubling the requested quote and read rates does not change the algorithm or the
expected results. It increases pressure in several areas:

- **CPU:** the existing worker threads spend less time parked and more time
  ingesting quotes and serving average queries.
- **Ticker-lock contention:** reads and writes reach hot tickers more frequently,
  increasing the chance that they compete for the same ticker monitor.
- **Allocation and retained memory:** twice as many accepted quotes create about
  twice as many quote objects and retained queue entries during a run that is
  shorter than the five-minute window.
- **Garbage collection:** the higher allocation rate can cause more frequent or
  longer collections.
- **Latency:** CPU saturation, monitor contention, or garbage collection can
  increase tail latency even when median latency remains low.
- **Steady-state expiry:** at the higher quote rate, more quotes can reach an
  expiry boundary together, increasing the size of an individual `O(k)` cleanup
  batch. This run did not reach the five-minute boundary, so it does not measure
  that effect.

If the machine cannot sustain the requested rate, the expected signals are
achieved throughput below the configured target, increasing latency, greater
monitor wait, more timestamp retries, or increased CPU and GC pressure.

| Metric | Observed result |
| --- | ---: |
| Writes | 3,000,000 attempted; 3,000,000 accepted; 0 rejected |
| Write rate | 100,000/s |
| Reads | 2,999,999 completed; 0 empty; 5 timestamp retries |
| Read rate | Approximately 100,000/s |
| Final-state accuracy | 20,000 non-empty averages checked; 0 mismatches |
| Write latency | p50 0.333 us; p95 0.667 us; p99 1.708 us; max 55.708 us |
| Read latency | p50 0.541 us; p95 1.125 us; p99 3.125 us; max 2,984.917 us |
| Heap | 43.2 MiB start; 317.5 MiB peak and end |
| Worker allocation | 386.0 MiB; 12.9 MiB/s; 67.5 bytes/operation |
| Measurement-phase GC | 9 collections; 134 ms collection time |
| Queue | Not applicable |
| Verdict | SUSTAINED |

Recording-wide JFR signals:

| Signal | Observed result |
| --- | ---: |
| `TickerState` monitor contention at least 1 us | 15 events |
| Total recorded `TickerState` wait | 68.548 ms |
| Longest recorded `TickerState` wait | 18.325 ms |
| All monitor contention at least 1 us | 53 events |
| Runnable CPU samples | 91 |
| Allocation samples | 2,795 |
| Recording-wide GC | 14 collections; 297.208 ms |
| Deliberate pacing parks | 6,468,620 |

The recording-wide GC numbers include JVM startup, warm-up, measurement, and
the final audit, while the harness GC metric covers only the measured phase. The
large thread-park count is expected because the harness parks workers between
scheduled operations; it is not evidence of ticker-lock contention.

## Baseline conclusion

The synchronized per-ticker implementation sustained both the required rate and
twice that rate on this machine. The two-times-rate profile did not expose a
sustained CPU, GC, or ticker-monitor bottleneck. Memory grew with the number of
retained quotes because both runs were shorter than the five-minute window.

A future alternative should be compared using at least:

- Correctness failures and final-state mismatches
- Achieved write and read throughput
- Write and read p50, p95, and p99 latency
- Allocation per operation and heap growth
- GC count and collection time
- `TickerState` monitor wait, CAS retries, or queue depth as applicable
- Behavior at a rate high enough to expose saturation

JFR adds overhead, so compare profiled runs with other profiled runs and
unprofiled runs with other unprofiled runs.

## Snapshot fast-read experiment

The experimental `SnapshotPriceIngestionEngine` keeps synchronized writer-owned
deques and running sums, then atomically publishes an immutable sum/count view.
A read uses one atomic snapshot lookup while that view remains valid. The first
read at or beyond the next expiry boundary enters a synchronized slow path,
removes expired quotes, and publishes a fresh snapshot.

Command:

```bash
./scripts/profile-load-test.sh \
  --engine=snapshot \
  --target-quotes-per-second=100000 \
  --target-reads-per-second=100000
```

| Metric | Synchronized baseline | Snapshot experiment |
| --- | ---: | ---: |
| Writes accepted | 3,000,000 | 3,000,000 |
| Reads completed | 2,999,999 | 3,000,000 |
| Accuracy mismatches | 0 | 0 |
| Write p50 | 0.333 us | 0.375 us |
| Write p95 | 0.667 us | 0.792 us |
| Write p99 | 1.708 us | 2.125 us |
| Read p50 | 0.541 us | 0.417 us |
| Read p95 | 1.125 us | 0.833 us |
| Read p99 | 3.125 us | 2.125 us |
| Worker allocation | 67.5 bytes/operation | 99.5 bytes/operation |
| Peak heap | 317.5 MiB | 346.0 MiB |
| Measurement-phase GC | 9 collections; 134 ms | 10 collections; 145 ms |
| Ticker monitor contention at least 1 us | 15 events; 68.548 ms | 0 events |
| Fast snapshot reads | Not applicable | 3,000,000, or 100% |
| Stale-refresh reads | Not applicable | 0 |

In this single run, snapshot publication improved read p50, p95, and p99 while
making write percentiles and allocation worse. The run is only 35 seconds long,
so it never reaches the first two-minute expiry boundary. All measured reads
therefore used the fast path. The slow path provides correct results for idle or
expired tickers; it is not expected to be faster, and its latency still needs a
separate expiry-focused workload.

The experiment remains an alternative rather than the selected implementation.
Repeated runs and an expiry workload are required before drawing a replacement
decision from these measurements.

## Two-minute-expiry comparison

Both engines were also run unprofiled for 150 measured seconds after a
five-second warm-up. This crosses the two-minute boundary and exercises removal
from the two-minute window. It does not reach the five-minute expiry boundary.

Commands:

```bash
./scripts/load-test.sh --engine=baseline --duration-seconds=150
./scripts/load-test.sh --engine=snapshot --duration-seconds=150
```

| Metric | Synchronized baseline | Snapshot experiment |
| --- | ---: | ---: |
| Writes accepted | 7,500,000 | 7,500,000 |
| Reads completed | 7,500,000 | 7,500,000 |
| Write/read rate | 50,000/s / 50,000/s | 50,000/s / 50,000/s |
| Accuracy mismatches | 0 | 0 |
| Write p50 | 0.334 us | 0.458 us |
| Write p95 | 0.750 us | 1.417 us |
| Write p99 | 2.000 us | 2.583 us |
| Write max | 200.666 us | 736.583 us |
| Read p50 | 0.542 us | 0.500 us |
| Read p95 | 1.292 us | 1.458 us |
| Read p99 | 3.166 us | 2.584 us |
| Read max | 465.708 us | 8,686.250 us |
| Worker allocation | 939.1 MiB; 65.6 bytes/operation | 1,418.1 MiB; 99.2 bytes/operation |
| Peak heap | 518.8 MiB | 551.9 MiB |
| GC | 26 collections; 379 ms | 31 collections; 381 ms |
| Fast snapshot reads | Not applicable | 7,150,943 |
| Stale-path entries | Not applicable | 348,945 |
| Verdict | SUSTAINED | SUSTAINED |

The snapshot path classified 7,499,888 reads: 95.347% entered the fast path and
4.653% detected a stale snapshot. The remaining 112 reads returned empty before
their selected ticker had any published state, so they are not classified as
fast or stale. A stale-path entry is not necessarily a quote-removal operation:
after waiting for the monitor, a reader may find that another reader or writer
already published a fresh snapshot.

The longer run confirms that both implementations retain exact final-state
results while two-minute quotes expire. The snapshot version improved read p50
and p99 in this run, but worsened read p95 and the maximum outlier. It also made
all write percentiles worse, allocated roughly 51% more memory, and used more
heap. This single comparison does not show a decisive overall latency win for
the snapshot design once expiry is active.
