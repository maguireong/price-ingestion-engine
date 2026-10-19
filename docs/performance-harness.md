# Performance Harness

## Purpose

The load harness measures the dependency-free Java baseline under concurrent
quote ingestion and average queries. It is intended to establish evidence before
changing the engine's synchronization or data structures.

The harness does not replace correctness tests, JMH microbenchmarks, or a
production capacity test. It measures the complete in-process engine workflow on
the machine and JVM where it is run.

## Default workload

The default command runs a five-second warm-up followed by a 30-second measured
phase:

```bash
./scripts/load-test.sh
```

Defaults:

| Setting | Value |
| --- | ---: |
| Engine | `baseline` |
| Target quote rate | 50,000 per second |
| Target read rate | 50,000 per second |
| Ticker universe | 10,000 |
| Producer threads | 4 |
| Reader threads | 4 |
| Ticker distribution | `hot-cold` |
| Hot ticker share | 20% |
| Traffic sent to hot tickers | 80% |
| Latency sample interval | 1 in 100 operations |

Each ticker belongs to exactly one producer. This preserves the engine's current
assumption that sequence numbers and timestamps arrive in order for a ticker,
while still exercising multiple writer threads across the ticker universe.

Readers select tickers with the same hot/cold distribution and alternate between
two-minute and five-minute queries. If a writer advances a ticker immediately
before a reader acquires its lock, the reader refreshes its timestamp and retries;
the report exposes that retry count.

An 80/20 workload intentionally makes hot tickers receive more than the nominal
per-ticker average required to reach 50,000 quotes per second across 10,000
tickers. It is a contention-oriented stress distribution, not a claim that every
ticker remains below five updates per second.

### Zipfian workload

Run a separate long-tail workload with:

```bash
./scripts/zipf-load-test.sh
```

The wrapper selects Zipf mode with exponent `1.0` and accepts the same overrides
as `load-test.sh`. For example, use
`./scripts/zipf-load-test.sh --zipf-exponent=0.8` for a gentler long tail.

Zipf mode ranks tickers from hottest to coldest and gives rank `i` a weight
proportional to `1 / i^s`, where `s` is `zipf-exponent`. Unlike the default
two-tier workload, every rank has a distinct expected probability. A higher
exponent concentrates more traffic on the top-ranked tickers.

Each ticker remains assigned to exactly one producer. Producer target rates are
proportional to the combined Zipf weight of their tickers, and each producer
performs weighted selection only among the tickers it owns. Readers select from
the same global Zipf distribution. The fixed random seed makes the generated
sequence reproducible.

At the full 50,000 quote-per-second rate, a Zipfian workload intentionally allows
the hottest tickers to exceed the exercise's nominal five updates per second. It
is a contention and long-tail scenario, not a per-ticker-cap scenario.

## Metrics

The report includes:

- Attempted, accepted, and rejected quotes
- Accepted quote throughput per second
- Total and empty reads, timestamp retries, and read throughput
- A post-phase accuracy audit of both windows for every ticker against an
  independent scan-based reference history
- Sampled write/read p50, p95, p99, and maximum latency
- Heap usage at the start, observed peak, and end of the measured phase
- Per-thread allocated bytes when the JVM exposes that measurement
- Garbage-collection count and reported collection time
- A target verdict requiring at least 99% of the configured quote rate, no
  rejected quotes, and no accuracy mismatches

The accuracy audit runs after the producer and reader threads stop, so each
comparison has one unambiguous final state. Expected-empty ticker/window results
are reported separately and excluded from numeric-average comparisons. An
unexpected empty or unexpected non-empty result is still an accuracy mismatch.
The audit validates the final state produced by the concurrent workload; it does
not replay and validate every individual read at its exact concurrency point.

The baseline has no receiver/worker queue because callers invoke the engine
directly. Queue depth is therefore reported as not applicable. A queued ingestion
design should add queue capacity and peak-depth metrics when it is implemented.

The reference history retains one additional quote reference per active quote.
Its bookkeeping runs outside the timed engine call, but it is part of the same
worker process and therefore contributes to overall harness memory and allocation.

The exercise does not currently define acceptable p99 latency or heap limits, so
the harness reports those measurements without inventing pass/fail thresholds.

## Java Flight Recorder profiling

Use the profiling wrapper when the regular metrics show a slowdown that needs
attribution:

```bash
./scripts/profile-load-test.sh
```

The wrapper runs the same load harness with Java Flight Recorder enabled, writes
a timestamped `.jfr` recording under `.build/profiles`, and prints a compact
bottleneck summary covering ticker-lock waits, CPU and allocation samples, GC,
and expected pacing parks. All normal harness options can be passed through, for
example:

```bash
./scripts/profile-load-test.sh \
  --warmup-seconds=2 \
  --duration-seconds=10
```

The recording uses the JDK `profile.jfc` configuration with detailed garbage
collection, high method and allocation profiling, and Java monitor contention
events with a one-microsecond threshold. This provides evidence for separating
CPU work, allocation or GC pressure, and time blocked on the per-ticker
`synchronized` monitor. Profiling adds overhead, so compare the result with an
unprofiled run rather than treating its latency numbers as the baseline.

Open the `.jfr` file in JDK Mission Control for timeline and stack analysis. The
JDK command-line tool can also inspect it:

```bash
jfr summary .build/profiles/price-load-<run-id>.jfr
jfr print --events jdk.JavaMonitorEnter,jdk.ExecutionSample \
  .build/profiles/price-load-<run-id>.jfr
```

The baseline invokes the engine directly, so a recording can show blocked caller
threads but cannot show receiver/worker queue buildup: no such queue exists.

## Five-minute expiry soak

Run the optional expiry soak with:

```bash
./scripts/expiry-soak-test.sh
```

The script runs the baseline and snapshot engines under both hot/cold and Zipf
traffic. Each case uses a five-second warm-up and a 310-second measured phase so
both rolling-window expiry boundaries are crossed. The default rate is reduced
to 5,000 writes and 5,000 reads per second with 1,000 tickers to keep retained
history and total local runtime manageable. Running all four cases takes about
21 minutes before final accuracy-audit overhead. Normal load-harness overrides
can be appended to the command.

This is deliberately separate from `./scripts/test.sh`: it validates sustained
expiry behavior and resource trends, while the fast suite uses logical time jumps
to cover the same boundary semantics deterministically.

### BigInteger expiry-soak result

The complete four-case soak was run on 2026-10-19 using OpenJDK 21.0.12.1 after
running sums and `PriceAverage.totalPriceMicros` changed to `BigInteger`.

| Metric | Baseline hot/cold | Baseline Zipf | Snapshot hot/cold | Snapshot Zipf |
| --- | ---: | ---: | ---: | ---: |
| Accepted writes | 1,550,000 | 1,550,003 | 1,550,000 | 1,550,003 |
| Rejected writes | 0 | 0 | 0 | 0 |
| Write/read rate | 5,000/s / 5,000/s | 5,000/s / 5,000/s | 5,000/s / 5,000/s | 5,000/s / 5,000/s |
| Accuracy mismatches | 0 | 0 | 0 | 0 |
| Write p99 | 5.208 us | 3.333 us | 5.417 us | 3.500 us |
| Read p99 | 3.958 us | 2.875 us | 3.167 us | 2.500 us |
| Snapshot fast reads | N/A | N/A | 1,342,331 (86.602%) | 1,343,024 (86.651%) |
| Snapshot stale refreshes | N/A | N/A | 207,665 | 206,897 |
| Peak heap | 252.5 MiB | 211.7 MiB | 283.9 MiB | 278.7 MiB |
| Allocation | 575.7 MiB; 194.7 bytes/op | 580.3 MiB; 196.3 bytes/op | 669.5 MiB; 226.5 bytes/op | 674.1 MiB; 228.0 bytes/op |
| GC | 6 collections; 49 ms | 7 collections; 50 ms | 6 collections; 49 ms | 6 collections; 43 ms |
| Verdict | Sustained | Sustained | Sustained | Sustained |

Every case crossed both expiry boundaries, completed its final 2,000-average
reference audit with zero mismatches, and sustained its configured rates. In this
run the snapshot engine improved read p99 but made write p99 and allocation worse;
it allocated about 16% more bytes per operation than the corresponding baseline.
Approximately 13.4% of snapshot reads entered the stale-refresh path after expiry
became active.

The reduced 5,000/s rate makes this an expiry-correctness and resource-behavior
soak, not a replacement for the earlier 50,000/s and 100,000/s capacity tests.
The 310-second phase crosses the five-minute boundary only once, so a longer run
is still needed to establish repeated-cycle steady-state heap behavior.

## Local baseline result

The default workload was run on 2026-09-12 using OpenJDK 17.0.14. This is a
developer-machine observation, not a production capacity guarantee or a result
that should be assumed on other hardware. It also predates the switch from
`long` to `BigInteger` running sums, so its allocation and latency figures are a
historical comparison point rather than current measurements.

| Metric | Observed result |
| --- | ---: |
| Accepted quotes | 1,500,000 |
| Rejected quotes | 0 |
| Accepted quote rate | 50,000/s |
| Average query rate | 50,000/s |
| Empty query results | 112 |
| Final-state accuracy audit | 20,000 non-empty averages checked; 0 mismatches |
| Sampled write p99 | 1.833 us |
| Sampled read p99 | 2.417 us |
| Observed peak heap | 150.3 MiB |
| Worker-thread allocation | 186.2 MiB total; 65.1 bytes/operation |
| Garbage collection | 8 collections; 91 ms reported collection time |

The run met the harness verdict of at least 99% of the configured quote rate,
zero rejections, and zero final-state accuracy mismatches. It did not validate a
latency or memory target because the exercise does not define either threshold.
The 35-second combined warm-up and measurement is also shorter than the
five-minute retention window, so this result does not establish steady-state
heap usage.

See the [current performance snapshot](solution-results-current-performance-snapshot.md) for the
complete Gradle, Lincheck, default-load, and two-times-rate JFR baselines.

## Configuration

Display all options:

```bash
./scripts/load-test.sh --help
```

Every option uses `--name=value` syntax. For example, a short smoke run is:

```bash
./scripts/load-test.sh \
  --warmup-seconds=0 \
  --duration-seconds=2 \
  --target-quotes-per-second=5000 \
  --target-reads-per-second=5000 \
  --ticker-count=1000 \
  --producer-threads=2 \
  --reader-threads=2 \
  --sample-every=10
```

The supported options are:

- `engine`, either `baseline` or the experimental `snapshot`
- `duration-seconds`
- `warmup-seconds`
- `target-quotes-per-second`
- `target-reads-per-second`
- `ticker-count`
- `producer-threads`
- `reader-threads`
- `ticker-distribution`, either `hot-cold` or `zipf`
- `zipf-exponent`, a finite positive number; used by `zipf` mode
- `hot-ticker-percent`
- `hot-traffic-percent`, used by `hot-cold` mode
- `sample-every`
- `random-seed`

## Interpreting results

Run the harness on an otherwise quiet machine with a fixed JDK and heap
configuration. Record those environment details with any published result.
Compare multiple runs rather than treating one result as a capacity guarantee.

In particular:

- A rejection indicates a sequencing defect in the generator or engine behavior
  that needs investigation.
- Falling below the configured quote rate indicates that the generators, engine,
  JVM, or machine did not sustain the requested schedule.
- High reader timestamp retries indicate contention around the current
  historical-query restriction.
- A growing heap is expected during a run shorter than the five-minute retention
  window; longer runs are required to evaluate steady-state memory.
- Sampled latency reduces measurement overhead but can miss isolated extremes.
  Lower `sample-every` for denser measurements and compare the added overhead.

## Verification

Deterministic tests cover configuration parsing, Zipfian selection, defaults,
sampling, and nearest-rank percentile calculation. Run them with the full test
script:

```bash
./scripts/test.sh
```
