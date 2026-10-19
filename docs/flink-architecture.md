# Using Apache Flink for Price Averages

Apache Flink is a possible production-oriented implementation of the price
ingestion engine. It is designed for long-running streams of events and can
maintain aggregates over multiple time intervals for every ticker.

Flink is not part of the current implementation. The existing engine is a
dependency-free, single-JVM baseline with a synchronous query API. This document
describes how the problem could map to Flink and when that additional platform
would be useful.

## When Flink fits

Flink becomes relevant when quotes arrive continuously from a source such as
Kafka and the system needs one or more of the following:

- Processing distributed across multiple workers
- Per-ticker state that can be redistributed when parallelism changes
- Event-time processing and an explicit policy for late or out-of-order quotes
- Checkpointed state and recovery after process or machine failure
- A continuous stream of derived averages for multiple downstream consumers
- More aggregation intervals without building separate ingestion services

The target of 50,000 quotes per second does not by itself prove that Flink is
needed. The current Java implementation should first be load-tested so that an
architecture change responds to measured limits or additional production
requirements.

## Mapping the ticker problem to Flink

A Flink pipeline could have the following shape:

```text
quote source
    -> keyBy(ticker)
    -> per-ticker state and deduplication
    -> rolling 2-minute and 5-minute aggregation
    -> updated-average stream
    -> materialized query store or downstream consumers
```

Calling `keyBy(ticker)` routes all events for the same ticker to the same keyed
operator. That operator can maintain the ticker's sequence number, quote history,
running sums, and counts as keyed state.

Flink checkpoints keyed state and can redistribute it when the application is
rescaled. Those capabilities are useful in a distributed production system but
add deployment, configuration, monitoring, and recovery concerns that the
single-JVM exercise intentionally avoids.

## Calculating different time intervals

### Window operators

Flink provides sliding event-time and processing-time windows. A separate window
definition could calculate each required interval, for example:

- Two-minute window with a chosen slide frequency
- Five-minute window with a chosen slide frequency

An `AggregateFunction` can incrementally maintain a sum and count instead of
buffering and rescanning every element when a window is evaluated.

The window size and slide are different concepts. A two-minute window with a
one-second slide produces a result every second for the preceding two minutes;
it does not provide a result for every possible timestamp. Overlapping sliding
windows can also associate an event with multiple window instances, so the slide
frequency affects state and processing cost.

### Keyed process function

A `KeyedProcessFunction` offers lower-level control and is closer to the current
deque-based engine. It could:

1. Reject a quote whose sequence number is duplicate or stale.
2. Add an accepted price to per-ticker sums and counts.
3. Retain enough quote or bucket state to reverse the contribution later.
4. Register event-time or processing-time timers for expiry.
5. Remove expired contributions when a timer fires.
6. Emit a new immutable two-minute and five-minute average snapshot.

Flink serializes element processing and timer callbacks for a keyed operator, so
they can safely update that operator's keyed state. Event-time timers depend on
watermarks; therefore, a production design must define timestamp extraction,
watermark progression, allowed lateness, and what happens when a source becomes
idle.

## Query model difference

The current interface is synchronous and pull-based:

```java
getTwoMinuteAverage(ticker, currentTimestamp)
getFiveMinuteAverage(ticker, currentTimestamp)
```

Flink is primarily push-based: events enter a pipeline and derived results leave
it as another stream. A typical production design would continuously emit the
latest average for each ticker into a materialized store. An API service would
then read from that store rather than invoke a method directly on the Flink
operator.

This separation provides a scalable serving interface, but introduces questions
that do not exist in the local engine:

- How fresh must a served average be?
- What store holds the latest value per ticker and interval?
- What delivery guarantee is required between Flink and that store?
- How are unavailable or recovering components represented to callers?

## Operational trade-offs

Adopting Flink introduces capabilities and corresponding responsibilities:

| Capability | Additional design work |
| --- | --- |
| Distributed keyed processing | Parallelism, partitioning, skew, and hot tickers |
| Event time | Watermarks, late events, idleness, and timestamp correctness |
| Durable state | State backend, checkpoint interval, storage, and recovery testing |
| Stream ingestion | Source offsets, replay behavior, backpressure, and overload policy |
| Continuous output | Sink guarantees, materialization, freshness, and query serving |
| Application upgrades | State compatibility, savepoints, deployment, and rollback |

Flink should be selected when these capabilities are requirements worth operating,
not only because the input happens to be a stream.

## Suggested prototype

If the exercise is extended to explore Flink, keep the first prototype narrow:

1. Generate quotes in process or read them from a small Kafka topic.
2. Partition with `keyBy(Quote::ticker)`.
3. Implement sum, count, sequence, and expiry using keyed state.
4. Emit updated averages and verify exact window-boundary behavior.
5. Add deterministic event-time tests with controlled watermarks.
6. Compare throughput, latency, memory, and operational complexity with the
   current Java baseline.

This comparison should determine whether Flink solves an actual production need
or merely moves a small in-memory algorithm into a larger runtime.

## References

- [Apache Flink: Stateful Stream Processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)
- [Apache Flink: Windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/)
- [Apache Flink: Process Function](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/)
