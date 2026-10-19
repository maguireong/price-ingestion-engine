package prices.load;

import prices.PriceIngestionEngine;
import prices.engine.SnapshotPriceIngestionEngine;
import prices.model.PriceAverage;
import prices.model.Quote;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

public final class PriceIngestionLoadHarness {

    private static final long BASE_TIMESTAMP_MILLIS = 1_000_000L;
    private static final long ONE_SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long HEAP_SAMPLE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    private final LoadTestConfig config;
    private final PriceIngestionEngine engine;
    private final String[] tickers;
    private final AtomicLongArray sequences;
    private final AtomicLongArray latestGeneratedTimestamps;
    private final ReferenceTickerState[] referenceStates;
    private final ZipfianTickerSelector[] zipfProducerSelectors;
    private final ZipfianTickerSelector zipfReaderSelector;
    private final int[] producerTargetRates;
    private final long harnessStartNanos = System.nanoTime();

    private PriceIngestionLoadHarness(LoadTestConfig config) {
        this.config = config;
        this.engine = config.engineMode().create();
        this.tickers = new String[config.tickerCount()];
        this.sequences = new AtomicLongArray(config.tickerCount());
        this.latestGeneratedTimestamps = new AtomicLongArray(config.tickerCount());
        this.referenceStates = new ReferenceTickerState[config.tickerCount()];
        for (int tickerIndex = 0; tickerIndex < tickers.length; tickerIndex++) {
            tickers[tickerIndex] = "T" + tickerIndex;
            latestGeneratedTimestamps.set(tickerIndex, BASE_TIMESTAMP_MILLIS);
            referenceStates[tickerIndex] = new ReferenceTickerState();
        }

        this.zipfProducerSelectors = createZipfProducerSelectors();
        this.zipfReaderSelector = config.tickerDistribution() == TickerDistribution.ZIPF
                ? new ZipfianTickerSelector(allTickerIndexes(), config.zipfExponent())
                : null;
        this.producerTargetRates = createProducerTargetRates();
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length == 1 && "--help".equals(arguments[0])) {
            System.out.print(LoadTestConfig.usage());
            return;
        }

        LoadTestConfig config;
        try {
            config = LoadTestConfig.parse(arguments);
        } catch (IllegalArgumentException failure) {
            System.err.println("Invalid load-test configuration: " + failure.getMessage());
            System.err.println(LoadTestConfig.usage());
            throw failure;
        }

        new PriceIngestionLoadHarness(config).run();
    }

    private void run() throws Exception {
        printConfiguration();

        if (config.warmupSeconds() > 0) {
            System.out.printf("%nWarming up for %d seconds...%n", config.warmupSeconds());
            PhaseResult warmup = runPhase(config.warmupSeconds());
            System.out.printf(
                    "Warm-up complete: %,d accepted quotes and %,d reads.%n",
                    warmup.acceptedQuotes(), warmup.reads());
        }

        System.out.printf("%nMeasuring for %d seconds...%n", config.durationSeconds());
        PhaseResult result = runPhase(config.durationSeconds());
        printResult(result);
    }

    private PhaseResult runPhase(int durationSeconds) throws Exception {
        SnapshotPriceIngestionEngine.ReadPathMetrics readMetricsStart = readPathMetrics();
        int workerCount = config.producerThreads() + config.readerThreads();
        ExecutorService executor = Executors.newFixedThreadPool(
                workerCount,
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("price-load-worker-" + WORKER_NUMBER.incrementAndGet());
                    thread.setDaemon(false);
                    return thread;
                });

        var ready = new CountDownLatch(workerCount);
        var start = new CountDownLatch(1);
        var phaseStartNanos = new AtomicLong();
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var attemptedQuotes = new LongAdder();
        var acceptedQuotes = new LongAdder();
        var rejectedQuotes = new LongAdder();
        var reads = new LongAdder();
        var emptyReads = new LongAdder();
        var historicalReadRetries = new LongAdder();
        var allocatedBytes = new LongAdder();
        var allocationMeasurements = new LongAdder();
        List<LatencyRecorder> writeRecorders = new ArrayList<>();
        List<LatencyRecorder> readRecorders = new ArrayList<>();

        for (int producerIndex = 0; producerIndex < config.producerThreads(); producerIndex++) {
            LatencyRecorder recorder = new LatencyRecorder(config.sampleEvery());
            writeRecorders.add(recorder);
            int index = producerIndex;
            executor.submit(() -> runGuarded(
                    ready,
                    start,
                    failures,
                    allocatedBytes,
                    allocationMeasurements,
                    () -> runProducer(
                            index,
                            phaseStartNanos.get(),
                            durationSeconds,
                            recorder,
                            attemptedQuotes,
                            acceptedQuotes,
                            rejectedQuotes)));
        }

        for (int readerIndex = 0; readerIndex < config.readerThreads(); readerIndex++) {
            LatencyRecorder recorder = new LatencyRecorder(config.sampleEvery());
            readRecorders.add(recorder);
            int index = readerIndex;
            executor.submit(() -> runGuarded(
                    ready,
                    start,
                    failures,
                    allocatedBytes,
                    allocationMeasurements,
                    () -> runReader(
                            index,
                            phaseStartNanos.get(),
                            durationSeconds,
                            recorder,
                            reads,
                            emptyReads,
                            historicalReadRetries)));
        }

        if (!ready.await(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            throw new IllegalStateException("load workers did not become ready");
        }

        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        long heapStartBytes = memory.getHeapMemoryUsage().getUsed();
        long heapPeakBytes = heapStartBytes;
        GcSnapshot gcStart = GcSnapshot.capture();
        long startNanos = System.nanoTime();
        long durationNanos = TimeUnit.SECONDS.toNanos(durationSeconds);
        long endNanos = startNanos + durationNanos;
        phaseStartNanos.set(startNanos);
        start.countDown();

        while (System.nanoTime() < endNanos) {
            heapPeakBytes = Math.max(heapPeakBytes, memory.getHeapMemoryUsage().getUsed());
            long remainingNanos = endNanos - System.nanoTime();
            if (remainingNanos > 0) {
                LockSupport.parkNanos(Math.min(HEAP_SAMPLE_NANOS, remainingNanos));
            }
        }

        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            throw new IllegalStateException("load workers did not stop after the measurement period");
        }
        if (!failures.isEmpty()) {
            IllegalStateException failure = new IllegalStateException("load worker failed");
            failures.forEach(failure::addSuppressed);
            throw failure;
        }

        long heapEndBytes = memory.getHeapMemoryUsage().getUsed();
        heapPeakBytes = Math.max(heapPeakBytes, heapEndBytes);
        GcSnapshot gc = GcSnapshot.capture().differenceFrom(gcStart);
        long measuredAllocation = allocationMeasurements.sum() == workerCount
                ? allocatedBytes.sum()
                : -1;
        SnapshotPriceIngestionEngine.ReadPathMetrics readMetricsEnd = readPathMetrics();
        AccuracySummary accuracy = AccuracyValidator.validate(
                engine, tickers, referenceStates, logicalTimestampMillis());

        return new PhaseResult(
                durationNanos,
                attemptedQuotes.sum(),
                acceptedQuotes.sum(),
                rejectedQuotes.sum(),
                reads.sum(),
                emptyReads.sum(),
                historicalReadRetries.sum(),
                LatencySummary.combine(writeRecorders),
                LatencySummary.combine(readRecorders),
                heapStartBytes,
                heapPeakBytes,
                heapEndBytes,
                measuredAllocation,
                gc,
                accuracy,
                readMetricsEnd.fastPathReads() - readMetricsStart.fastPathReads(),
                readMetricsEnd.staleRefreshReads() - readMetricsStart.staleRefreshReads());
    }

    private SnapshotPriceIngestionEngine.ReadPathMetrics readPathMetrics() {
        return engine instanceof SnapshotPriceIngestionEngine snapshotEngine
                ? snapshotEngine.readPathMetrics()
                : new SnapshotPriceIngestionEngine.ReadPathMetrics(0, 0);
    }

    private void runProducer(
            int producerIndex,
            long phaseStartNanos,
            int durationSeconds,
            LatencyRecorder latency,
            LongAdder attemptedQuotes,
            LongAdder acceptedQuotes,
            LongAdder rejectedQuotes) {
        int hotTickerCount = hotTickerCount();
        int[] ownedHotTickers = ownedTickerIndexes(0, hotTickerCount, producerIndex);
        int[] ownedColdTickers = ownedTickerIndexes(hotTickerCount, tickers.length, producerIndex);
        int targetRate = producerTargetRates[producerIndex];
        long intervalNanos = ONE_SECOND_NANOS / targetRate;
        long endNanos = phaseStartNanos + TimeUnit.SECONDS.toNanos(durationSeconds);
        long nextOperationNanos = phaseStartNanos;
        var random = new SplittableRandom(config.randomSeed() + producerIndex);

        while (awaitNextOperation(nextOperationNanos, endNanos)) {
            int tickerIndex = config.tickerDistribution() == TickerDistribution.ZIPF
                    ? zipfProducerSelectors[producerIndex].select(random)
                    : chooseTicker(random, ownedHotTickers, ownedColdTickers);
            long sequence = sequences.incrementAndGet(tickerIndex);
            long timestampMillis = logicalTimestampMillis();
            latestGeneratedTimestamps.set(tickerIndex, timestampMillis);
            long priceMicros = 100_000_000L + random.nextLong(50_000_000L);
            Quote quote = new Quote(tickers[tickerIndex], priceMicros, timestampMillis, sequence);

            long operationStart = System.nanoTime();
            boolean accepted = engine.ingest(quote);
            latency.record(System.nanoTime() - operationStart);
            attemptedQuotes.increment();
            if (accepted) {
                acceptedQuotes.increment();
                referenceStates[tickerIndex].accept(quote);
            } else {
                rejectedQuotes.increment();
            }

            nextOperationNanos = nextDeadline(nextOperationNanos, intervalNanos);
        }
    }

    private void runReader(
            int readerIndex,
            long phaseStartNanos,
            int durationSeconds,
            LatencyRecorder latency,
            LongAdder reads,
            LongAdder emptyReads,
            LongAdder historicalReadRetries) {
        int targetRate = workerRate(
                config.targetReadsPerSecond(), config.readerThreads(), readerIndex);
        long intervalNanos = ONE_SECOND_NANOS / targetRate;
        long endNanos = phaseStartNanos + TimeUnit.SECONDS.toNanos(durationSeconds);
        long nextOperationNanos = phaseStartNanos;
        var random = new SplittableRandom(config.randomSeed() * 31 + readerIndex);

        while (awaitNextOperation(nextOperationNanos, endNanos)) {
            int tickerIndex = config.tickerDistribution() == TickerDistribution.ZIPF
                    ? zipfReaderSelector.select(random)
                    : chooseTicker(random, 0, hotTickerCount(), hotTickerCount(), tickers.length);
            long operationStart = System.nanoTime();
            Optional<PriceAverage> average =
                    readAverage(tickerIndex, random.nextBoolean(), historicalReadRetries);
            latency.record(System.nanoTime() - operationStart);
            reads.increment();
            if (average.isEmpty()) {
                emptyReads.increment();
            }

            nextOperationNanos = nextDeadline(nextOperationNanos, intervalNanos);
        }
    }

    private Optional<PriceAverage> readAverage(
            int tickerIndex, boolean twoMinute, LongAdder historicalReadRetries) {
        while (true) {
            long currentTimestampMillis = Math.max(
                    logicalTimestampMillis(), latestGeneratedTimestamps.get(tickerIndex));
            try {
                return twoMinute
                        ? engine.getTwoMinuteAverage(tickers[tickerIndex], currentTimestampMillis)
                        : engine.getFiveMinuteAverage(tickers[tickerIndex], currentTimestampMillis);
            } catch (IllegalArgumentException failure) {
                if (!"historical average queries are not supported".equals(failure.getMessage())) {
                    throw failure;
                }
                historicalReadRetries.increment();
                Thread.onSpinWait();
            }
        }
    }

    private int chooseTicker(
            SplittableRandom random, int[] hotTickerIndexes, int[] coldTickerIndexes) {
        return random.nextInt(100) < config.hotTrafficPercent()
                ? hotTickerIndexes[random.nextInt(hotTickerIndexes.length)]
                : coldTickerIndexes[random.nextInt(coldTickerIndexes.length)];
    }

    private int chooseTicker(
            SplittableRandom random,
            int hotStart,
            int hotEnd,
            int coldStart,
            int coldEnd) {
        return random.nextInt(100) < config.hotTrafficPercent()
                ? random.nextInt(hotStart, hotEnd)
                : random.nextInt(coldStart, coldEnd);
    }

    private int[] ownedTickerIndexes(int startInclusive, int endExclusive, int producerIndex) {
        int size = 0;
        for (int tickerIndex = startInclusive; tickerIndex < endExclusive; tickerIndex++) {
            if (tickerIndex % config.producerThreads() == producerIndex) {
                size++;
            }
        }

        int[] indexes = new int[size];
        int resultIndex = 0;
        for (int tickerIndex = startInclusive; tickerIndex < endExclusive; tickerIndex++) {
            if (tickerIndex % config.producerThreads() == producerIndex) {
                indexes[resultIndex++] = tickerIndex;
            }
        }
        return indexes;
    }

    private int[] allTickerIndexes() {
        int[] indexes = new int[tickers.length];
        for (int index = 0; index < indexes.length; index++) {
            indexes[index] = index;
        }
        return indexes;
    }

    private ZipfianTickerSelector[] createZipfProducerSelectors() {
        if (config.tickerDistribution() != TickerDistribution.ZIPF) {
            return null;
        }

        ZipfianTickerSelector[] selectors = new ZipfianTickerSelector[config.producerThreads()];
        for (int producerIndex = 0; producerIndex < selectors.length; producerIndex++) {
            selectors[producerIndex] = new ZipfianTickerSelector(
                    ownedTickerIndexes(0, tickers.length, producerIndex),
                    config.zipfExponent());
        }
        return selectors;
    }

    private int[] createProducerTargetRates() {
        int[] rates = new int[config.producerThreads()];
        if (config.tickerDistribution() == TickerDistribution.HOT_COLD) {
            for (int producerIndex = 0; producerIndex < rates.length; producerIndex++) {
                rates[producerIndex] = workerRate(
                        config.targetQuotesPerSecond(), rates.length, producerIndex);
            }
            return rates;
        }

        double totalWeight = 0.0;
        for (ZipfianTickerSelector selector : zipfProducerSelectors) {
            totalWeight += selector.totalWeight();
        }

        int remainingRate = config.targetQuotesPerSecond() - rates.length;
        int assignedRemainingRate = 0;
        double cumulativeWeight = 0.0;
        for (int producerIndex = 0; producerIndex < rates.length; producerIndex++) {
            cumulativeWeight += zipfProducerSelectors[producerIndex].totalWeight();
            int nextAssignedRate = producerIndex == rates.length - 1
                    ? remainingRate
                    : (int) Math.floor(remainingRate * cumulativeWeight / totalWeight);
            rates[producerIndex] = 1 + nextAssignedRate - assignedRemainingRate;
            assignedRemainingRate = nextAssignedRate;
        }
        return rates;
    }

    private int hotTickerCount() {
        return Math.max(1, tickers.length * config.hotTickerPercent() / 100);
    }

    private long logicalTimestampMillis() {
        return BASE_TIMESTAMP_MILLIS
                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - harnessStartNanos);
    }

    private static boolean awaitNextOperation(long deadlineNanos, long endNanos) {
        while (true) {
            long now = System.nanoTime();
            if (now >= endNanos) {
                return false;
            }
            long waitNanos = deadlineNanos - now;
            if (waitNanos <= 0) {
                return true;
            }
            LockSupport.parkNanos(waitNanos);
        }
    }

    private static long nextDeadline(long previousDeadline, long intervalNanos) {
        long next = previousDeadline + intervalNanos;
        long now = System.nanoTime();
        return next < now - ONE_SECOND_NANOS ? now : next;
    }

    private static int workerRate(int totalRate, int workerCount, int workerIndex) {
        int baseRate = totalRate / workerCount;
        return baseRate + (workerIndex < totalRate % workerCount ? 1 : 0);
    }

    private static void runGuarded(
            CountDownLatch ready,
            CountDownLatch start,
            ConcurrentLinkedQueue<Throwable> failures,
            LongAdder allocatedBytes,
            LongAdder allocationMeasurements,
            Runnable operation) {
        ready.countDown();
        try {
            start.await();
            long allocationBefore = ThreadAllocation.currentThreadAllocatedBytes();
            operation.run();
            long allocationAfter = ThreadAllocation.currentThreadAllocatedBytes();
            if (allocationBefore >= 0 && allocationAfter >= allocationBefore) {
                allocatedBytes.add(allocationAfter - allocationBefore);
                allocationMeasurements.increment();
            }
        } catch (Throwable failure) {
            failures.add(failure);
        }
    }

    private void printConfiguration() {
        System.out.println("Price ingestion load harness");
        System.out.printf("  engine:                 %s%n", config.engineMode().optionValue());
        System.out.printf("  duration:               %d s%n", config.durationSeconds());
        System.out.printf("  warm-up:                %d s%n", config.warmupSeconds());
        System.out.printf("  target quote rate:      %,d/s%n", config.targetQuotesPerSecond());
        System.out.printf("  target read rate:       %,d/s%n", config.targetReadsPerSecond());
        System.out.printf("  tickers:                %,d%n", config.tickerCount());
        System.out.printf("  producers/readers:      %d/%d%n", config.producerThreads(), config.readerThreads());
        if (config.tickerDistribution() == TickerDistribution.ZIPF) {
            System.out.printf(
                    "  traffic distribution:   zipf (exponent %.3f)%n",
                    config.zipfExponent());
        } else {
            System.out.printf(
                    "  traffic distribution:   hot-cold (%d%% of traffic to %d%% of tickers)%n",
                    config.hotTrafficPercent(), config.hotTickerPercent());
        }
        System.out.printf("  latency sampling:       1 in %,d operations%n", config.sampleEvery());
    }

    private void printResult(PhaseResult result) {
        double elapsedSeconds = result.elapsedNanos() / 1_000_000_000.0;
        double acceptedRate = result.acceptedQuotes() / elapsedSeconds;
        double readRate = result.reads() / elapsedSeconds;
        boolean sustainedTarget = acceptedRate >= config.targetQuotesPerSecond() * 0.99
                && result.rejectedQuotes() == 0
                && result.accuracy().accurate();

        System.out.println("\nResults");
        System.out.println("  Workload");
        System.out.printf(
                "    Writes:     %,d attempted, %,d accepted, %,d rejected%n",
                result.attemptedQuotes(),
                result.acceptedQuotes(),
                result.rejectedQuotes());
        System.out.printf(
                "    Write rate: %,.0f/s achieved, %,d/s configured%n",
                acceptedRate,
                config.targetQuotesPerSecond());
        System.out.printf(
                "    Reads:      %,d completed, %,d empty results, %,d timestamp retries%n",
                result.reads(),
                result.emptyReads(),
                result.historicalReadRetries());
        System.out.printf(
                "    Read rate:  %,.0f/s achieved, %,d/s configured%n",
                readRate,
                config.targetReadsPerSecond());
        if (config.engineMode() == EngineMode.SNAPSHOT) {
            long classifiedReads = result.snapshotFastPathReads() + result.snapshotStaleRefreshReads();
            double fastPathPercent = classifiedReads == 0
                    ? 0
                    : result.snapshotFastPathReads() * 100.0 / classifiedReads;
            System.out.printf(
                    "    Read paths: %,d fast (%.3f%%), %,d stale refreshes%n",
                    result.snapshotFastPathReads(),
                    fastPathPercent,
                    result.snapshotStaleRefreshReads());
        }

        System.out.println("  Accuracy");
        System.out.printf(
                "    Final state: %,d non-empty averages checked, "
                        + "%,d expected-empty results excluded, "
                        + "%,d mismatches%n",
                result.accuracy().nonEmptyAveragesChecked(),
                result.accuracy().expectedEmptyResultsExcluded(),
                result.accuracy().mismatches());
        result.accuracy().mismatchExamples().forEach(example ->
                System.out.println("      Mismatch: " + example));

        System.out.println("  Latency");
        printLatency("write", result.writeLatency());
        printLatency("read", result.readLatency());

        System.out.println("  Runtime");
        System.out.printf(
                "    Heap:       start %.1f MiB, peak %.1f MiB, end %.1f MiB%n",
                mebibytes(result.heapStartBytes()),
                mebibytes(result.heapPeakBytes()),
                mebibytes(result.heapEndBytes()));

        if (result.allocatedBytes() >= 0) {
            long operations = result.attemptedQuotes() + result.reads();
            System.out.printf(
                    "    Allocation: %.1f MiB total, %.1f MiB/s, %.1f bytes/operation%n",
                    mebibytes(result.allocatedBytes()),
                    mebibytes(result.allocatedBytes()) / elapsedSeconds,
                    operations == 0 ? 0 : (double) result.allocatedBytes() / operations);
        } else {
            System.out.println("    Allocation: unavailable on this JVM");
        }

        System.out.printf(
                "    GC:         %,d collections, %,d ms collection time%n",
                result.garbageCollection().collectionCount(),
                result.garbageCollection().collectionTimeMillis());
        System.out.println("    Queue:      not applicable; the baseline invokes the engine directly");

        System.out.println("  Verdict");
        System.out.printf(
                "    Target: %s%n",
                sustainedTarget ? "SUSTAINED" : "NOT SUSTAINED");
        System.out.println(
                "    Criteria: >=99% of configured write rate, zero rejected writes, "
                        + "and zero accuracy mismatches");
        System.out.println(
                "    Note: no read-rate, p99, or memory pass/fail threshold is currently defined");
    }

    private static void printLatency(String name, LatencySummary latency) {
        System.out.printf(
                "    %-5s %,d samples; p50 %.3f us, p95 %.3f us, p99 %.3f us, max %.3f us%n",
                name,
                latency.sampleCount(),
                micros(latency.p50Nanos()),
                micros(latency.p95Nanos()),
                micros(latency.p99Nanos()),
                micros(latency.maxNanos()));
    }

    private static double micros(long nanos) {
        return nanos / 1_000.0;
    }

    private static double mebibytes(long bytes) {
        return bytes / 1_048_576.0;
    }

    private static final AtomicInteger WORKER_NUMBER = new AtomicInteger();
}
