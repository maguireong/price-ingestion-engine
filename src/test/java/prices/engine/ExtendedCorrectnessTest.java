package prices.engine;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import prices.PriceIngestionEngine;
import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;

public final class ExtendedCorrectnessTest {

    @Test
    public void baselineAccumulatesBeyondLongRange() {
        assertAccumulatesBeyondLongRange(new InMemoryPriceIngestionEngine());
    }

    @Test
    public void snapshotAccumulatesBeyondLongRange() {
        assertAccumulatesBeyondLongRange(new SnapshotPriceIngestionEngine());
    }

    @Test
    public void baselineExpiresLargeBatchesAcrossBothWindows() {
        assertLargeExpiryBatch(new InMemoryPriceIngestionEngine());
    }

    @Test
    public void snapshotExpiresLargeBatchesAcrossBothWindows() {
        assertLargeExpiryBatch(new SnapshotPriceIngestionEngine());
    }

    @Test
    public void bothEnginesMatchReferenceAcrossSeededRandomTimeline() {
        PriceIngestionEngine[] engines = {
                new InMemoryPriceIngestionEngine(),
                new SnapshotPriceIngestionEngine()
        };
        var reference = new ReferenceModel();
        var random = new SplittableRandom(42);
        String[] tickers = {"T0", "T1", "T2", "T3", "T4"};
        long[] lastSequences = new long[tickers.length];
        long now = 0;

        for (int operation = 0; operation < 10_000; operation++) {
            now += random.nextLong(20_001);
            if (operation > 0 && operation % 997 == 0) {
                now += Window.FIVE_MINUTES.durationMillis() + 1;
            }

            int writeTickerIndex = random.nextInt(tickers.length);
            String writeTicker = tickers[writeTickerIndex];
            if (random.nextInt(100) < 65) {
                long sequence = nextSequence(random, lastSequences[writeTickerIndex]);
                long priceMicros = random.nextLong(1_000_000_001L);
                Quote quote = new Quote(writeTicker, priceMicros, now, sequence);
                boolean expectedAccepted = reference.ingest(quote);

                for (PriceIngestionEngine engine : engines) {
                    assertEquals(
                            expectedAccepted,
                            engine.ingest(quote),
                            engine.getClass().getSimpleName() + " acceptance at operation " + operation);
                }
                if (expectedAccepted) {
                    lastSequences[writeTickerIndex] = sequence;
                }
            }

            String readTicker = tickers[random.nextInt(tickers.length)];
            assertEnginesMatch(reference, engines, readTicker, now, Window.TWO_MINUTES, operation);
            assertEnginesMatch(reference, engines, readTicker, now, Window.FIVE_MINUTES, operation);
        }
    }

    @Test
    public void snapshotReadersAndWriterRemainConsistentAtExpiryBoundary() throws Exception {
        var engine = new SnapshotPriceIngestionEngine();
        long firstTimestamp = 1_000;
        long expiry = firstTimestamp + Window.TWO_MINUTES.durationMillis();
        assertTrue(engine.ingest(new Quote("AAPL", 100_000_000, firstTimestamp, 1)), "initial quote accepted");

        int readerCount = 32;
        var ready = new CountDownLatch(readerCount + 1);
        var start = new CountDownLatch(1);
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var observed = new ConcurrentLinkedQueue<String>();
        var executor = Executors.newFixedThreadPool(readerCount + 1);

        try {
            for (int reader = 0; reader < readerCount; reader++) {
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        observed.add(engine.getTwoMinuteAverage("AAPL", expiry)
                                .map(ExtendedCorrectnessTest::snapshot)
                                .orElse("empty"));
                    } catch (Throwable failure) {
                        failures.add(failure);
                    }
                });
            }
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    engine.ingest(new Quote("AAPL", 200_000_000, expiry, 2));
                } catch (Throwable failure) {
                    failures.add(failure);
                }
            });

            assertTrue(ready.await(5, TimeUnit.SECONDS), "workers became ready");
            start.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "workers completed");
        } finally {
            executor.shutdownNow();
        }

        assertTrue(failures.isEmpty(), "concurrent operations did not fail: " + failures);
        assertEquals(readerCount, observed.size(), "completed reader count");
        for (String result : observed) {
            assertTrue(
                    result.equals("empty") || result.equals("200000000:1"),
                    "reader observed a legal state: " + result);
        }
        assertAverage(engine.getTwoMinuteAverage("AAPL", expiry), 200_000_000, 1, "final snapshot");
    }

    private static void assertAccumulatesBeyondLongRange(PriceIngestionEngine engine) {
        String ticker = "LARGE-SUM";
        long initialPrice = Long.MAX_VALUE - 5;
        assertTrue(engine.ingest(new Quote(ticker, initialPrice, 0, 1)), "initial quote accepted");
        assertTrue(engine.ingest(new Quote(ticker, 10, 1, 2)), "second quote accepted");

        BigInteger expected = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(5));
        assertAverage(engine.getTwoMinuteAverage(ticker, 1), expected, 2, "two-minute large sum");
        assertAverage(engine.getFiveMinuteAverage(ticker, 1), expected, 2, "five-minute large sum");
    }

    private static void assertLargeExpiryBatch(PriceIngestionEngine engine) {
        String ticker = "BURST";
        int quoteCount = 20_000;
        for (int index = 0; index < quoteCount; index++) {
            assertTrue(
                    engine.ingest(new Quote(ticker, 1_000_000, index, index + 1L)),
                    "burst quote accepted");
        }

        long expiryTimestamp = quoteCount - 1L + Window.FIVE_MINUTES.durationMillis();
        assertTrue(engine.getTwoMinuteAverage(ticker, expiryTimestamp).isEmpty(), "two-minute batch expired");
        assertTrue(engine.getFiveMinuteAverage(ticker, expiryTimestamp).isEmpty(), "five-minute batch expired");

        assertTrue(
                engine.ingest(new Quote(ticker, 7_000_000, expiryTimestamp + 1, quoteCount + 1L)),
                "post-expiry quote accepted");
        assertAverage(
                engine.getFiveMinuteAverage(ticker, expiryTimestamp + 1),
                7_000_000,
                1,
                "post-expiry state");
    }

    private static long nextSequence(SplittableRandom random, long lastSequence) {
        if (lastSequence > 0 && random.nextInt(100) < 20) {
            return random.nextBoolean() ? lastSequence : lastSequence - 1;
        }
        return lastSequence + 1;
    }

    private static void assertEnginesMatch(
            ReferenceModel reference,
            PriceIngestionEngine[] engines,
            String ticker,
            long now,
            Window window,
            int operation) {
        Optional<PriceAverage> expected = reference.average(ticker, now, window);
        for (PriceIngestionEngine engine : engines) {
            Optional<PriceAverage> actual = window == Window.TWO_MINUTES
                    ? engine.getTwoMinuteAverage(ticker, now)
                    : engine.getFiveMinuteAverage(ticker, now);
            assertOptionalAverageEquals(
                    expected,
                    actual,
                    engine.getClass().getSimpleName() + " " + window + " at operation " + operation);
        }
    }

    private static void assertOptionalAverageEquals(
            Optional<PriceAverage> expected,
            Optional<PriceAverage> actual,
            String description) {
        if (expected.isEmpty() || actual.isEmpty()) {
            if (expected.isEmpty() != actual.isEmpty()) {
                throw new AssertionError(description + ": expected " + expected + " but was " + actual);
            }
            return;
        }
        assertEquals(
                expected.orElseThrow().totalPriceMicros(),
                actual.orElseThrow().totalPriceMicros(),
                description + " sum");
        assertEquals(expected.orElseThrow().quoteCount(), actual.orElseThrow().quoteCount(), description + " count");
    }

    private static void assertAverage(
            Optional<PriceAverage> actual,
            long expectedSum,
            long expectedCount,
            String description) {
        assertAverage(actual, BigInteger.valueOf(expectedSum), expectedCount, description);
    }

    private static void assertAverage(
            Optional<PriceAverage> actual,
            BigInteger expectedSum,
            long expectedCount,
            String description) {
        PriceAverage average = actual.orElseThrow(() -> new AssertionError(description + ": expected a value"));
        assertEquals(expectedSum, average.totalPriceMicros(), description + " sum");
        assertEquals(expectedCount, average.quoteCount(), description + " count");
    }

    private static String snapshot(PriceAverage average) {
        return average.totalPriceMicros() + ":" + average.quoteCount();
    }

    private static void assertEquals(long expected, long actual, String description) {
        if (expected != actual) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertEquals(BigInteger expected, BigInteger actual, String description) {
        if (!expected.equals(actual)) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertEquals(boolean expected, boolean actual, String description) {
        if (expected != actual) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertTrue(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError(description);
        }
    }

    private static final class ReferenceModel {
        private final Map<String, List<Quote>> acceptedQuotes = new HashMap<>();
        private final Map<String, Long> lastSequences = new HashMap<>();

        private boolean ingest(Quote quote) {
            long lastSequence = lastSequences.getOrDefault(quote.ticker(), -1L);
            if (quote.sequenceNumber() <= lastSequence) {
                return false;
            }
            acceptedQuotes.computeIfAbsent(quote.ticker(), ignored -> new ArrayList<>()).add(quote);
            lastSequences.put(quote.ticker(), quote.sequenceNumber());
            return true;
        }

        private Optional<PriceAverage> average(String ticker, long now, Window window) {
            long cutoff = now - window.durationMillis();
            BigInteger sum = BigInteger.ZERO;
            long count = 0;
            for (Quote quote : acceptedQuotes.getOrDefault(ticker, List.of())) {
                if (quote.timestampMillis() > cutoff) {
                    sum = sum.add(BigInteger.valueOf(quote.priceMicros()));
                    count++;
                }
            }
            return count == 0 ? Optional.empty() : Optional.of(new PriceAverage(sum, count));
        }
    }
}
