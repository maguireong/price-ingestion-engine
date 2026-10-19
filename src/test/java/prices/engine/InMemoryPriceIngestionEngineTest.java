package prices.engine;

import org.junit.Test;
import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class InMemoryPriceIngestionEngineTest {

    @Test
    public void calculatesTwoAndFiveMinuteAverages() {
        var engine = new InMemoryPriceIngestionEngine();
        long now = 400_000;

        engine.ingest(quote("AAPL", 100_000_000, now - 240_000, 1));
        engine.ingest(quote("AAPL", 200_000_000, now - 60_000, 2));
        engine.ingest(quote("AAPL", 400_000_000, now, 3));

        assertDecimalEquals(
                "300.000000",
                engine.getTwoMinuteAverage("AAPL", now).orElseThrow().asBigDecimal(6, RoundingMode.UNNECESSARY));
        assertDecimalEquals(
                "233.333333",
                engine.getFiveMinuteAverage("AAPL", now).orElseThrow().asBigDecimal(6, RoundingMode.DOWN));
    }

    @Test
    public void expiresQuotesAtExactWindowBoundary() {
        var engine = new InMemoryPriceIngestionEngine();
        long now = 600_000;

        engine.ingest(quote("MSFT", 100_000_000, now - Window.TWO_MINUTES.durationMillis(), 1));
        engine.ingest(quote("MSFT", 300_000_000, now, 2));

        PriceAverage twoMinute = engine.getTwoMinuteAverage("MSFT", now).orElseThrow();
        assertEquals(300_000_000L, twoMinute.totalPriceMicros(), "two-minute sum");
        assertEquals(1L, twoMinute.quoteCount(), "two-minute count");
        assertEquals(2L, engine.getFiveMinuteAverage("MSFT", now).orElseThrow().quoteCount(), "five-minute count");
    }

    @Test
    public void rejectsDuplicateAndStaleSequences() {
        var engine = new InMemoryPriceIngestionEngine();

        assertTrue(engine.ingest(quote("TSLA", 200_000_000, 1_000, 10)), "first quote accepted");
        assertFalse(engine.ingest(quote("TSLA", 201_000_000, 1_001, 10)), "duplicate rejected");
        assertFalse(engine.ingest(quote("TSLA", 199_000_000, 1_002, 9)), "stale quote rejected");
        assertEquals(1L, engine.getFiveMinuteAverage("TSLA", 1_002).orElseThrow().quoteCount(), "deduplicated count");
    }

    @Test
    public void keepsTickersIndependent() {
        var engine = new InMemoryPriceIngestionEngine();

        engine.ingest(quote("AAPL", 100_000_000, 1_000, 1));
        engine.ingest(quote("MSFT", 250_000_000, 1_000, 1));

        assertEquals(100_000_000L, engine.getTwoMinuteAverage("AAPL", 1_000).orElseThrow().totalPriceMicros(), "AAPL sum");
        assertEquals(250_000_000L, engine.getTwoMinuteAverage("MSFT", 1_000).orElseThrow().totalPriceMicros(), "MSFT sum");
        assertEquals(2L, engine.tickerCount(), "ticker count");
    }

    @Test
    public void expiresStateWhenNoNewQuotesArrive() {
        var engine = new InMemoryPriceIngestionEngine();
        engine.ingest(quote("NVDA", 900_000_000, 1_000, 1));

        long afterFiveMinutes = 1_000 + Window.FIVE_MINUTES.durationMillis();
        assertTrue(engine.getFiveMinuteAverage("NVDA", afterFiveMinutes).isEmpty(), "expired five-minute window");
    }

    @Test
    public void handlesConcurrentWritersForDifferentTickers() throws Exception {
        int tickerCount = 8;
        int quotesPerTicker = 1_000;
        var engine = new InMemoryPriceIngestionEngine();
        var ready = new CountDownLatch(tickerCount);
        var start = new CountDownLatch(1);
        List<Throwable> failures = new ArrayList<>();

        var executor = Executors.newFixedThreadPool(tickerCount);
        try {
            for (int tickerIndex = 0; tickerIndex < tickerCount; tickerIndex++) {
                String ticker = "T" + tickerIndex;
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int sequence = 1; sequence <= quotesPerTicker; sequence++) {
                            engine.ingest(quote(ticker, 1_000_000, sequence, sequence));
                        }
                    } catch (Throwable failure) {
                        synchronized (failures) {
                            failures.add(failure);
                        }
                    }
                });
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS), "writers became ready");
            start.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "writers completed");
        } finally {
            executor.shutdownNow();
        }

        assertTrue(failures.isEmpty(), "concurrent writers did not fail: " + failures);
        for (int tickerIndex = 0; tickerIndex < tickerCount; tickerIndex++) {
            String ticker = "T" + tickerIndex;
            assertEquals(
                    quotesPerTicker,
                    engine.getFiveMinuteAverage(ticker, quotesPerTicker).orElseThrow().quoteCount(),
                    ticker + " quote count");
        }
    }

    private static Quote quote(String ticker, long priceMicros, long timestampMillis, long sequenceNumber) {
        return new Quote(ticker, priceMicros, timestampMillis, sequenceNumber);
    }

    private static void assertDecimalEquals(String expected, BigDecimal actual) {
        if (new BigDecimal(expected).compareTo(actual) != 0) {
            throw new AssertionError("expected " + expected + " but was " + actual);
        }
    }

    private static void assertEquals(long expected, long actual, String description) {
        if (expected != actual) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertTrue(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError(description);
        }
    }

    private static void assertFalse(boolean condition, String description) {
        assertTrue(!condition, description);
    }
}
