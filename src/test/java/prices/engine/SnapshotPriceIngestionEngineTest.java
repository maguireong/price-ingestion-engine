package prices.engine;

import java.math.BigInteger;
import org.junit.Test;
import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;

public final class SnapshotPriceIngestionEngineTest {

    @Test
    public void readsPublishedSnapshotWithoutTakingTheStaleRefreshPath() {
        var engine = new SnapshotPriceIngestionEngine();
        engine.ingest(quote(100_000_000, 1_000, 1));
        engine.ingest(quote(300_000_000, 2_000, 2));

        PriceAverage average = engine.getTwoMinuteAverage("AAPL", 2_000).orElseThrow();

        assertBigIntegerEquals(400_000_000, average.totalPriceMicros(), "two-minute sum");
        assertEquals(2, average.quoteCount(), "two-minute count");
        assertEquals(1, engine.readPathMetrics().fastPathReads(), "fast reads");
        assertEquals(0, engine.readPathMetrics().staleRefreshReads(), "stale refreshes");
    }

    @Test
    public void refreshesAnExpiredSnapshotAndPublishesItForFollowingReaders() {
        var engine = new SnapshotPriceIngestionEngine();
        engine.ingest(quote(100_000_000, 1_000, 1));
        long expiry = 1_000 + Window.TWO_MINUTES.durationMillis();

        assertTrue(engine.getTwoMinuteAverage("AAPL", expiry).isEmpty(), "expired read");
        assertTrue(engine.getTwoMinuteAverage("AAPL", expiry).isEmpty(), "following read");

        assertEquals(1, engine.readPathMetrics().fastPathReads(), "fast reads after refresh");
        assertEquals(1, engine.readPathMetrics().staleRefreshReads(), "stale refreshes");
    }

    @Test
    public void expiresOnlyTheWindowWhoseBoundaryHasPassed() {
        var engine = new SnapshotPriceIngestionEngine();
        long now = 400_000;
        engine.ingest(quote(100_000_000, now - Window.TWO_MINUTES.durationMillis(), 1));
        engine.ingest(quote(300_000_000, now, 2));

        PriceAverage twoMinute = engine.getTwoMinuteAverage("AAPL", now).orElseThrow();
        PriceAverage fiveMinute = engine.getFiveMinuteAverage("AAPL", now).orElseThrow();

        assertBigIntegerEquals(300_000_000, twoMinute.totalPriceMicros(), "two-minute sum");
        assertEquals(1, twoMinute.quoteCount(), "two-minute count");
        assertBigIntegerEquals(400_000_000, fiveMinute.totalPriceMicros(), "five-minute sum");
        assertEquals(2, fiveMinute.quoteCount(), "five-minute count");
    }

    @Test
    public void rejectsDuplicateAndStaleSequencesWithoutPublishingThem() {
        var engine = new SnapshotPriceIngestionEngine();

        assertTrue(engine.ingest(quote(100_000_000, 1_000, 10)), "first quote accepted");
        assertFalse(engine.ingest(quote(200_000_000, 1_001, 10)), "duplicate rejected");
        assertFalse(engine.ingest(quote(300_000_000, 1_002, 9)), "stale quote rejected");

        PriceAverage average = engine.getFiveMinuteAverage("AAPL", 1_002).orElseThrow();
        assertBigIntegerEquals(100_000_000, average.totalPriceMicros(), "published sum");
        assertEquals(1, average.quoteCount(), "published count");
    }

    private static Quote quote(long priceMicros, long timestampMillis, long sequenceNumber) {
        return new Quote("AAPL", priceMicros, timestampMillis, sequenceNumber);
    }

    private static void assertEquals(long expected, long actual, String description) {
        if (expected != actual) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertBigIntegerEquals(long expected, BigInteger actual, String description) {
        if (!BigInteger.valueOf(expected).equals(actual)) {
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
