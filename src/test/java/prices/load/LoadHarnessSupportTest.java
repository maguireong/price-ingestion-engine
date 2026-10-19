package prices.load;

import prices.engine.InMemoryPriceIngestionEngine;
import prices.enums.Window;
import prices.model.Quote;
import java.util.List;
import org.junit.Test;

public final class LoadHarnessSupportTest {

    @Test
    public void usesDocumentedDefaults() {
        LoadTestConfig config = LoadTestConfig.defaultsConfig();

        assertEquals(EngineMode.BASELINE, config.engineMode(), "default engine");
        assertEquals(30, config.durationSeconds(), "default duration");
        assertEquals(50_000, config.targetQuotesPerSecond(), "default quote rate");
        assertEquals(10_000, config.tickerCount(), "default ticker count");
        assertEquals(TickerDistribution.HOT_COLD, config.tickerDistribution(), "default distribution");
        assertDoubleEquals(1.0, config.zipfExponent(), "default Zipf exponent");
        assertEquals(20, config.hotTickerPercent(), "default hot ticker share");
        assertEquals(80, config.hotTrafficPercent(), "default hot traffic share");
    }

    @Test
    public void parsesOverrides() {
        LoadTestConfig config = LoadTestConfig.parse(new String[] {
                "--duration-seconds=2",
                "--engine=snapshot",
                "--warmup-seconds=0",
                "--target-quotes-per-second=1000",
                "--target-reads-per-second=500",
                "--ticker-count=100",
                "--producer-threads=2",
                "--reader-threads=2",
                "--ticker-distribution=zipf",
                "--zipf-exponent=1.2",
                "--sample-every=10",
                "--random-seed=7"
        });

        assertEquals(2, config.durationSeconds(), "overridden duration");
        assertEquals(EngineMode.SNAPSHOT, config.engineMode(), "overridden engine");
        assertEquals(0, config.warmupSeconds(), "overridden warmup");
        assertEquals(1_000, config.targetQuotesPerSecond(), "overridden quote rate");
        assertEquals(500, config.targetReadsPerSecond(), "overridden read rate");
        assertEquals(TickerDistribution.ZIPF, config.tickerDistribution(), "overridden distribution");
        assertDoubleEquals(1.2, config.zipfExponent(), "overridden Zipf exponent");
        assertEquals(10, config.sampleEvery(), "overridden sample interval");
        assertEquals(7, config.randomSeed(), "overridden seed");
    }

    @Test
    public void rejectsUnknownArguments() {
        assertThrows(
                () -> LoadTestConfig.parse(new String[] {"--not-a-setting=1"}),
                "unknown arguments are rejected");
    }

    @Test
    public void rejectsInvalidTickerDistribution() {
        assertThrows(
                () -> LoadTestConfig.parse(new String[] {
                        "--ticker-count=4",
                        "--producer-threads=4"
                }),
                "each producer needs hot and cold tickers");
    }

    @Test
    public void rejectsInvalidZipfConfiguration() {
        assertThrows(
                () -> LoadTestConfig.parse(new String[] {"--ticker-distribution=unknown"}),
                "unknown distributions are rejected");
        assertThrows(
                () -> LoadTestConfig.parse(new String[] {
                        "--ticker-distribution=zipf",
                        "--zipf-exponent=0"
                }),
                "non-positive Zipf exponents are rejected");
    }

    @Test
    public void samplesAtConfiguredIntervalAndCalculatesPercentiles() {
        var first = new LatencyRecorder(2);
        first.record(10);
        first.record(20);
        first.record(30);
        first.record(40);

        var second = new LatencyRecorder(2);
        second.record(50);
        second.record(60);
        second.record(70);
        second.record(80);

        LatencySummary summary = LatencySummary.combine(List.of(first, second));

        assertEquals(8, summary.operationCount(), "operation count");
        assertEquals(4, summary.sampleCount(), "sample count");
        assertEquals(40, summary.p50Nanos(), "p50");
        assertEquals(80, summary.p95Nanos(), "p95");
        assertEquals(80, summary.p99Nanos(), "p99");
        assertEquals(80, summary.maxNanos(), "maximum");
    }

    @Test
    public void reportsEmptySummaryWhenNoSampleIsSelected() {
        var recorder = new LatencyRecorder(100);
        recorder.record(10);
        recorder.record(20);

        LatencySummary summary = LatencySummary.combine(List.of(recorder));

        assertEquals(2, summary.operationCount(), "unsampled operation count");
        assertEquals(0, summary.sampleCount(), "empty sample count");
        assertEquals(0, summary.p99Nanos(), "empty p99");
    }

    @Test
    public void referenceStateCalculatesBothWindowsByScanningAcceptedQuotes() {
        var reference = new ReferenceTickerState();
        long now = 400_000;

        reference.accept(new Quote("AAPL", 100_000_000, now - 240_000, 1));
        reference.accept(new Quote("AAPL", 200_000_000, now - 60_000, 2));
        reference.accept(new Quote("AAPL", 400_000_000, now, 3));

        assertEquals(
                600_000_000,
                reference.average(now, Window.TWO_MINUTES).orElseThrow().totalPriceMicros(),
                "reference two-minute sum");
        assertEquals(
                700_000_000,
                reference.average(now, Window.FIVE_MINUTES).orElseThrow().totalPriceMicros(),
                "reference five-minute sum");
    }

    @Test
    public void accuracyAuditExcludesExpectedEmptyResults() {
        var engine = new InMemoryPriceIngestionEngine();
        var reference = new ReferenceTickerState();
        String[] tickers = {"AAPL", "MSFT"};
        ReferenceTickerState[] references = {reference, new ReferenceTickerState()};
        Quote quote = new Quote("AAPL", 100_000_000, 1_000, 1);
        engine.ingest(quote);
        reference.accept(quote);

        AccuracySummary summary = AccuracyValidator.validate(engine, tickers, references, 1_000);

        assertEquals(2, summary.nonEmptyAveragesChecked(), "non-empty average checks");
        assertEquals(2, summary.expectedEmptyResultsExcluded(), "expected empty exclusions");
        assertEquals(0, summary.mismatches(), "accuracy mismatches");
    }

    @Test
    public void accuracyAuditReportsIncorrectAverages() {
        var engine = new InMemoryPriceIngestionEngine();
        var reference = new ReferenceTickerState();
        engine.ingest(new Quote("AAPL", 100_000_000, 1_000, 1));
        reference.accept(new Quote("AAPL", 200_000_000, 1_000, 1));

        AccuracySummary summary = AccuracyValidator.validate(
                engine,
                new String[] {"AAPL"},
                new ReferenceTickerState[] {reference},
                1_000);

        assertEquals(2, summary.nonEmptyAveragesChecked(), "non-empty average checks");
        assertEquals(0, summary.expectedEmptyResultsExcluded(), "expected empty exclusions");
        assertEquals(2, summary.mismatches(), "accuracy mismatches");
        assertEquals(2, summary.mismatchExamples().size(), "mismatch examples");
    }

    @Test
    public void accuracyAuditReportsUnexpectedEmptyResults() {
        var reference = new ReferenceTickerState();
        reference.accept(new Quote("AAPL", 100_000_000, 1_000, 1));

        AccuracySummary summary = AccuracyValidator.validate(
                new InMemoryPriceIngestionEngine(),
                new String[] {"AAPL"},
                new ReferenceTickerState[] {reference},
                1_000);

        assertEquals(2, summary.nonEmptyAveragesChecked(), "non-empty average checks");
        assertEquals(0, summary.expectedEmptyResultsExcluded(), "expected empty exclusions");
        assertEquals(2, summary.mismatches(), "unexpected empty mismatches");
    }

    @Test
    public void accuracyAuditReportsUnexpectedNonEmptyResults() {
        var engine = new InMemoryPriceIngestionEngine();
        engine.ingest(new Quote("AAPL", 100_000_000, 1_000, 1));

        AccuracySummary summary = AccuracyValidator.validate(
                engine,
                new String[] {"AAPL"},
                new ReferenceTickerState[] {new ReferenceTickerState()},
                1_000);

        assertEquals(0, summary.nonEmptyAveragesChecked(), "non-empty average checks");
        assertEquals(2, summary.expectedEmptyResultsExcluded(), "expected empty exclusions");
        assertEquals(2, summary.mismatches(), "unexpected non-empty mismatches");
    }

    private static void assertEquals(long expected, long actual, String description) {
        if (expected != actual) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertEquals(Object expected, Object actual, String description) {
        if (!expected.equals(actual)) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertDoubleEquals(double expected, double actual, String description) {
        if (Double.compare(expected, actual) != 0) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertThrows(Runnable operation, String description) {
        try {
            operation.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError(description + ": expected IllegalArgumentException");
    }
}
