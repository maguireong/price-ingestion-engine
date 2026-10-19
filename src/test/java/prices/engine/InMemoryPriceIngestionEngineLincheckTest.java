package prices.engine;

import org.jetbrains.lincheck.datastructures.ModelCheckingOptions;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.Test;
import prices.model.PriceAverage;
import prices.model.Quote;

public final class InMemoryPriceIngestionEngineLincheckTest {

    private static final String TICKER = "AAPL";
    private static final long TIMESTAMP_MILLIS = 1_000;
    private static final int THREADS = 3;
    private static final int MODEL_CHECKING_ITERATIONS = 10;
    private static final int MODEL_CHECKING_INVOCATIONS = 100;
    private static final int MODEL_CHECKING_ACTORS_PER_THREAD = 2;
    private static final int STRESS_ITERATIONS = 50;
    private static final int STRESS_INVOCATIONS = 100;
    private static final int NO_OF_OPERATIONS_PER_THREAD = 5;

    private final InMemoryPriceIngestionEngine engine = new InMemoryPriceIngestionEngine();

    @Operation
    public boolean ingestLowerSequence() {
        return engine.ingest(new Quote(TICKER, 100_000_000, TIMESTAMP_MILLIS, 1));
    }

    @Operation
    public boolean ingestHigherSequence() {
        return engine.ingest(new Quote(TICKER, 300_000_000, TIMESTAMP_MILLIS, 2));
    }

    @Operation
    public String readTwoMinuteAverage() {
        return engine.getTwoMinuteAverage(TICKER, TIMESTAMP_MILLIS)
                .map(InMemoryPriceIngestionEngineLincheckTest::snapshot)
                .orElse("empty");
    }

    @Operation
    public String readFiveMinuteAverage() {
        return engine.getFiveMinuteAverage(TICKER, TIMESTAMP_MILLIS)
                .map(InMemoryPriceIngestionEngineLincheckTest::snapshot)
                .orElse("empty");
    }

    @Operation
    public String readTwoMinuteAverageAtExpiry() {
        return engine.getTwoMinuteAverage(
                        TICKER, TIMESTAMP_MILLIS + prices.enums.Window.TWO_MINUTES.durationMillis())
                .map(InMemoryPriceIngestionEngineLincheckTest::snapshot)
                .orElse("empty");
    }

    @Operation
    public String readFiveMinuteAverageAtExpiry() {
        return engine.getFiveMinuteAverage(
                        TICKER, TIMESTAMP_MILLIS + prices.enums.Window.FIVE_MINUTES.durationMillis())
                .map(InMemoryPriceIngestionEngineLincheckTest::snapshot)
                .orElse("empty");
    }

    @Test
    public void operationsAreLinearizableUnderModelChecking() {
        new ModelCheckingOptions()
                .iterations(MODEL_CHECKING_ITERATIONS)
                .invocationsPerIteration(MODEL_CHECKING_INVOCATIONS)
                .threads(THREADS)
                .actorsPerThread(MODEL_CHECKING_ACTORS_PER_THREAD)
                .check(getClass());

        printSummary(
                "model checking",
                MODEL_CHECKING_ITERATIONS,
                MODEL_CHECKING_INVOCATIONS,
                MODEL_CHECKING_ACTORS_PER_THREAD);
    }

    @Test
    public void operationsAreLinearizableUnderStress() {
        new StressOptions()
                .iterations(STRESS_ITERATIONS)
                .invocationsPerIteration(STRESS_INVOCATIONS)
                .threads(THREADS)
                .actorsPerThread(NO_OF_OPERATIONS_PER_THREAD)
                .check(getClass());

        printSummary(
                "stress",
                STRESS_ITERATIONS,
                STRESS_INVOCATIONS,
                NO_OF_OPERATIONS_PER_THREAD);
    }

    private static String snapshot(PriceAverage average) {
        return average.totalPriceMicros() + ":" + average.quoteCount();
    }

    private static void printSummary(
            String mode, int iterations, int invocations, int actorsPerThread) {
        System.out.printf(
                "Lincheck %s passed: same-ticker writes and reads were linearizable "
                        + "(%d iterations, %d invocations/iteration, %d threads, "
                        + "%d actors/thread).%n",
                mode,
                iterations,
                invocations,
                THREADS,
                actorsPerThread);
    }
}
