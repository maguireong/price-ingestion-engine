package prices.load;

import prices.PriceIngestionEngine;
import prices.enums.Window;
import prices.model.PriceAverage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class AccuracyValidator {

    private static final int MAX_MISMATCH_EXAMPLES = 5;

    private AccuracyValidator() {}

    static AccuracySummary validate(
            PriceIngestionEngine engine,
            String[] tickers,
            ReferenceTickerState[] referenceStates,
            long currentTimestampMillis) {
        long nonEmptyAveragesChecked = 0;
        long expectedEmptyResultsExcluded = 0;
        long mismatches = 0;
        List<String> mismatchExamples = new ArrayList<>();

        for (int tickerIndex = 0; tickerIndex < tickers.length; tickerIndex++) {
            String ticker = tickers[tickerIndex];
            ReferenceTickerState referenceState = referenceStates[tickerIndex];

            for (Window window : Window.values()) {
                Optional<PriceAverage> expected = referenceState.average(currentTimestampMillis, window);
                Optional<PriceAverage> actual = read(engine, ticker, currentTimestampMillis, window);

                if (expected.isEmpty()) {
                    expectedEmptyResultsExcluded++;
                } else {
                    nonEmptyAveragesChecked++;
                }

                if (!expected.equals(actual)) {
                    mismatches++;
                    if (mismatchExamples.size() < MAX_MISMATCH_EXAMPLES) {
                        mismatchExamples.add(String.format(
                                "%s %s expected=%s actual=%s",
                                ticker,
                                window,
                                describe(expected),
                                describe(actual)));
                    }
                }
            }
        }

        return new AccuracySummary(
                nonEmptyAveragesChecked,
                expectedEmptyResultsExcluded,
                mismatches,
                mismatchExamples);
    }

    private static Optional<PriceAverage> read(
            PriceIngestionEngine engine,
            String ticker,
            long currentTimestampMillis,
            Window window) {
        return switch (window) {
            case TWO_MINUTES -> engine.getTwoMinuteAverage(ticker, currentTimestampMillis);
            case FIVE_MINUTES -> engine.getFiveMinuteAverage(ticker, currentTimestampMillis);
        };
    }

    private static String describe(Optional<PriceAverage> average) {
        return average
                .map(value -> value.totalPriceMicros() + ":" + value.quoteCount())
                .orElse("empty");
    }
}
