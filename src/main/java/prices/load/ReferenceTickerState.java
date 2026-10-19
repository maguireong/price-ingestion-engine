package prices.load;

import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

/**
 * Independent, scan-based reference state used only by the load harness.
 */
final class ReferenceTickerState {

    private final Deque<Quote> quotes = new ArrayDeque<>();

    void accept(Quote quote) {
        long fiveMinuteCutoff = quote.timestampMillis() - Window.FIVE_MINUTES.durationMillis();
        while (!quotes.isEmpty() && quotes.peekFirst().timestampMillis() <= fiveMinuteCutoff) {
            quotes.removeFirst();
        }
        quotes.addLast(quote);
    }

    Optional<PriceAverage> average(long currentTimestampMillis, Window window) {
        long cutoffMillis = currentTimestampMillis - window.durationMillis();
        long sum = 0;
        long count = 0;

        for (Quote quote : quotes) {
            if (quote.timestampMillis() > cutoffMillis
                    && quote.timestampMillis() <= currentTimestampMillis) {
                sum = Math.addExact(sum, quote.priceMicros());
                count++;
            }
        }

        return count == 0
                ? Optional.empty()
                : Optional.of(new PriceAverage(sum, count));
    }
}
