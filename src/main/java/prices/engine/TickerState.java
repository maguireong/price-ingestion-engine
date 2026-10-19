package prices.engine;

import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

final class TickerState {

    private final Deque<Quote> twoMinuteQuotes = new ArrayDeque<>();
    private final Deque<Quote> fiveMinuteQuotes = new ArrayDeque<>();

    private BigInteger twoMinuteSum = BigInteger.ZERO;
    private BigInteger fiveMinuteSum = BigInteger.ZERO;
    private long lastSequenceNumber = -1;
    private long lastTimestampMillis = -1;

    synchronized boolean ingest(Quote quote) {
        if (quote.sequenceNumber() <= lastSequenceNumber) {
            return false;
        }

        // The deques are timestamp ordered, so an accepted quote cannot be older
        // than the last quote already stored for this ticker.
        if (quote.timestampMillis() < lastTimestampMillis) {
            throw new IllegalArgumentException("quote timestamps must not move backwards for a ticker");
        }

        expire(quote.timestampMillis());
        BigInteger priceMicros = BigInteger.valueOf(quote.priceMicros());
        BigInteger updatedTwoMinuteSum = twoMinuteSum.add(priceMicros);
        BigInteger updatedFiveMinuteSum = fiveMinuteSum.add(priceMicros);

        // The same quote contributes to both windows until each one expires it.
        twoMinuteQuotes.addLast(quote);
        fiveMinuteQuotes.addLast(quote);

        // Commit the running aggregates and ordering metadata for this ticker.
        twoMinuteSum = updatedTwoMinuteSum;
        fiveMinuteSum = updatedFiveMinuteSum;
        lastSequenceNumber = quote.sequenceNumber();
        lastTimestampMillis = quote.timestampMillis();
        return true;
    }

    synchronized Optional<PriceAverage> average(long currentTimestampMillis, Window window) {
        if (currentTimestampMillis < lastTimestampMillis) {
            throw new IllegalArgumentException("historical average queries are not supported");
        }
        expire(currentTimestampMillis);

        return switch (window) {
            case TWO_MINUTES -> toAverage(twoMinuteSum, twoMinuteQuotes.size());
            case FIVE_MINUTES -> toAverage(fiveMinuteSum, fiveMinuteQuotes.size());
        };
    }

    private void expire(long currentTimestampMillis) {
        twoMinuteSum = expireBeforeOrAt(
                twoMinuteQuotes,
                twoMinuteSum,
                currentTimestampMillis - Window.TWO_MINUTES.durationMillis());
        fiveMinuteSum = expireBeforeOrAt(
                fiveMinuteQuotes,
                fiveMinuteSum,
                currentTimestampMillis - Window.FIVE_MINUTES.durationMillis());
    }

    private static BigInteger expireBeforeOrAt(
            Deque<Quote> quotes, BigInteger sum, long cutoffMillis) {
        while (!quotes.isEmpty() && quotes.peekFirst().timestampMillis() <= cutoffMillis) {
            sum = sum.subtract(BigInteger.valueOf(quotes.removeFirst().priceMicros()));
        }
        return sum;
    }

    private static Optional<PriceAverage> toAverage(BigInteger sum, int count) {
        return count == 0
                ? Optional.empty()
                : Optional.of(new PriceAverage(sum, count));
    }
}
