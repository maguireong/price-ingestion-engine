package prices.engine;

import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

final class SnapshotTickerState {

    private static final long NO_EXPIRY = Long.MAX_VALUE;

    private final Deque<Quote> twoMinuteQuotes = new ArrayDeque<>();
    private final Deque<Quote> fiveMinuteQuotes = new ArrayDeque<>();
    private final AtomicReference<AverageSnapshot> published =
            new AtomicReference<>(AverageSnapshot.empty());

    private long twoMinuteSum;
    private long fiveMinuteSum;
    private long lastSequenceNumber = -1;
    private long lastTimestampMillis = -1;

    synchronized boolean ingest(Quote quote) {
        if (quote.sequenceNumber() <= lastSequenceNumber) {
            return false;
        }
        if (quote.timestampMillis() < lastTimestampMillis) {
            throw new IllegalArgumentException("quote timestamps must not move backwards for a ticker");
        }

        expire(quote.timestampMillis());
        long updatedTwoMinuteSum = Math.addExact(twoMinuteSum, quote.priceMicros());
        long updatedFiveMinuteSum = Math.addExact(fiveMinuteSum, quote.priceMicros());

        twoMinuteQuotes.addLast(quote);
        fiveMinuteQuotes.addLast(quote);
        twoMinuteSum = updatedTwoMinuteSum;
        fiveMinuteSum = updatedFiveMinuteSum;
        lastSequenceNumber = quote.sequenceNumber();
        lastTimestampMillis = quote.timestampMillis();
        publish();
        return true;
    }

    Optional<PriceAverage> average(
            long currentTimestampMillis,
            Window window,
            LongAdder fastPathReads,
            LongAdder staleRefreshReads) {
        AverageSnapshot snapshot = published.get();
        validateTimestamp(currentTimestampMillis, snapshot.lastTimestampMillis());
        if (currentTimestampMillis < snapshot.validUntil(window)) {
            fastPathReads.increment();
            return snapshot.average(window);
        }

        staleRefreshReads.increment();
        return refreshAndRead(currentTimestampMillis, window);
    }

    private synchronized Optional<PriceAverage> refreshAndRead(
            long currentTimestampMillis, Window window) {
        AverageSnapshot snapshot = published.get();
        validateTimestamp(currentTimestampMillis, lastTimestampMillis);
        if (currentTimestampMillis < snapshot.validUntil(window)) {
            return snapshot.average(window);
        }

        expire(currentTimestampMillis);
        publish();
        return published.get().average(window);
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

    private void publish() {
        published.set(new AverageSnapshot(
                lastTimestampMillis,
                twoMinuteSum,
                twoMinuteQuotes.size(),
                fiveMinuteSum,
                fiveMinuteQuotes.size(),
                nextExpiry(twoMinuteQuotes, Window.TWO_MINUTES),
                nextExpiry(fiveMinuteQuotes, Window.FIVE_MINUTES)));
    }

    private static long expireBeforeOrAt(Deque<Quote> quotes, long sum, long cutoffMillis) {
        while (!quotes.isEmpty() && quotes.peekFirst().timestampMillis() <= cutoffMillis) {
            sum = Math.subtractExact(sum, quotes.removeFirst().priceMicros());
        }
        return sum;
    }

    private static long nextExpiry(Deque<Quote> quotes, Window window) {
        if (quotes.isEmpty()) {
            return NO_EXPIRY;
        }
        long timestamp = quotes.peekFirst().timestampMillis();
        long duration = window.durationMillis();
        return timestamp > Long.MAX_VALUE - duration ? Long.MAX_VALUE : timestamp + duration;
    }

    private static void validateTimestamp(long currentTimestampMillis, long lastTimestampMillis) {
        if (currentTimestampMillis < lastTimestampMillis) {
            throw new IllegalArgumentException("historical average queries are not supported");
        }
    }

    private record AverageSnapshot(
            long lastTimestampMillis,
            long twoMinuteSum,
            int twoMinuteCount,
            long fiveMinuteSum,
            int fiveMinuteCount,
            long twoMinuteValidUntilMillis,
            long fiveMinuteValidUntilMillis) {

        private static AverageSnapshot empty() {
            return new AverageSnapshot(-1, 0, 0, 0, 0, NO_EXPIRY, NO_EXPIRY);
        }

        private long validUntil(Window window) {
            return switch (window) {
                case TWO_MINUTES -> twoMinuteValidUntilMillis;
                case FIVE_MINUTES -> fiveMinuteValidUntilMillis;
            };
        }

        private Optional<PriceAverage> average(Window window) {
            long sum = window == Window.TWO_MINUTES ? twoMinuteSum : fiveMinuteSum;
            int count = window == Window.TWO_MINUTES ? twoMinuteCount : fiveMinuteCount;
            return count == 0
                    ? Optional.empty()
                    : Optional.of(new PriceAverage(sum, count));
        }
    }
}
