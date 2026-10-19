package prices.engine;

import prices.PriceIngestionEngine;
import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

public final class SnapshotPriceIngestionEngine implements PriceIngestionEngine {

    private final ConcurrentMap<String, SnapshotTickerState> states = new ConcurrentHashMap<>();
    private final LongAdder fastPathReads = new LongAdder();
    private final LongAdder staleRefreshReads = new LongAdder();

    @Override
    public boolean ingest(Quote quote) {
        if (quote == null) {
            throw new IllegalArgumentException("quote must not be null");
        }

        return states
                .computeIfAbsent(quote.ticker(), ignored -> new SnapshotTickerState())
                .ingest(quote);
    }

    @Override
    public Optional<PriceAverage> getTwoMinuteAverage(String ticker, long currentTimestampMillis) {
        return average(ticker, currentTimestampMillis, Window.TWO_MINUTES);
    }

    @Override
    public Optional<PriceAverage> getFiveMinuteAverage(String ticker, long currentTimestampMillis) {
        return average(ticker, currentTimestampMillis, Window.FIVE_MINUTES);
    }

    public ReadPathMetrics readPathMetrics() {
        return new ReadPathMetrics(fastPathReads.sum(), staleRefreshReads.sum());
    }

    private Optional<PriceAverage> average(
            String ticker, long currentTimestampMillis, Window window) {
        if (ticker == null || ticker.isBlank()) {
            throw new IllegalArgumentException("ticker must not be blank");
        }
        if (currentTimestampMillis < 0) {
            throw new IllegalArgumentException("currentTimestampMillis must not be negative");
        }

        SnapshotTickerState state = states.get(ticker);
        if (state == null) {
            return Optional.empty();
        }
        return state.average(currentTimestampMillis, window, fastPathReads, staleRefreshReads);
    }

    public record ReadPathMetrics(long fastPathReads, long staleRefreshReads) {}
}
