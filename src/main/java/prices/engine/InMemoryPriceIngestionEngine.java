package prices.engine;

import prices.PriceIngestionEngine;
import prices.enums.Window;
import prices.model.PriceAverage;
import prices.model.Quote;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryPriceIngestionEngine implements PriceIngestionEngine {

    private final ConcurrentMap<String, TickerState> states = new ConcurrentHashMap<>();

    @Override
    public boolean ingest(Quote quote) {
        // Validate before reading the ticker used as the map key.
        if (quote == null) {
            throw new IllegalArgumentException("quote must not be null");
        }

        // Reuse this ticker's state, or create it when the first quote arrives.
        // TickerState performs sequence validation and updates the rolling windows;
        // its result indicates whether the quote was accepted.
        return states
                .computeIfAbsent(quote.ticker(), ignored -> new TickerState())
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

    int tickerCount() {
        return states.size();
    }

    private Optional<PriceAverage> average(
            String ticker, long currentTimestampMillis, Window window) {
        if (ticker == null || ticker.isBlank()) {
            throw new IllegalArgumentException("ticker must not be blank");
        }
        if (currentTimestampMillis < 0) {
            throw new IllegalArgumentException("currentTimestampMillis must not be negative");
        }

        TickerState state = states.get(ticker);
        if (state == null) {
            return Optional.empty();
        }
        return state.average(currentTimestampMillis, window);
    }
}
