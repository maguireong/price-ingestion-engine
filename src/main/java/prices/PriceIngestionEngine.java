package prices;

import prices.model.PriceAverage;
import prices.model.Quote;
import java.util.Optional;

public interface PriceIngestionEngine {

    /**
     * @return true when accepted, or false when the sequence number is duplicate or stale
     */
    boolean ingest(Quote quote);

    Optional<PriceAverage> getTwoMinuteAverage(String ticker, long currentTimestampMillis);

    Optional<PriceAverage> getFiveMinuteAverage(String ticker, long currentTimestampMillis);
}
