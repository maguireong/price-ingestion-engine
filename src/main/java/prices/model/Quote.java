package prices.model;

/**
 * A price quote represented using fixed-point millionths.
 */
public record Quote(String ticker, long priceMicros, long timestampMillis, long sequenceNumber) {

    public Quote {
        if (ticker == null || ticker.isBlank()) {
            throw new IllegalArgumentException("ticker must not be blank");
        }
        if (!ticker.equals(ticker.trim())) {
            throw new IllegalArgumentException("ticker must not have surrounding whitespace");
        }
        if (priceMicros < 0) {
            throw new IllegalArgumentException("priceMicros must not be negative");
        }
        if (timestampMillis < 0) {
            throw new IllegalArgumentException("timestampMillis must not be negative");
        }
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("sequenceNumber must not be negative");
        }
    }
}
