package prices.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Exact, unbounded aggregate used to derive a displayable decimal average
 * without losing the underlying integer sum or count.
 */
public record PriceAverage(BigInteger totalPriceMicros, long quoteCount) {

    private static final int PRICE_SCALE = 6;

    public PriceAverage {
        Objects.requireNonNull(totalPriceMicros, "totalPriceMicros");
        if (quoteCount <= 0) {
            throw new IllegalArgumentException("quoteCount must be positive");
        }
    }

    public BigDecimal asBigDecimal(int scale, RoundingMode roundingMode) {
        if (scale < 0) {
            throw new IllegalArgumentException("scale must not be negative");
        }
        var total = new BigDecimal(totalPriceMicros, PRICE_SCALE);
        return total.divide(BigDecimal.valueOf(quoteCount), scale, roundingMode);
    }
}
