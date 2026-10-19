package prices.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Exact aggregate used to derive a displayable decimal average without losing
 * the underlying integer sum or count.
 */
public record PriceAverage(long totalPriceMicros, long quoteCount) {

    private static final int PRICE_SCALE = 6;

    public PriceAverage {
        if (quoteCount <= 0) {
            throw new IllegalArgumentException("quoteCount must be positive");
        }
    }

    public BigDecimal asBigDecimal(int scale, RoundingMode roundingMode) {
        if (scale < 0) {
            throw new IllegalArgumentException("scale must not be negative");
        }
        var total = BigDecimal.valueOf(totalPriceMicros, PRICE_SCALE);
        return total.divide(BigDecimal.valueOf(quoteCount), scale, roundingMode);
    }
}
