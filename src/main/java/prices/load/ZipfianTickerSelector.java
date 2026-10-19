package prices.load;

import java.util.SplittableRandom;

final class ZipfianTickerSelector {

    private final int[] tickerIndexes;
    private final double[] cumulativeWeights;
    private final double totalWeight;

    ZipfianTickerSelector(int[] tickerIndexes, double exponent) {
        if (tickerIndexes.length == 0) {
            throw new IllegalArgumentException("tickerIndexes must not be empty");
        }
        if (!Double.isFinite(exponent) || exponent <= 0.0) {
            throw new IllegalArgumentException("zipf exponent must be finite and positive");
        }

        this.tickerIndexes = tickerIndexes.clone();
        this.cumulativeWeights = new double[tickerIndexes.length];

        double cumulativeWeight = 0.0;
        for (int index = 0; index < tickerIndexes.length; index++) {
            int tickerIndex = tickerIndexes[index];
            if (tickerIndex < 0) {
                throw new IllegalArgumentException("ticker indexes must not be negative");
            }
            cumulativeWeight += 1.0 / Math.pow(tickerIndex + 1.0, exponent);
            cumulativeWeights[index] = cumulativeWeight;
        }
        totalWeight = cumulativeWeight;
    }

    int select(SplittableRandom random) {
        double target = random.nextDouble(totalWeight);
        int low = 0;
        int high = cumulativeWeights.length - 1;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (target < cumulativeWeights[middle]) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        return tickerIndexes[low];
    }

    double totalWeight() {
        return totalWeight;
    }
}
