package prices.load;

import java.util.SplittableRandom;
import org.junit.Test;

public final class ZipfianTickerSelectorTest {

    @Test
    public void createsAReproducibleLongTailAcrossTickerRanks() {
        int tickerCount = 100;
        int[] tickerIndexes = new int[tickerCount];
        for (int index = 0; index < tickerIndexes.length; index++) {
            tickerIndexes[index] = index;
        }

        var selector = new ZipfianTickerSelector(tickerIndexes, 1.0);
        var firstRandom = new SplittableRandom(42);
        var secondRandom = new SplittableRandom(42);
        long[] selectionsByTicker = new long[tickerCount];

        for (int selection = 0; selection < 500_000; selection++) {
            int first = selector.select(firstRandom);
            int second = selector.select(secondRandom);
            assertEquals(first, second, "selection is reproducible");
            selectionsByTicker[first]++;
        }

        assertTrue(
                selectionsByTicker[0] > selectionsByTicker[9],
                "the top-ranked ticker is selected more often than rank 10");
        assertTrue(
                selectionsByTicker[9] > selectionsByTicker[99],
                "rank 10 is selected more often than rank 100");
        assertTrue(
                sum(selectionsByTicker, 0, 10) > sum(selectionsByTicker, 90, 100),
                "the head receives more traffic than the tail");
    }

    @Test
    public void selectsOnlyFromTheTickersOwnedByTheProducer() {
        int[] ownedTickerIndexes = {1, 5, 9, 13};
        var selector = new ZipfianTickerSelector(ownedTickerIndexes, 1.0);
        var random = new SplittableRandom(7);

        for (int selection = 0; selection < 10_000; selection++) {
            int tickerIndex = selector.select(random);
            assertTrue(contains(ownedTickerIndexes, tickerIndex), "selected ticker is owned");
        }
    }

    private static long sum(long[] values, int startInclusive, int endExclusive) {
        long result = 0;
        for (int index = startInclusive; index < endExclusive; index++) {
            result += values[index];
        }
        return result;
    }

    private static boolean contains(int[] values, int expected) {
        for (int value : values) {
            if (value == expected) {
                return true;
            }
        }
        return false;
    }

    private static void assertEquals(int expected, int actual, String description) {
        if (expected != actual) {
            throw new AssertionError(description + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertTrue(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError(description);
        }
    }
}
