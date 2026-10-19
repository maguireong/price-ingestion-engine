package prices.load;

import java.util.Arrays;

final class LatencyRecorder {

    private final int sampleEvery;
    private long operationCount;
    private long[] samples = new long[1_024];
    private int sampleCount;

    LatencyRecorder(int sampleEvery) {
        if (sampleEvery <= 0) {
            throw new IllegalArgumentException("sampleEvery must be positive");
        }
        this.sampleEvery = sampleEvery;
    }

    void record(long latencyNanos) {
        if (latencyNanos < 0) {
            throw new IllegalArgumentException("latencyNanos must not be negative");
        }

        operationCount++;
        if (operationCount % sampleEvery != 0) {
            return;
        }

        if (sampleCount == samples.length) {
            samples = Arrays.copyOf(samples, samples.length * 2);
        }
        samples[sampleCount++] = latencyNanos;
    }

    long operationCount() {
        return operationCount;
    }

    long[] samples() {
        return Arrays.copyOf(samples, sampleCount);
    }
}
