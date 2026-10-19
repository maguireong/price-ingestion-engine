package prices.load;

import java.util.Arrays;
import java.util.List;

record LatencySummary(
        long operationCount,
        int sampleCount,
        long p50Nanos,
        long p95Nanos,
        long p99Nanos,
        long maxNanos) {

    static LatencySummary combine(List<LatencyRecorder> recorders) {
        long operations = recorders.stream().mapToLong(LatencyRecorder::operationCount).sum();
        int samples = recorders.stream().mapToInt(recorder -> recorder.samples().length).sum();
        if (samples == 0) {
            return new LatencySummary(operations, 0, 0, 0, 0, 0);
        }

        long[] combined = new long[samples];
        int offset = 0;
        for (LatencyRecorder recorder : recorders) {
            long[] workerSamples = recorder.samples();
            System.arraycopy(workerSamples, 0, combined, offset, workerSamples.length);
            offset += workerSamples.length;
        }
        Arrays.sort(combined);

        return new LatencySummary(
                operations,
                samples,
                percentile(combined, 50),
                percentile(combined, 95),
                percentile(combined, 99),
                combined[combined.length - 1]);
    }

    private static long percentile(long[] sortedSamples, int percentile) {
        int nearestRank = (int) Math.ceil(percentile / 100.0 * sortedSamples.length);
        return sortedSamples[Math.max(0, nearestRank - 1)];
    }
}
