package prices.load;

record PhaseResult(
        long elapsedNanos,
        long attemptedQuotes,
        long acceptedQuotes,
        long rejectedQuotes,
        long reads,
        long emptyReads,
        long historicalReadRetries,
        LatencySummary writeLatency,
        LatencySummary readLatency,
        long heapStartBytes,
        long heapPeakBytes,
        long heapEndBytes,
        long allocatedBytes,
        GcSnapshot garbageCollection,
        AccuracySummary accuracy,
        long snapshotFastPathReads,
        long snapshotStaleRefreshReads) {
}
