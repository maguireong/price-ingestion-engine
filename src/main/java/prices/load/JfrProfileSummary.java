package prices.load;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

public final class JfrProfileSummary {

    private static final String TICKER_STATE_CLASS = "prices.engine.TickerState";
    private static final String SNAPSHOT_TICKER_STATE_CLASS = "prices.engine.SnapshotTickerState";

    private JfrProfileSummary() {}

    public static void main(String[] arguments) throws IOException {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("expected one .jfr recording path");
        }

        long threadParks = 0;
        long executionSamples = 0;
        long allocationSamples = 0;
        long garbageCollections = 0;
        Duration garbageCollectionTime = Duration.ZERO;
        long monitorContentions = 0;
        long tickerStateContentions = 0;
        Duration tickerStateContentionTime = Duration.ZERO;
        Duration longestTickerStateContention = Duration.ZERO;

        try (var recording = new RecordingFile(Path.of(arguments[0]))) {
            while (recording.hasMoreEvents()) {
                RecordedEvent event = recording.readEvent();
                switch (event.getEventType().getName()) {
                    case "jdk.ThreadPark" -> threadParks++;
                    case "jdk.ExecutionSample", "jdk.NativeMethodSample" -> executionSamples++;
                    case "jdk.ObjectAllocationSample" -> allocationSamples++;
                    case "jdk.GarbageCollection" -> {
                        garbageCollections++;
                        garbageCollectionTime = garbageCollectionTime.plus(event.getDuration());
                    }
                    case "jdk.JavaMonitorEnter" -> {
                        monitorContentions++;
                        RecordedClass monitorClass = event.getClass("monitorClass");
                        if (isTickerState(monitorClass)) {
                            tickerStateContentions++;
                            Duration duration = event.getDuration();
                            tickerStateContentionTime = tickerStateContentionTime.plus(duration);
                            if (duration.compareTo(longestTickerStateContention) > 0) {
                                longestTickerStateContention = duration;
                            }
                        }
                    }
                    default -> {
                        // The compact summary intentionally ignores JVM metadata events.
                    }
                }
            }
        }

        System.out.println("Java Flight Recorder bottleneck signals");
        System.out.printf(
                "  Ticker locks: %,d contentions >= 1 us, %.3f ms total wait, %.3f ms longest wait%n",
                tickerStateContentions,
                millis(tickerStateContentionTime),
                millis(longestTickerStateContention));
        System.out.printf("  All monitors: %,d contentions >= 1 us%n", monitorContentions);
        System.out.printf("  CPU samples: %,d runnable execution samples%n", executionSamples);
        System.out.printf("  Allocation: %,d sampled allocation events%n", allocationSamples);
        System.out.printf(
                "  GC: %,d recording-wide collections, %.3f ms total duration%n",
                garbageCollections,
                millis(garbageCollectionTime));
        System.out.printf(
                "  Pacing: %,d thread parks; these are expected from rate scheduling%n",
                threadParks);
        System.out.println("  Queue: not applicable; the baseline invokes the engine directly");
    }

    private static double millis(Duration duration) {
        return duration.toNanos() / 1_000_000.0;
    }

    private static boolean isTickerState(RecordedClass monitorClass) {
        return TICKER_STATE_CLASS.equals(monitorClass.getName())
                || SNAPSHOT_TICKER_STATE_CLASS.equals(monitorClass.getName());
    }
}
