package prices.load;

import java.util.LinkedHashMap;
import java.util.Map;

record LoadTestConfig(
        EngineMode engineMode,
        int durationSeconds,
        int warmupSeconds,
        int targetQuotesPerSecond,
        int targetReadsPerSecond,
        int tickerCount,
        int producerThreads,
        int readerThreads,
        TickerDistribution tickerDistribution,
        double zipfExponent,
        int hotTickerPercent,
        int hotTrafficPercent,
        int sampleEvery,
        long randomSeed) {

    private static final Map<String, String> DEFAULTS = defaults();

    LoadTestConfig {
        requirePositive(durationSeconds, "duration-seconds");
        requireNonNegative(warmupSeconds, "warmup-seconds");
        requirePositive(targetQuotesPerSecond, "target-quotes-per-second");
        requirePositive(targetReadsPerSecond, "target-reads-per-second");
        requirePositive(tickerCount, "ticker-count");
        requirePositive(producerThreads, "producer-threads");
        requirePositive(readerThreads, "reader-threads");
        if (!Double.isFinite(zipfExponent) || zipfExponent <= 0.0) {
            throw new IllegalArgumentException("zipf-exponent must be finite and positive");
        }
        requirePercent(hotTickerPercent, "hot-ticker-percent");
        requirePercent(hotTrafficPercent, "hot-traffic-percent");
        requirePositive(sampleEvery, "sample-every");

        if (tickerCount < producerThreads) {
            throw new IllegalArgumentException(
                    "ticker-count must provide at least one ticker per producer");
        }
        if (targetQuotesPerSecond < producerThreads) {
            throw new IllegalArgumentException(
                    "target-quotes-per-second must be at least producer-threads");
        }
        if (targetReadsPerSecond < readerThreads) {
            throw new IllegalArgumentException(
                    "target-reads-per-second must be at least reader-threads");
        }

        if (tickerDistribution == TickerDistribution.HOT_COLD) {
            int hotTickerCount = Math.max(1, tickerCount * hotTickerPercent / 100);
            int coldTickerCount = tickerCount - hotTickerCount;
            if (hotTickerCount < producerThreads || coldTickerCount < producerThreads) {
                throw new IllegalArgumentException(
                        "hot-ticker-percent must provide at least one hot and one cold ticker per producer");
            }
        }
    }

    static LoadTestConfig defaultsConfig() {
        return from(DEFAULTS);
    }

    static LoadTestConfig parse(String[] arguments) {
        Map<String, String> values = new LinkedHashMap<>(DEFAULTS);
        for (String argument : arguments) {
            if (!argument.startsWith("--") || !argument.contains("=")) {
                throw new IllegalArgumentException(
                        "arguments must use --name=value syntax: " + argument);
            }

            int separator = argument.indexOf('=');
            String name = argument.substring(2, separator);
            String value = argument.substring(separator + 1);
            if (!values.containsKey(name)) {
                throw new IllegalArgumentException("unknown argument: --" + name);
            }
            if (value.isBlank()) {
                throw new IllegalArgumentException("argument must have a value: --" + name);
            }
            values.put(name, value);
        }
        return from(values);
    }

    static String usage() {
        return """
                Options use --name=value syntax:
                  --engine=baseline
                  --duration-seconds=30
                  --warmup-seconds=5
                  --target-quotes-per-second=50000
                  --target-reads-per-second=50000
                  --ticker-count=10000
                  --producer-threads=4
                  --reader-threads=4
                  --ticker-distribution=hot-cold
                  --zipf-exponent=1.0
                  --hot-ticker-percent=20
                  --hot-traffic-percent=80
                  --sample-every=100
                  --random-seed=42
                """;
    }

    private static LoadTestConfig from(Map<String, String> values) {
        try {
            return new LoadTestConfig(
                    EngineMode.parse(values.get("engine")),
                    integer(values, "duration-seconds"),
                    integer(values, "warmup-seconds"),
                    integer(values, "target-quotes-per-second"),
                    integer(values, "target-reads-per-second"),
                    integer(values, "ticker-count"),
                    integer(values, "producer-threads"),
                    integer(values, "reader-threads"),
                    TickerDistribution.parse(values.get("ticker-distribution")),
                    Double.parseDouble(values.get("zipf-exponent")),
                    integer(values, "hot-ticker-percent"),
                    integer(values, "hot-traffic-percent"),
                    integer(values, "sample-every"),
                    Long.parseLong(values.get("random-seed")));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("load-test arguments must be valid numbers", failure);
        }
    }

    private static int integer(Map<String, String> values, String name) {
        return Integer.parseInt(values.get(name));
    }

    private static Map<String, String> defaults() {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("engine", "baseline");
        defaults.put("duration-seconds", "30");
        defaults.put("warmup-seconds", "5");
        defaults.put("target-quotes-per-second", "50000");
        defaults.put("target-reads-per-second", "50000");
        defaults.put("ticker-count", "10000");
        defaults.put("producer-threads", "4");
        defaults.put("reader-threads", "4");
        defaults.put("ticker-distribution", "hot-cold");
        defaults.put("zipf-exponent", "1.0");
        defaults.put("hot-ticker-percent", "20");
        defaults.put("hot-traffic-percent", "80");
        defaults.put("sample-every", "100");
        defaults.put("random-seed", "42");
        return Map.copyOf(defaults);
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }

    private static void requirePercent(int value, String name) {
        if (value <= 0 || value >= 100) {
            throw new IllegalArgumentException(name + " must be between 1 and 99");
        }
    }
}
