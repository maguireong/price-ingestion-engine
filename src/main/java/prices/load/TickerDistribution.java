package prices.load;

enum TickerDistribution {
    HOT_COLD("hot-cold"),
    ZIPF("zipf");

    private final String optionValue;

    TickerDistribution(String optionValue) {
        this.optionValue = optionValue;
    }

    String optionValue() {
        return optionValue;
    }

    static TickerDistribution parse(String value) {
        for (TickerDistribution distribution : values()) {
            if (distribution.optionValue.equals(value)) {
                return distribution;
            }
        }
        throw new IllegalArgumentException("ticker-distribution must be hot-cold or zipf");
    }
}
