package prices.enums;

public enum Window {
    TWO_MINUTES(2 * 60 * 1_000L),
    FIVE_MINUTES(5 * 60 * 1_000L);

    private final long durationMillis;

    Window(long durationMillis) {
        this.durationMillis = durationMillis;
    }

    public long durationMillis() {
        return durationMillis;
    }
}
