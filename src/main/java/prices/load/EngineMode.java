package prices.load;

import prices.PriceIngestionEngine;
import prices.engine.InMemoryPriceIngestionEngine;
import prices.engine.SnapshotPriceIngestionEngine;

enum EngineMode {
    BASELINE("baseline") {
        @Override
        PriceIngestionEngine create() {
            return new InMemoryPriceIngestionEngine();
        }
    },
    SNAPSHOT("snapshot") {
        @Override
        PriceIngestionEngine create() {
            return new SnapshotPriceIngestionEngine();
        }
    };

    private final String optionValue;

    EngineMode(String optionValue) {
        this.optionValue = optionValue;
    }

    abstract PriceIngestionEngine create();

    String optionValue() {
        return optionValue;
    }

    static EngineMode parse(String value) {
        for (EngineMode mode : values()) {
            if (mode.optionValue.equals(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("engine must be baseline or snapshot");
    }
}
