package prices.load;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

record GcSnapshot(long collectionCount, long collectionTimeMillis) {

    static GcSnapshot capture() {
        long count = 0;
        long timeMillis = 0;
        for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            count += Math.max(0, collector.getCollectionCount());
            timeMillis += Math.max(0, collector.getCollectionTime());
        }
        return new GcSnapshot(count, timeMillis);
    }

    GcSnapshot differenceFrom(GcSnapshot earlier) {
        return new GcSnapshot(
                collectionCount - earlier.collectionCount,
                collectionTimeMillis - earlier.collectionTimeMillis);
    }
}
