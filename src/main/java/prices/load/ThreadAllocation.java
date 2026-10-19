package prices.load;

import java.lang.management.ManagementFactory;

final class ThreadAllocation {

    private static final com.sun.management.ThreadMXBean THREAD_BEAN = threadBean();

    private ThreadAllocation() {
    }

    static long currentThreadAllocatedBytes() {
        if (THREAD_BEAN == null) {
            return -1;
        }
        return THREAD_BEAN.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private static com.sun.management.ThreadMXBean threadBean() {
        if (!(ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean bean)) {
            return null;
        }

        try {
            if (!bean.isThreadAllocatedMemorySupported()) {
                return null;
            }
            if (!bean.isThreadAllocatedMemoryEnabled()) {
                bean.setThreadAllocatedMemoryEnabled(true);
            }
            return bean;
        } catch (RuntimeException unsupportedOrDenied) {
            return null;
        }
    }
}
