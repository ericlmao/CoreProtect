package net.coreprotect.database;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts how many segments have been decoded.
 *
 * <p>
 * Reading a segment is the expensive part of answering a lookup, so this makes it possible to state
 * plainly, in a test or in the console, how much reading a query actually did rather than inferring
 * it from timings.
 * </p>
 */
public final class ColdSegmentReadCounter {

    private static final AtomicLong READS = new AtomicLong();

    private ColdSegmentReadCounter() {
        throw new IllegalStateException("Utility class");
    }

    static void recordRead() {
        READS.incrementAndGet();
    }

    /**
     * @return how many segments have been decoded since the server started
     */
    public static long reads() {
        return READS.get();
    }
}
