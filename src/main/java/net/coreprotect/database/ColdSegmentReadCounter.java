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

    private static final AtomicLong FRAME_READS = new AtomicLong();

    private static final AtomicLong PAYLOAD_READS = new AtomicLong();

    static void recordRead() {
        READS.incrementAndGet();
    }

    private static final AtomicLong GROUP_READS = new AtomicLong();

    static void recordGroupRead() {
        GROUP_READS.incrementAndGet();
    }

    /**
     * @return how many groups of rows have been decompressed and decoded, out of the segments read
     */
    public static long groupReads() {
        return GROUP_READS.get();
    }

    static void recordFrameRead() {
        FRAME_READS.incrementAndGet();
    }

    static void recordPayloadRead() {
        PAYLOAD_READS.incrementAndGet();
    }

    /**
     * @return how many times a segment's stored scalar frame had to be read from the database,
     *         rather than being found already in memory; the payload frame is read apart and
     *         counted by {@link #payloadFetches()}
     */
    public static long frameReads() {
        return FRAME_READS.get();
    }

    private static final AtomicLong UNIT_READS = new AtomicLong();

    private static final AtomicLong UNITS_OPENED = new AtomicLong();

    static void recordUnitOpened() {
        UNITS_OPENED.incrementAndGet();
    }

    /**
     * @return how many units of rows the decoder has opened, because at least one of their groups
     *         could hold a row it was asked for
     */
    public static long unitsOpened() {
        return UNITS_OPENED.get();
    }

    private static final AtomicLong BYTES_READ = new AtomicLong();

    static void recordUnitReads(long units) {
        UNIT_READS.addAndGet(units);
    }

    static void recordBytesRead(long bytes) {
        BYTES_READ.addAndGet(bytes);
    }

    /**
     * @return how many unit bodies of segments stored with their units apart have been read from
     *         the database, rather than being found already in memory
     */
    public static long unitReads() {
        return UNIT_READS.get();
    }

    /**
     * @return how many bytes of stored frames and unit bodies have been read from the database,
     *         which is what a lookup that finds nothing in memory pays for before it decodes
     */
    public static long bytesRead() {
        return BYTES_READ.get();
    }

    private static final AtomicLong PAYLOAD_FETCHES = new AtomicLong();

    private static final AtomicLong PAYLOAD_BYTES = new AtomicLong();

    /**
     * Records that a segment's stored payload frame was read from the database. The payload is read
     * apart from the scalar frame, and only once a row a lookup keeps has a text or blob value in
     * it, so this is what shows how much reading that saves.
     *
     * @param bytes
     *            how many stored bytes were read
     */
    static void recordPayloadFetch(long bytes) {
        PAYLOAD_FETCHES.incrementAndGet();
        PAYLOAD_BYTES.addAndGet(bytes);
        BYTES_READ.addAndGet(bytes);
    }

    /**
     * @return how many segment payload frames have been read from the database, rather than being
     *         found already in memory
     */
    public static long payloadFetches() {
        return PAYLOAD_FETCHES.get();
    }

    /**
     * @return how many of the bytes counted by {@link #bytesRead()} were stored payload frames
     */
    public static long payloadBytesRead() {
        return PAYLOAD_BYTES.get();
    }

    /**
     * @return how many times a segment's payload frame had to be decompressed
     */
    public static long payloadReads() {
        return PAYLOAD_READS.get();
    }

    /**
     * @return how many segments have been decoded since the server started
     */
    public static long reads() {
        return READS.get();
    }

    private static final AtomicLong SCRATCH_ROWS = new AtomicLong();

    static void recordScratchRows(long rows) {
        SCRATCH_ROWS.addAndGet(rows);
    }

    /**
     * @return how many decoded rows have been written to a lookup's temporary table, which is what
     *         a read pays for beyond decoding
     */
    public static long scratchRows() {
        return SCRATCH_ROWS.get();
    }

    private static final AtomicLong PEAK_HELD_ROWS = new AtomicLong();

    static void recordHeldRows(long rows) {
        PEAK_HELD_ROWS.accumulateAndGet(rows, Math::max);
    }

    /**
     * @return the most rows one lookup has held in its temporary tables at once since the last
     *         reset, which is what bounds the temporary space a lookup takes
     */
    public static long peakHeldRows() {
        return PEAK_HELD_ROWS.get();
    }

    /** Starts measuring {@link #peakHeldRows()} afresh. */
    public static void resetPeakHeldRows() {
        PEAK_HELD_ROWS.set(0);
    }
}
