package net.coreprotect.database;

import java.util.Arrays;
import java.util.Map;

/**
 * Records how many rows of a segment belong to each value of a column.
 *
 * <p>
 * Knowing that a segment holds four thousand rows for a given player, rather than merely that it
 * holds some, turns two expensive questions into arithmetic: how many rows a lookup will find, and
 * which segment holds the rows a particular page needs. A lookup can then read one segment instead
 * of every segment, no matter how much history there is.
 * </p>
 *
 * <p>
 * The counts are written once when a segment is sealed and are never updated, because a segment is
 * never modified after that.
 * </p>
 */
public final class SegmentStatistics {

    /** Values counted per segment before the statistics are dropped as too varied to be useful. */
    static final int MAXIMUM_VALUES = 4096;

    private static final int VERSION = 1;

    private final long[] values;
    private final int[] counts;

    private SegmentStatistics(long[] values, int[] counts) {
        this.values = values;
        this.counts = counts;
    }

    /**
     * Encodes the row counts of one column in one segment.
     *
     * @param counts
     *            how many rows each value accounts for
     * @return the stored form, or null when there is nothing worth storing
     */
    public static byte[] encode(Map<Long, Integer> counts) {
        if (counts == null || counts.isEmpty() || counts.size() > MAXIMUM_VALUES) {
            return null;
        }

        long[] values = new long[counts.size()];
        int index = 0;
        for (Long value : counts.keySet()) {
            values[index++] = value;
        }
        Arrays.sort(values);

        Output output = new Output(2 + (values.length * 6));
        output.write(VERSION);
        output.writeVarUnsigned(values.length);
        long previous = 0;
        for (long value : values) {
            output.writeZigZag(value - previous);
            previous = value;
        }
        for (long value : values) {
            output.writeVarUnsigned(counts.get(value));
        }
        return output.toByteArray();
    }

    /**
     * @param encoded
     *            the stored form, may be null
     * @return the decoded statistics, or null when the segment recorded none
     */
    public static SegmentStatistics decode(byte[] encoded) {
        if (encoded == null || encoded.length < 2 || encoded[0] != VERSION) {
            return null;
        }

        try {
            Input input = new Input(encoded);
            input.read();
            int count = (int) input.readVarUnsigned();
            if (count <= 0 || count > MAXIMUM_VALUES) {
                return null;
            }

            long[] values = new long[count];
            long previous = 0;
            for (int index = 0; index < count; index++) {
                previous = previous + input.readZigZag();
                values[index] = previous;
            }

            int[] counts = new int[count];
            for (int index = 0; index < count; index++) {
                counts[index] = (int) input.readVarUnsigned();
            }
            return new SegmentStatistics(values, counts);
        }
        catch (RuntimeException exception) {
            // Statistics are an optimisation; unreadable ones simply mean the segment is opened.
            return null;
        }
    }

    /**
     * @param value
     *            the value to count
     * @return how many rows of the segment hold that value
     */
    public int count(long value) {
        int index = Arrays.binarySearch(values, value);
        return index < 0 ? 0 : counts[index];
    }

    /**
     * @param wanted
     *            the values a lookup asked for
     * @return how many rows of the segment hold any of them
     */
    public int count(long[] wanted) {
        int total = 0;
        for (long value : wanted) {
            total = total + count(value);
        }
        return total;
    }

    /** Minimal writer for the statistics encoding. */
    private static final class Output {
        private byte[] buffer;
        private int length;

        private Output(int capacity) {
            this.buffer = new byte[Math.max(16, capacity)];
        }

        private void write(int value) {
            if (length == buffer.length) {
                buffer = Arrays.copyOf(buffer, buffer.length * 2);
            }
            buffer[length++] = (byte) value;
        }

        private void writeVarUnsigned(long value) {
            long remaining = value;
            while ((remaining & ~0x7FL) != 0) {
                write((int) ((remaining & 0x7F) | 0x80));
                remaining >>>= 7;
            }
            write((int) remaining);
        }

        private void writeZigZag(long value) {
            writeVarUnsigned((value << 1) ^ (value >> 63));
        }

        private byte[] toByteArray() {
            return Arrays.copyOf(buffer, length);
        }
    }

    /** Reader for the statistics encoding. */
    private static final class Input {
        private final byte[] data;
        private int offset;

        private Input(byte[] data) {
            this.data = data;
        }

        private int read() {
            if (offset >= data.length) {
                throw new IllegalArgumentException("Segment statistics ended unexpectedly");
            }
            return Byte.toUnsignedInt(data[offset++]);
        }

        private long readVarUnsigned() {
            long value = 0;
            int shift = 0;
            while (true) {
                int current = read();
                value |= ((long) (current & 0x7F)) << shift;
                if ((current & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift > 63) {
                    throw new IllegalArgumentException("Segment statistics contain a malformed number");
                }
            }
        }

        private long readZigZag() {
            long value = readVarUnsigned();
            return (value >>> 1) ^ -(value & 1);
        }
    }
}
