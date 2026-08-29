package net.coreprotect.database;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a block of consecutive rows into the two compressed frames a cold segment is made of.
 *
 * <p>
 * Rows are stored column by column rather than row by row. Every value in a column has the same
 * shape, so a column of timestamps or of interned identifiers compresses far better than the same
 * numbers scattered through interleaved rows. Integers are written as zig-zag varint deltas against
 * the previous value in their column, which turns the ever-increasing row ids and timestamps into a
 * run of very small numbers.
 * </p>
 *
 * <p>
 * The scalar frame holds the numeric planes and the lengths of every text and blob value; the
 * payload frame holds the text and blob bytes themselves. Splitting them means a query that only
 * needs to know which rows match never has to decompress the payloads, and it lets the payload frame
 * be compressed with a trained dictionary while the scalar frame is not.
 * </p>
 */
public final class ColdSegmentCodec {

    /** Format version written into the scalar frame. */
    public static final int CODEC_VERSION = 1;

    private static final int MAGIC_FIRST = 'C';
    private static final int MAGIC_SECOND = 'S';

    /** Column kinds, mirroring the SQLite storage classes CoreProtect uses. */
    public static final int TYPE_INTEGER = 1;
    public static final int TYPE_TEXT = 2;
    public static final int TYPE_BLOB = 3;
    public static final int TYPE_REAL = 4;

    private ColdSegmentCodec() {
        throw new IllegalStateException("Codec class");
    }

    /** The encoded form of one segment, before compression. */
    public static final class Frames {
        private final byte[] scalars;
        private final byte[] payload;

        Frames(byte[] scalars, byte[] payload) {
            this.scalars = scalars;
            this.payload = payload;
        }

        public byte[] getScalars() {
            return scalars;
        }

        public byte[] getPayload() {
            return payload;
        }
    }

    /** The decoded contents of one segment. */
    public static final class Rows {
        private final long[] rowIds;
        private final Object[][] values;

        Rows(long[] rowIds, Object[][] values) {
            this.rowIds = rowIds;
            this.values = values;
        }

        public int size() {
            return rowIds.length;
        }

        public long getRowId(int row) {
            return rowIds[row];
        }

        /**
         * @param row
         *            the row index
         * @return the column values of that row, in the column order the segment was encoded with
         */
        public Object[] getValues(int row) {
            return values[row];
        }
    }

    /**
     * Encodes a block of rows.
     *
     * @param types
     *            the column kind of each column
     * @param rowIds
     *            the row id of each row, in ascending order
     * @param rows
     *            the column values of each row
     * @return the encoded frames
     */
    public static Frames encode(int[] types, long[] rowIds, List<Object[]> rows) {
        int rowCount = rows.size();
        if (rowCount != rowIds.length) {
            throw new IllegalArgumentException("Row id count does not match the row count");
        }

        Output scalars = new Output();
        ByteArrayOutputStream payload = new ByteArrayOutputStream(Math.max(64, rowCount * 8));

        scalars.write(MAGIC_FIRST);
        scalars.write(MAGIC_SECOND);
        scalars.write(CODEC_VERSION);
        scalars.writeVarUnsigned(types.length);
        for (int type : types) {
            scalars.write(type);
        }
        scalars.writeVarUnsigned(rowCount);

        long previousRowId = 0;
        for (long rowId : rowIds) {
            scalars.writeZigZag(rowId - previousRowId);
            previousRowId = rowId;
        }

        for (int column = 0; column < types.length; column++) {
            writeNullBitmap(scalars, rows, column);
            switch (types[column]) {
                case TYPE_INTEGER:
                    writeIntegerPlane(scalars, rows, column);
                    break;
                case TYPE_REAL:
                    writeRealPlane(scalars, rows, column);
                    break;
                case TYPE_TEXT:
                case TYPE_BLOB:
                    writeBytePlane(scalars, payload, rows, column, types[column] == TYPE_TEXT);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column kind " + types[column]);
            }
        }

        return new Frames(scalars.toByteArray(), payload.toByteArray());
    }

    /**
     * Decodes a segment.
     *
     * @param scalars
     *            the decompressed scalar frame
     * @param payload
     *            the decompressed payload frame, may be empty
     * @return the decoded rows
     */
    /**
     * Decides, from the scalar values of a row, whether the row is worth building.
     *
     * <p>
     * A lookup that wants one player out of a segment of sixty five thousand rows should not pay to
     * build objects for the other sixty four thousand. The filter sees the plain numbers of a row
     * before anything is assembled, so rows that cannot match cost nothing beyond the arithmetic
     * that decoded them.
     * </p>
     */
    @FunctionalInterface
    public interface RowFilter {
        /**
         * @param columns
         *            the numeric value of each column, meaningless for text and blob columns
         * @param present
         *            whether each column holds a value
         * @return true if the row should be built
         */
        boolean accept(long[] columns, boolean[] present);
    }

    public static Rows decode(byte[] scalars, byte[] payload) {
        return decode(scalars, payload, null);
    }

    /**
     * Decodes a segment, keeping only the rows a filter accepts.
     *
     * @param scalars
     *            the decompressed scalar frame
     * @param payload
     *            the decompressed payload frame, may be empty
     * @param filter
     *            decides which rows to build, or null to build all of them
     * @return the decoded rows
     */
    /**
     * Counts the rows a filter accepts, without building any of them.
     *
     * @param scalars
     *            the decompressed scalar frame
     * @param payload
     *            the decompressed payload frame, may be empty
     * @param filter
     *            decides which rows count, or null to count them all
     * @return the number of accepted rows
     */
    public static int count(byte[] scalars, byte[] payload, RowFilter filter) {
        return decode(scalars, payload, filter, true).size();
    }

    public static Rows decode(byte[] scalars, byte[] payload, RowFilter filter) {
        return decode(scalars, payload, filter, false);
    }

    private static Rows decode(byte[] scalars, byte[] payload, RowFilter filter, boolean countOnly) {
        Input input = new Input(scalars);
        if (input.read() != MAGIC_FIRST || input.read() != MAGIC_SECOND) {
            throw new IllegalArgumentException("Segment does not use the CoreProtect segment format");
        }
        int version = input.read();
        if (version != CODEC_VERSION) {
            throw new IllegalArgumentException("Unsupported segment format version " + version);
        }

        int columnCount = (int) input.readVarUnsigned();
        int[] types = new int[columnCount];
        for (int column = 0; column < columnCount; column++) {
            types[column] = input.read();
        }

        int rowCount = (int) input.readVarUnsigned();
        long[] rowIds = new long[rowCount];
        long previousRowId = 0;
        for (int row = 0; row < rowCount; row++) {
            previousRowId = previousRowId + input.readZigZag();
            rowIds[row] = previousRowId;
        }

        // Columns are decoded into plain arrays first. Nothing is turned into an object until the
        // filter has had a chance to reject the row, which is what keeps a selective lookup cheap.
        long[][] planes = new long[columnCount][];
        boolean[][] present = new boolean[columnCount][];
        int[][] payloadOffsets = new int[columnCount][];
        int[][] payloadLengths = new int[columnCount][];

        int payloadOffset = 0;
        for (int column = 0; column < columnCount; column++) {
            present[column] = readNullBitmap(input, rowCount);
            switch (types[column]) {
                case TYPE_INTEGER:
                case TYPE_REAL: {
                    long[] plane = new long[rowCount];
                    long previous = 0;
                    for (int row = 0; row < rowCount; row++) {
                        if (!present[column][row]) {
                            continue;
                        }
                        if (types[column] == TYPE_INTEGER) {
                            previous = previous + input.readZigZag();
                            plane[row] = previous;
                        }
                        else {
                            plane[row] = input.readFixed64();
                        }
                    }
                    planes[column] = plane;
                    break;
                }
                case TYPE_TEXT:
                case TYPE_BLOB: {
                    int[] offsets = new int[rowCount];
                    int[] lengths = new int[rowCount];
                    for (int row = 0; row < rowCount; row++) {
                        if (!present[column][row]) {
                            continue;
                        }
                        int length = (int) input.readVarUnsigned();
                        if (payloadOffset + length > payload.length) {
                            throw new IllegalArgumentException("Segment payload frame is shorter than the scalar frame describes");
                        }
                        offsets[row] = payloadOffset;
                        lengths[row] = length;
                        payloadOffset = payloadOffset + length;
                    }
                    payloadOffsets[column] = offsets;
                    payloadLengths[column] = lengths;
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unsupported column kind " + types[column]);
            }
        }

        input.requireEnd();

        long[] rowValues = new long[columnCount];
        boolean[] rowPresent = new boolean[columnCount];
        int kept = 0;
        boolean[] accepted = new boolean[rowCount];
        for (int row = 0; row < rowCount; row++) {
            if (filter != null) {
                for (int column = 0; column < columnCount; column++) {
                    rowPresent[column] = present[column][row];
                    rowValues[column] = planes[column] == null ? 0 : planes[column][row];
                }
                if (!filter.accept(rowValues, rowPresent)) {
                    continue;
                }
            }
            accepted[row] = true;
            kept++;
        }

        if (countOnly) {
            return new Rows(new long[kept], new Object[kept][]);
        }

        long[] keptRowIds = new long[kept];
        Object[][] values = new Object[kept][];
        int index = 0;
        for (int row = 0; row < rowCount; row++) {
            if (!accepted[row]) {
                continue;
            }

            Object[] built = new Object[columnCount];
            for (int column = 0; column < columnCount; column++) {
                if (!present[column][row]) {
                    continue;
                }
                switch (types[column]) {
                    case TYPE_INTEGER:
                        built[column] = Long.valueOf(planes[column][row]);
                        break;
                    case TYPE_REAL:
                        built[column] = Double.valueOf(Double.longBitsToDouble(planes[column][row]));
                        break;
                    default: {
                        byte[] bytes = new byte[payloadLengths[column][row]];
                        System.arraycopy(payload, payloadOffsets[column][row], bytes, 0, bytes.length);
                        built[column] = types[column] == TYPE_TEXT ? new String(bytes, StandardCharsets.UTF_8) : bytes;
                        break;
                    }
                }
            }

            keptRowIds[index] = rowIds[row];
            values[index] = built;
            index++;
        }

        return new Rows(keptRowIds, values);
    }

    private static void writeNullBitmap(Output output, List<Object[]> rows, int column) {
        int rowCount = rows.size();
        byte[] bitmap = new byte[(rowCount + 7) / 8];
        for (int row = 0; row < rowCount; row++) {
            if (rows.get(row)[column] != null) {
                bitmap[row >>> 3] |= (byte) (1 << (row & 7));
            }
        }
        output.write(bitmap);
    }

    private static boolean[] readNullBitmap(Input input, int rowCount) {
        boolean[] present = new boolean[rowCount];
        byte[] bitmap = input.read((rowCount + 7) / 8);
        for (int row = 0; row < rowCount; row++) {
            present[row] = (bitmap[row >>> 3] & (1 << (row & 7))) != 0;
        }
        return present;
    }

    private static void writeIntegerPlane(Output output, List<Object[]> rows, int column) {
        long previous = 0;
        for (Object[] row : rows) {
            Object value = row[column];
            if (value == null) {
                continue;
            }
            long current = ((Number) value).longValue();
            output.writeZigZag(current - previous);
            previous = current;
        }
    }

    private static void writeRealPlane(Output output, List<Object[]> rows, int column) {
        for (Object[] row : rows) {
            Object value = row[column];
            if (value != null) {
                output.writeFixed64(Double.doubleToLongBits(((Number) value).doubleValue()));
            }
        }
    }

    private static void writeBytePlane(Output output, ByteArrayOutputStream payload, List<Object[]> rows, int column, boolean text) {
        for (Object[] row : rows) {
            Object value = row[column];
            if (value == null) {
                continue;
            }
            byte[] bytes = text ? ((String) value).getBytes(StandardCharsets.UTF_8) : (byte[]) value;
            output.writeVarUnsigned(bytes.length);
            payload.write(bytes, 0, bytes.length);
        }
    }

    /** Minimal growable byte sink with the varint helpers the planes are written with. */
    private static final class Output {
        private byte[] buffer = new byte[1024];
        private int length;

        private void ensure(int extra) {
            if (length + extra <= buffer.length) {
                return;
            }
            int capacity = buffer.length;
            while (capacity < length + extra) {
                capacity = capacity * 2;
            }
            byte[] grown = new byte[capacity];
            System.arraycopy(buffer, 0, grown, 0, length);
            buffer = grown;
        }

        void write(int value) {
            ensure(1);
            buffer[length++] = (byte) value;
        }

        void write(byte[] bytes) {
            ensure(bytes.length);
            System.arraycopy(bytes, 0, buffer, length, bytes.length);
            length = length + bytes.length;
        }

        void writeVarUnsigned(long value) {
            long remaining = value;
            while ((remaining & ~0x7FL) != 0) {
                write((int) ((remaining & 0x7F) | 0x80));
                remaining >>>= 7;
            }
            write((int) remaining);
        }

        void writeZigZag(long value) {
            writeVarUnsigned((value << 1) ^ (value >> 63));
        }

        void writeFixed64(long value) {
            for (int shift = 0; shift < 64; shift += 8) {
                write((int) ((value >>> shift) & 0xFF));
            }
        }

        byte[] toByteArray() {
            byte[] result = new byte[length];
            System.arraycopy(buffer, 0, result, 0, length);
            return result;
        }
    }

    /** Reader for the format {@link Output} writes. */
    private static final class Input {
        private final byte[] data;
        private int offset;

        private Input(byte[] data) {
            this.data = data;
        }

        int read() {
            if (offset >= data.length) {
                throw new IllegalArgumentException("Segment frame ended unexpectedly");
            }
            return Byte.toUnsignedInt(data[offset++]);
        }

        byte[] read(int count) {
            if (offset + count > data.length) {
                throw new IllegalArgumentException("Segment frame ended unexpectedly");
            }
            byte[] bytes = new byte[count];
            System.arraycopy(data, offset, bytes, 0, count);
            offset = offset + count;
            return bytes;
        }

        long readVarUnsigned() {
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
                    throw new IllegalArgumentException("Segment frame contains a malformed number");
                }
            }
        }

        long readZigZag() {
            long value = readVarUnsigned();
            return (value >>> 1) ^ -(value & 1);
        }

        long readFixed64() {
            long value = 0;
            for (int shift = 0; shift < 64; shift += 8) {
                value |= ((long) read()) << shift;
            }
            return value;
        }

        void requireEnd() {
            if (offset != data.length) {
                throw new IllegalArgumentException("Segment frame has " + (data.length - offset) + " unread bytes");
            }
        }
    }

    /**
     * @param types
     *            the column kinds of a table
     * @return true if the table has any text or blob column, and therefore a payload frame
     */
    public static boolean hasPayload(int[] types) {
        for (int type : types) {
            if (type == TYPE_TEXT || type == TYPE_BLOB) {
                return true;
            }
        }
        return false;
    }

    /**
     * Splits a payload frame back into the individual values it holds, which is what a dictionary
     * trainer needs as samples.
     *
     * @param scalars
     *            the decompressed scalar frame
     * @param payload
     *            the decompressed payload frame
     * @return the individual payload values
     */
    public static List<byte[]> payloadSamples(byte[] scalars, byte[] payload) {
        Rows rows = decode(scalars, payload);
        List<byte[]> samples = new ArrayList<>();
        for (int row = 0; row < rows.size(); row++) {
            for (Object value : rows.getValues(row)) {
                if (value instanceof byte[]) {
                    samples.add((byte[]) value);
                }
                else if (value instanceof String) {
                    samples.add(((String) value).getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        return samples;
    }
}
