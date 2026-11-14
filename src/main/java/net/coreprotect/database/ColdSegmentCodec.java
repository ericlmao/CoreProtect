package net.coreprotect.database;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.luben.zstd.Zstd;

import net.coreprotect.utility.serialize.BlobCompression;

/**
 * Encodes a block of consecutive rows into the two frames a segment is made of.
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
 *
 * <p>
 * Since version 2 the scalar frame is divided into groups of {@link #GROUP_ROWS} rows. Each group
 * is compressed on its own and the frame begins with a directory recording, for every group, the
 * smallest and largest value of each integer column. A lookup that asks for one place, one player or
 * one hour tests those ranges first and decompresses and decodes only the groups that can hold what
 * it wants. Decoding is the expensive part of reading a segment, so a lookup restricted to a few
 * blocks reads a fraction of each segment instead of all of it. The frame is self-contained and is
 * stored as it is, without being compressed again as a whole. Version 1 frames are still read.
 * </p>
 *
 * <p>
 * Since version 3 each group also lists the chunks its rows lie in, with how many rows lie in each.
 * Rows are ordered along a curve through the world, but a range of x and a range of z still spans
 * far more ground than a group of rows along that curve really covers, so a lookup about one place
 * would otherwise decode groups that only surround it. The list answers exactly, and a lookup
 * decodes only the groups that hold rows in the chunks it asks about. Version 2 frames are still
 * read, and simply have no lists.
 * </p>
 *
 * <p>
 * Since version 4 the unit bodies are no longer part of the scalar frame. The frame holds the
 * header and the directory alone, a few kilobytes, and each unit body is stored as a row of its own
 * beside the segment. A lookup about one place reads the directory, works out from it which units
 * hold groups it can match, and reads only those, where it used to read the whole frame from the
 * database to use one or two of its units. The directory is the version 3 directory unchanged, so
 * a version 3 frame is exactly a version 4 directory followed by its unit bodies, and the codec can
 * turn one into the other without decoding anything.
 * </p>
 */
public final class ColdSegmentCodec {

    /** Format version written into the scalar frame. */
    public static final int CODEC_VERSION = 4;

    /** The format whose unit bodies are stored apart from the directory, which is the current one. */
    static final int VERSION_SPLIT = 4;

    /** The grouped format with chunk lists and its unit bodies in line, still read. */
    static final int VERSION_CHUNKS = 3;

    /** The first format, one delta coded body for the whole segment, still read. */
    static final int VERSION_ONE = 1;

    /** The first format divided into groups, without the chunks of each group, still read. */
    static final int VERSION_GROUPS = 2;

    /**
     * The most chunks a group lists. A group of rows ordered by place covers a handful of chunks;
     * one of chat, commands or sessions, ordered by player, can cover a chunk per row, and a list
     * that long would cost more to store and test than decoding the group does. Such a group lists
     * nothing and is treated as able to hold any chunk.
     */
    static final int GROUP_CHUNK_LIMIT = 64;

    /** Format of the stored chunk counts of a whole segment, written as their first byte. */
    private static final int CHUNK_COUNTS_FORMAT = 1;

    /**
     * Rows per group, the unit a lookup decodes. A group of 1,024 rows lets a lookup about one block
     * decode a sixty-fourth of a full segment rather than all of it.
     */
    static final int GROUP_ROWS = 1024;

    /**
     * Groups compressed together. Compressing each group on its own loses the repetition between
     * neighbouring groups, and decompressing is cheap next to decoding, so groups are decoded one at
     * a time but compressed several at a time, with each column laid out across all of them. Eight
     * was measured on production segments: the space is within a few percent of compressing a whole
     * segment at once, and a lookup about one place still costs a fraction of a millisecond per
     * segment.
     */
    static final int UNIT_GROUPS = 8;

    private static final int MAGIC_FIRST = 'C';
    private static final int MAGIC_SECOND = 'S';

    /** How a group's body is stored. */
    private static final int BODY_PLAIN = 0;
    private static final int BODY_ZSTD = 1;

    /** Column kinds, mirroring the SQLite storage classes CoreProtect uses. */
    public static final int TYPE_INTEGER = 1;
    public static final int TYPE_TEXT = 2;
    public static final int TYPE_BLOB = 3;
    public static final int TYPE_REAL = 4;

    private ColdSegmentCodec() {
        throw new IllegalStateException("Codec class");
    }

    /**
     * The encoded form of one segment.
     *
     * <p>
     * A segment of the current format is a directory, the bodies of its units, and a payload frame.
     * The directory and the bodies are stored apart, so that a lookup can read the few units it
     * needs. {@link #getScalars()} still offers them joined into one self-contained frame, for code
     * that decodes a whole segment held in memory.
     * </p>
     */
    public static final class Frames {
        private final byte[] directory;
        private final byte[][] units;
        private final byte[] payload;
        private final byte[] chunkCounts;
        private byte[] scalars;

        Frames(byte[] scalars, byte[] payload) {
            this.directory = null;
            this.units = null;
            this.scalars = scalars;
            this.payload = payload;
            this.chunkCounts = null;
        }

        Frames(byte[] directory, byte[][] units, byte[] payload, byte[] chunkCounts) {
            this.directory = directory;
            this.units = units;
            this.payload = payload;
            this.chunkCounts = chunkCounts;
        }

        /**
         * @return how many rows of the segment lie in each chunk, in the form stored beside the
         *         segment, or null for a frame of a format that does not record them
         */
        public byte[] getChunkCounts() {
            return chunkCounts;
        }

        /**
         * @return the whole scalar frame in one piece, with the unit bodies in line after the
         *         directory as version 3 stored it, ready to decode on its own
         */
        public synchronized byte[] getScalars() {
            if (scalars == null) {
                scalars = inline(directory, units);
            }
            return scalars;
        }

        /**
         * @return the header and directory of the scalar frame, without the unit bodies, as it is
         *         stored in the segment row; null for a frame of the first format, which has no units
         */
        public byte[] getDirectory() {
            return directory;
        }

        /**
         * @return the stored body of each unit, by unit number; empty for a frame of the first format
         */
        public byte[][] getUnits() {
            return units == null ? new byte[0][] : units;
        }

        /**
         * @return the payload frame, or null for frames split from a stored scalar frame, whose
         *         payload is stored apart and left as it is
         */
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
     * The columns a table has now, for reading segments packed before some of them were added.
     *
     * <p>
     * A segment records the columns its table had when it was packed, and a column added to the
     * table since then, which SQLite always appends, is missing from it. Read against the table's
     * columns, such a segment shows each missing column holding the table's default for it, which
     * is the value SQLite gives every row that was already in the table when the column was added.
     * Filters, ranges and decoded rows then all see the table's columns, whichever build packed the
     * segment. Nothing stored changes.
     * </p>
     */
    public static final class Columns {
        /** Stands for a default the codec cannot show, such as a text value or an expression. */
        public static final Object UNSUPPORTED_DEFAULT = new Object();

        private final int[] types;
        private final Object[] defaults;

        /**
         * @param types
         *            the column kind of each of the table's columns
         * @param defaults
         *            each column's default: null, a {@link Long} for an integer column, a
         *            {@link Double} for a real one, or {@link #UNSUPPORTED_DEFAULT}
         */
        public Columns(int[] types, Object[] defaults) {
            if (types.length != defaults.length) {
                throw new IllegalArgumentException("Every column has one default");
            }
            for (int column = 0; column < types.length; column++) {
                Object value = defaults[column];
                boolean valid = value == null || value == UNSUPPORTED_DEFAULT || (types[column] == TYPE_INTEGER && value instanceof Long)
                        || (types[column] == TYPE_REAL && value instanceof Double);
                if (!valid) {
                    throw new IllegalArgumentException("Column " + column + " has a default of the wrong kind");
                }
            }
            this.types = types.clone();
            this.defaults = defaults.clone();
        }

        /**
         * @return how many columns the table has
         */
        public int size() {
            return types.length;
        }
    }

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
         *            the numeric value of each column, or for a text or blob column the length of
         *            its value in bytes
         * @param present
         *            whether each column holds a value
         * @return true if the row should be built
         */
        boolean accept(long[] columns, boolean[] present);

        /**
         * Decides whether a row is worth building when its row id matters too. A page confined to
         * a range of row ids, or to positions in an order that breaks ties by row id, has to see
         * the id; every other filter only looks at the values and needs nothing more.
         *
         * @param rowId
         *            the row's id
         * @param columns
         *            the numeric value of each column
         * @param present
         *            whether each column holds a value
         * @return true if the row should be built
         */
        default boolean acceptRow(long rowId, long[] columns, boolean[] present) {
            return accept(columns, present);
        }

        /**
         * Decides from the range of values a group of rows holds whether any of them could be
         * accepted, so a group that cannot hold a match is never decompressed or decoded.
         *
         * @param minimum
         *            the smallest value of each column in the group
         * @param maximum
         *            the largest value of each column in the group
         * @param known
         *            whether the range of a column is known; it is only known for integer columns
         *            that hold a value in every row of the group
         * @return false only if no row of the group can be accepted
         */
        default boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
            return true;
        }

        /**
         * Decides from the chunks a group of rows lies in whether any of them could be accepted,
         * before the group is decompressed or decoded. Asked only of groups that list their chunks;
         * a group without a list may hold any chunk and is never ruled out this way.
         *
         * @param chunks
         *            every chunk the group has rows in, and how many rows lack a place altogether
         * @return false only if no row of the group can be accepted
         */
        default boolean mayMatchChunks(ChunkList chunks) {
            return true;
        }
    }

    /**
     * The chunks a run of rows lies in, each with how many of the rows lie there, ordered by world,
     * then chunk x, then chunk z.
     *
     * <p>
     * A row lies in a chunk only when it has a world and both coordinates. Rows that lack any of
     * them are counted apart, as unlocated, and a group holding one is never ruled out by its
     * chunks: whatever a lookup does with a row that has no place, it is not the chunk list's place
     * to decide.
     * </p>
     */
    public static final class ChunkList {
        private final int[] worldIds;
        private final int[] chunkXs;
        private final int[] chunkZs;
        private final int[] rows;
        private final int unlocatedRows;

        ChunkList(int[] worldIds, int[] chunkXs, int[] chunkZs, int[] rows, int unlocatedRows) {
            this.worldIds = worldIds;
            this.chunkXs = chunkXs;
            this.chunkZs = chunkZs;
            this.rows = rows;
            this.unlocatedRows = unlocatedRows;
        }

        /**
         * @return how many distinct chunks are listed
         */
        public int size() {
            return worldIds.length;
        }

        public int worldId(int index) {
            return worldIds[index];
        }

        public int chunkX(int index) {
            return chunkXs[index];
        }

        public int chunkZ(int index) {
            return chunkZs[index];
        }

        /**
         * @return how many rows lie in the chunk at that index
         */
        public int rows(int index) {
            return rows[index];
        }

        /**
         * @return how many rows lack a world or a coordinate, and so lie in no chunk
         */
        public int unlocatedRows() {
            return unlocatedRows;
        }

        /**
         * @return the key a segment's chunk filter stores for the chunk at that index
         */
        public long chunkKey(int index) {
            return SegmentFilter.chunkKey(worldIds[index], chunkXs[index], chunkZs[index]);
        }
    }

    /**
     * Tallies the chunks of a run of rows as they are walked.
     *
     * <p>
     * Every distinct chunk is kept until there are more than the limit, after which the tally only
     * remembers that it overflowed. Stopping part way would list the chunks seen so far as if they
     * were all of them, and a lookup trusting that list would skip rows it wants.
     * </p>
     */
    static final class ChunkTally {
        private final int limit;
        private final Map<Chunk, int[]> counts = new HashMap<>();
        private final Chunk probe = new Chunk();
        private int unlocated;
        private boolean overflowed;

        /**
         * @param limit
         *            the most distinct chunks worth listing
         */
        ChunkTally(int limit) {
            this.limit = limit;
        }

        /**
         * Counts one row from the values of its world and block coordinate columns.
         *
         * @param world
         *            the row's world id, or null
         * @param x
         *            the row's x coordinate, or null
         * @param z
         *            the row's z coordinate, or null
         */
        void add(Object world, Object x, Object z) {
            if (world == null || x == null || z == null) {
                unlocated++;
                return;
            }
            add(true, ((Number) world).longValue(), ((Number) x).longValue(), ((Number) z).longValue());
        }

        /**
         * Counts one row from the plain numbers of its world and block coordinate columns.
         *
         * @param located
         *            whether the row has a world and both coordinates
         */
        void add(boolean located, long world, long x, long z) {
            // Coordinates and world ids are written as integers. A value outside that range cannot
            // be told apart from another once narrowed, so the row is counted as having no place
            // rather than in a chunk it may not lie in.
            if (!located || world != (int) world || x != (int) x || z != (int) z) {
                unlocated++;
                return;
            }
            if (overflowed) {
                return;
            }
            probe.set((int) world, SegmentFilter.chunkOf((int) x), SegmentFilter.chunkOf((int) z));
            int[] count = counts.get(probe);
            if (count != null) {
                count[0]++;
                return;
            }
            if (counts.size() >= limit) {
                overflowed = true;
                counts.clear();
                return;
            }
            counts.put(probe.copy(), new int[] { 1 });
        }

        /**
         * @return the tallied chunks in order, or null when there were more than the limit
         */
        ChunkList toList() {
            if (overflowed) {
                return null;
            }
            List<Map.Entry<Chunk, int[]>> entries = new ArrayList<>(counts.entrySet());
            entries.sort((first, second) -> first.getKey().compareTo(second.getKey()));
            int size = entries.size();
            int[] worldIds = new int[size];
            int[] chunkXs = new int[size];
            int[] chunkZs = new int[size];
            int[] rows = new int[size];
            for (int index = 0; index < size; index++) {
                Chunk chunk = entries.get(index).getKey();
                worldIds[index] = chunk.world;
                chunkXs[index] = chunk.x;
                chunkZs[index] = chunk.z;
                rows[index] = entries.get(index).getValue()[0];
            }
            return new ChunkList(worldIds, chunkXs, chunkZs, rows, unlocated);
        }
    }

    /** A chunk of one world, as a key while tallying. */
    private static final class Chunk implements Comparable<Chunk> {
        private int world;
        private int x;
        private int z;

        void set(int world, int x, int z) {
            this.world = world;
            this.x = x;
            this.z = z;
        }

        Chunk copy() {
            Chunk copy = new Chunk();
            copy.set(world, x, z);
            return copy;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Chunk)) {
                return false;
            }
            Chunk chunk = (Chunk) other;
            return chunk.world == world && chunk.x == x && chunk.z == z;
        }

        @Override
        public int hashCode() {
            return (world * 31 + x) * 1_000_003 + z;
        }

        @Override
        public int compareTo(Chunk other) {
            int byWorld = Integer.compare(world, other.world);
            if (byWorld != 0) {
                return byWorld;
            }
            int byX = Integer.compare(x, other.x);
            return byX != 0 ? byX : Integer.compare(z, other.z);
        }
    }

    /**
     * Supplies the decompressed payload frame of a segment, only once a row that needs it is built.
     *
     * <p>
     * The payload frame holds every item and block blob, and is usually most of a segment. A lookup
     * that keeps no rows of a segment, or keeps rows whose text and blob columns are all empty,
     * never has to decompress it at all.
     * </p>
     */
    @FunctionalInterface
    public interface Payload {
        /**
         * @return the decompressed payload frame, never null
         * @throws IllegalStateException
         *             if the frame cannot be read
         */
        byte[] get();
    }

    /**
     * Encodes a block of rows in the current format.
     *
     * @param types
     *            the column kind of each column
     * @param rowIds
     *            the row id of each row
     * @param rows
     *            the column values of each row
     * @return the encoded frames
     */
    public static Frames encode(int[] types, long[] rowIds, List<Object[]> rows) {
        return encode(types, rowIds, rows, GROUP_ROWS);
    }

    /**
     * Encodes a block of rows in the current format, recording which chunks each group of rows and
     * the whole segment lie in.
     *
     * @param types
     *            the column kind of each column
     * @param rowIds
     *            the row id of each row
     * @param rows
     *            the column values of each row
     * @param location
     *            the indexes of the world, x and z columns, or null when the rows have no place
     * @return the encoded frames
     */
    public static Frames encode(int[] types, long[] rowIds, List<Object[]> rows, int[] location) {
        return encode(types, rowIds, rows, GROUP_ROWS, UNIT_GROUPS, location);
    }

    /**
     * Encodes a block of rows in the current format, with a chosen group size.
     *
     * @param types
     *            the column kind of each column
     * @param rowIds
     *            the row id of each row
     * @param rows
     *            the column values of each row
     * @param groupRows
     *            rows per group
     * @return the encoded frames
     */
    static Frames encode(int[] types, long[] rowIds, List<Object[]> rows, int groupRows) {
        return encode(types, rowIds, rows, groupRows, UNIT_GROUPS);
    }

    /**
     * Encodes a block of rows in the current format, with a chosen group size and number of groups
     * compressed together.
     */
    static Frames encode(int[] types, long[] rowIds, List<Object[]> rows, int groupRows, int unitGroupCount) {
        return encode(types, rowIds, rows, groupRows, unitGroupCount, null);
    }

    /**
     * Encodes a block of rows in the current format, with a chosen group size, number of groups
     * compressed together, and the columns that place a row in a chunk.
     */
    static Frames encode(int[] types, long[] rowIds, List<Object[]> rows, int groupRows, int unitGroupCount, int[] location) {
        int rowCount = rows.size();
        if (rowCount != rowIds.length) {
            throw new IllegalArgumentException("Row id count does not match the row count");
        }
        if (groupRows <= 0) {
            throw new IllegalArgumentException("Groups must hold at least one row");
        }
        checkTypes(types);
        if (location != null && location.length != 3) {
            throw new IllegalArgumentException("A location is a world, an x and a z column");
        }

        Output header = new Output();
        header.write(MAGIC_FIRST);
        header.write(MAGIC_SECOND);
        header.write(CODEC_VERSION);
        header.writeVarUnsigned(types.length);
        for (int type : types) {
            header.write(type);
        }
        header.writeVarUnsigned(rowCount);
        int groupCount = rowCount == 0 ? 0 : (rowCount + groupRows - 1) / groupRows;
        int unitGroups = Math.max(1, unitGroupCount);
        int unitCount = (groupCount + unitGroups - 1) / unitGroups;
        header.writeVarUnsigned(groupCount);
        header.writeVarUnsigned(unitCount);
        header.writeVarUnsigned(unitGroups);

        ByteArrayOutputStream payload = new ByteArrayOutputStream(Math.max(64, rowCount * 8));
        Output groups = new Output();
        List<byte[]> units = new ArrayList<>(unitCount);
        boolean compress = BlobCompression.isAvailable();
        int level = BlobCompression.storageLevel();

        for (int unit = 0; unit < unitCount; unit++) {
            int firstGroup = unit * unitGroups;
            int lastGroup = Math.min(groupCount, (unit + 1) * unitGroups);
            int unitSize = lastGroup - firstGroup;

            // Laid out column by column across the whole unit, as a segment of the first format is:
            // all of the unit's row ids, then all of its times, and so on. Compression finds far more
            // in a long run of one column than in columns taking turns every group. Each group's
            // share of each column is written as a chunk of its own, so one group can still be
            // decoded without the others.
            int[][] chunkLengths = new int[unitSize][types.length + 1];
            int[][] payloadStarts = new int[unitSize][types.length];
            Output unitBody = new Output();
            for (int chunk = 0; chunk <= types.length; chunk++) {
                for (int group = firstGroup; group < lastGroup; group++) {
                    int from = group * groupRows;
                    int to = Math.min(rowCount, from + groupRows);
                    int start = unitBody.length;
                    if (chunk == 0) {
                        writeRowIds(unitBody, rowIds, from, to);
                    }
                    else {
                        int column = chunk - 1;
                        payloadStarts[group - firstGroup][column] = payload.size();
                        writeColumn(unitBody, payload, types[column], rows.subList(from, to), column);
                    }
                    chunkLengths[group - firstGroup][chunk] = unitBody.length - start;
                }
            }

            for (int group = firstGroup; group < lastGroup; group++) {
                int from = group * groupRows;
                int to = Math.min(rowCount, from + groupRows);
                groups.writeVarUnsigned(to - from);
                for (int chunk = 0; chunk <= types.length; chunk++) {
                    groups.writeVarUnsigned(chunkLengths[group - firstGroup][chunk]);
                }
                for (int column = 0; column < types.length; column++) {
                    if (types[column] == TYPE_TEXT || types[column] == TYPE_BLOB) {
                        groups.writeVarUnsigned(payloadStarts[group - firstGroup][column]);
                    }
                }
                writeZones(groups, types, rows.subList(from, to));
                writeGroupChunks(groups, rows, from, to, location);
            }

            byte[] raw = unitBody.toByteArray();
            byte[] stored = raw;
            int kind = BODY_PLAIN;
            if (compress) {
                byte[] packed = Zstd.compress(raw, level);
                if (packed.length < raw.length) {
                    stored = packed;
                    kind = BODY_ZSTD;
                }
            }
            units.add(stored);
            header.write(kind);
            header.writeVarUnsigned(raw.length);
            header.writeVarUnsigned(stored.length);
        }

        header.write(groups.toByteArray());
        return new Frames(header.toByteArray(), units.toArray(new byte[0][]), payload.toByteArray(), chunkCounts(rows, location));
    }

    /**
     * Turns the stored text and blob bytes of one unit into what is kept for it, when a segment keeps
     * each unit's share of the payload apart. Compaction compresses each share on its own as it is
     * made, so a segment of a million rows never holds its whole payload uncompressed at once.
     */
    @FunctionalInterface
    interface PayloadPacker {
        /**
         * @param raw
         *            the text and blob bytes of one unit, possibly empty
         * @return what is stored for them
         * @throws java.sql.SQLException
         *             if they cannot be packed
         */
        byte[] pack(byte[] raw) throws java.sql.SQLException;
    }

    /**
     * Builds a segment of the current format a group of rows at a time.
     *
     * <p>
     * {@link #encode} is handed every row of a segment at once, which is right for a segment of
     * sixty five thousand rows read out of the live table. Compaction writes segments of up to a
     * million rows, merged out of many others, and holding all of them as decoded rows would take
     * gigabytes. This encoder holds one unit's groups while it lays them out column by column, and
     * keeps only the encoded bytes after that. Given the same rows in the same groups it writes
     * exactly what {@link #encode} writes.
     * </p>
     *
     * <p>
     * The text and blob bytes of each unit are kept apart, one share per unit in the order the unit
     * numbers run, and joined together they are exactly the payload frame {@link #encode} writes.
     * The offsets in the directory are into that joined frame either way, so a reader holding only
     * some of the shares finds a value by which share its offset falls in.
     * </p>
     */
    static final class Encoder {
        private final int[] types;
        private final int[] location;
        private final PayloadPacker packer;
        private final boolean compress = BlobCompression.isAvailable();
        private final int level = BlobCompression.storageLevel();
        private final Output groupDirectory = new Output();
        private final Output unitTable = new Output();
        private final List<byte[]> units = new ArrayList<>();
        private final List<byte[]> payloads = new ArrayList<>();
        private final ChunkTally chunks = new ChunkTally(Integer.MAX_VALUE);
        private final List<PendingGroup> pending = new ArrayList<>(UNIT_GROUPS);
        private long payloadSize;
        private int rowCount;
        private int groupCount;
        private boolean closed;

        /** One group encoded column by column, waiting for the rest of its unit. */
        private static final class PendingGroup {
            private final int rows;
            private final byte[][] chunks;
            private final byte[][] payloads;
            private final byte[] tail;

            private PendingGroup(int rows, byte[][] chunks, byte[][] payloads, byte[] tail) {
                this.rows = rows;
                this.chunks = chunks;
                this.payloads = payloads;
                this.tail = tail;
            }
        }

        /**
         * @param types
         *            the column kind of each column
         * @param location
         *            the indexes of the world, x and z columns, or null when the rows have no place
         * @param packer
         *            what to store for each unit's text and blob bytes, or null to keep them as they
         *            are
         */
        Encoder(int[] types, int[] location, PayloadPacker packer) {
            checkTypes(types);
            if (location != null && location.length != 3) {
                throw new IllegalArgumentException("A location is a world, an x and a z column");
            }
            this.types = types.clone();
            this.location = location;
            this.packer = packer;
        }

        /**
         * @return how many rows have been added
         */
        int rowCount() {
            return rowCount;
        }

        /**
         * Adds one group of rows. Every group but the last must hold exactly {@link #GROUP_ROWS}
         * rows, as the groups {@link #encode} makes do.
         *
         * @param rowIds
         *            the row ids, of which the first {@code count} are used
         * @param rows
         *            the rows, of which the first {@code count} are used
         * @param count
         *            how many rows the group holds, between one and {@link #GROUP_ROWS}
         * @throws java.sql.SQLException
         *             if a finished unit's payload cannot be packed
         */
        void addGroup(long[] rowIds, List<Object[]> rows, int count) throws java.sql.SQLException {
            if (closed) {
                throw new IllegalStateException("A group was added after the last short one or after the segment was finished");
            }
            if (count <= 0 || count > GROUP_ROWS || count > rowIds.length || count > rows.size()) {
                throw new IllegalArgumentException("A group holds between one and " + GROUP_ROWS + " rows");
            }
            List<Object[]> group = rows.size() == count ? rows : rows.subList(0, count);
            byte[][] chunkBytes = new byte[types.length + 1][];
            byte[][] payloadBytes = new byte[types.length][];
            Output rowIdChunk = new Output();
            writeRowIds(rowIdChunk, rowIds, 0, count);
            chunkBytes[0] = rowIdChunk.toByteArray();
            for (int column = 0; column < types.length; column++) {
                Output chunk = new Output();
                ByteArrayOutputStream values = new ByteArrayOutputStream();
                writeColumn(chunk, values, types[column], group, column);
                chunkBytes[column + 1] = chunk.toByteArray();
                payloadBytes[column] = values.toByteArray();
            }
            Output tail = new Output();
            writeZones(tail, types, group);
            writeGroupChunks(tail, group, 0, count, location);
            if (location != null) {
                for (Object[] row : group) {
                    chunks.add(row[location[0]], row[location[1]], row[location[2]]);
                }
            }

            pending.add(new PendingGroup(count, chunkBytes, payloadBytes, tail.toByteArray()));
            rowCount = rowCount + count;
            groupCount++;
            closed = count < GROUP_ROWS;
            if (pending.size() == UNIT_GROUPS) {
                flushUnit();
            }
        }

        /**
         * Lays the groups of one unit out column by column across the unit, as {@link #encode}
         * does, and lets go of their encoded columns.
         */
        private void flushUnit() throws java.sql.SQLException {
            if (pending.isEmpty()) {
                return;
            }
            Output body = new Output();
            int[][] chunkLengths = new int[pending.size()][types.length + 1];
            for (int chunk = 0; chunk <= types.length; chunk++) {
                for (int group = 0; group < pending.size(); group++) {
                    byte[] bytes = pending.get(group).chunks[chunk];
                    body.write(bytes);
                    chunkLengths[group][chunk] = bytes.length;
                }
            }

            ByteArrayOutputStream unitPayload = new ByteArrayOutputStream();
            int[][] payloadStarts = new int[pending.size()][types.length];
            for (int column = 0; column < types.length; column++) {
                for (int group = 0; group < pending.size(); group++) {
                    payloadStarts[group][column] = Math.toIntExact(payloadSize + unitPayload.size());
                    byte[] bytes = pending.get(group).payloads[column];
                    unitPayload.write(bytes, 0, bytes.length);
                }
            }

            for (int group = 0; group < pending.size(); group++) {
                PendingGroup entry = pending.get(group);
                groupDirectory.writeVarUnsigned(entry.rows);
                for (int chunk = 0; chunk <= types.length; chunk++) {
                    groupDirectory.writeVarUnsigned(chunkLengths[group][chunk]);
                }
                for (int column = 0; column < types.length; column++) {
                    if (types[column] == TYPE_TEXT || types[column] == TYPE_BLOB) {
                        groupDirectory.writeVarUnsigned(payloadStarts[group][column]);
                    }
                }
                groupDirectory.write(entry.tail);
            }

            byte[] raw = body.toByteArray();
            byte[] stored = raw;
            int kind = BODY_PLAIN;
            if (compress) {
                byte[] packed = Zstd.compress(raw, level);
                if (packed.length < raw.length) {
                    stored = packed;
                    kind = BODY_ZSTD;
                }
            }
            units.add(stored);
            unitTable.write(kind);
            unitTable.writeVarUnsigned(raw.length);
            unitTable.writeVarUnsigned(stored.length);

            byte[] rawPayload = unitPayload.toByteArray();
            payloadSize = payloadSize + rawPayload.length;
            payloads.add(packer == null ? rawPayload : packer.pack(rawPayload));
            pending.clear();
        }

        /**
         * Finishes the segment.
         *
         * @return the directory, the stored unit bodies, what was kept of each unit's text and blob
         *         bytes, and the rows per chunk
         * @throws java.sql.SQLException
         *             if the last unit's payload cannot be packed
         */
        Encoded finish() throws java.sql.SQLException {
            flushUnit();
            closed = true;
            Output header = new Output();
            header.write(MAGIC_FIRST);
            header.write(MAGIC_SECOND);
            header.write(CODEC_VERSION);
            header.writeVarUnsigned(types.length);
            for (int type : types) {
                header.write(type);
            }
            header.writeVarUnsigned(rowCount);
            header.writeVarUnsigned(groupCount);
            header.writeVarUnsigned(units.size());
            header.writeVarUnsigned(UNIT_GROUPS);
            header.write(unitTable.toByteArray());
            header.write(groupDirectory.toByteArray());

            ChunkList chunkList = location == null ? new ChunkList(new int[0], new int[0], new int[0], new int[0], rowCount) : chunks.toList();
            return new Encoded(header.toByteArray(), units.toArray(new byte[0][]), payloads.toArray(new byte[0][]), payloadSize, encodeChunkCounts(chunkList));
        }
    }

    /** A segment built by an {@link Encoder}. */
    static final class Encoded {
        private final byte[] directory;
        private final byte[][] units;
        private final byte[][] payloads;
        private final long payloadSize;
        private final byte[] chunkCounts;

        private Encoded(byte[] directory, byte[][] units, byte[][] payloads, long payloadSize, byte[] chunkCounts) {
            this.directory = directory;
            this.units = units;
            this.payloads = payloads;
            this.payloadSize = payloadSize;
            this.chunkCounts = chunkCounts;
        }

        byte[] getDirectory() {
            return directory;
        }

        byte[][] getUnits() {
            return units;
        }

        /**
         * @return what was kept of each unit's text and blob bytes, by unit number
         */
        byte[][] getUnitPayloads() {
            return payloads;
        }

        /**
         * @return how many text and blob bytes the segment holds, before any packing
         */
        long getPayloadSize() {
            return payloadSize;
        }

        byte[] getChunkCounts() {
            return chunkCounts;
        }
    }

    /**
     * Joins a directory and its unit bodies into one self-contained frame, the form version 3
     * stored. The directory of both versions is the same, so only the version changes.
     *
     * @param directory
     *            a directory of the current format
     * @param units
     *            the stored body of every unit, by unit number
     * @return the frame with the bodies in line
     */
    static byte[] inline(byte[] directory, byte[][] units) {
        if (!isSplit(directory)) {
            throw new IllegalArgumentException("Only a directory whose units are stored apart can be joined with them");
        }
        int length = directory.length;
        for (byte[] unit : units) {
            length = length + unit.length;
        }
        byte[] frame = Arrays.copyOf(directory, length);
        frame[2] = (byte) VERSION_CHUNKS;
        int position = directory.length;
        for (byte[] unit : units) {
            System.arraycopy(unit, 0, frame, position, unit.length);
            position = position + unit.length;
        }
        return frame;
    }

    /**
     * Splits a self-contained version 3 frame into its directory and unit bodies, the form the
     * current version stores, without decoding or compressing anything. The bodies are copied as
     * they are, so the split frame decodes to exactly the rows the joined one did.
     *
     * @param scalars
     *            a version 3 scalar frame
     * @return the directory and unit bodies; the payload is stored apart and is not part of the
     *         result, so {@link Frames#getPayload()} is null
     */
    static Frames split(byte[] scalars) {
        Directory directory = readDirectory(scalars);
        if (directory.version != VERSION_CHUNKS) {
            throw new IllegalArgumentException("Only a version " + VERSION_CHUNKS + " frame can be split as it is, not version " + directory.version);
        }
        byte[][] units = new byte[directory.unitCount][];
        int position = directory.end;
        for (int unit = 0; unit < directory.unitCount; unit++) {
            int stored = directory.unitStored[unit];
            if (position + stored > scalars.length) {
                throw new IllegalArgumentException("Segment frame ended unexpectedly");
            }
            units[unit] = Arrays.copyOfRange(scalars, position, position + stored);
            position = position + stored;
        }
        if (position != scalars.length) {
            throw new IllegalArgumentException("Segment frame does not end where its units do");
        }
        byte[] head = Arrays.copyOf(scalars, directory.end);
        head[2] = (byte) VERSION_SPLIT;
        return new Frames(head, units, null, null);
    }

    /**
     * Writes the chunks a group's rows lie in, or that it lists none: when the rows have no place,
     * or lie in more chunks than are worth listing.
     */
    private static void writeGroupChunks(Output output, List<Object[]> rows, int from, int to, int[] location) {
        ChunkList chunks = location == null ? null : tally(rows, from, to, location, GROUP_CHUNK_LIMIT);
        if (chunks == null) {
            output.writeVarUnsigned(0);
            return;
        }
        output.writeVarUnsigned(chunks.size() + 1L);
        writeChunkEntries(output, chunks);
    }

    private static ChunkList tally(List<Object[]> rows, int from, int to, int[] location, int limit) {
        ChunkTally tally = new ChunkTally(limit);
        for (int row = from; row < to; row++) {
            Object[] values = rows.get(row);
            tally.add(values[location[0]], values[location[1]], values[location[2]]);
        }
        return tally.toList();
    }

    /**
     * Writes a list of chunks as differences from the entry before it, which are small because the
     * list is ordered: the same world, a neighbouring x, a nearby z.
     */
    private static void writeChunkEntries(Output output, ChunkList chunks) {
        output.writeVarUnsigned(chunks.unlocatedRows());
        int previousWorld = 0;
        int previousX = 0;
        int previousZ = 0;
        for (int index = 0; index < chunks.size(); index++) {
            output.writeZigZag((long) chunks.worldId(index) - previousWorld);
            output.writeZigZag((long) chunks.chunkX(index) - previousX);
            output.writeZigZag((long) chunks.chunkZ(index) - previousZ);
            output.writeVarUnsigned(chunks.rows(index) - 1L);
            previousWorld = chunks.worldId(index);
            previousX = chunks.chunkX(index);
            previousZ = chunks.chunkZ(index);
        }
    }

    private static ChunkList readChunkEntries(Input input, int size) {
        int unlocated = checkedCount(input.readVarUnsigned());
        int[] worldIds = new int[size];
        int[] chunkXs = new int[size];
        int[] chunkZs = new int[size];
        int[] rows = new int[size];
        long world = 0;
        long x = 0;
        long z = 0;
        for (int index = 0; index < size; index++) {
            world = world + input.readZigZag();
            x = x + input.readZigZag();
            z = z + input.readZigZag();
            worldIds[index] = (int) world;
            chunkXs[index] = (int) x;
            chunkZs[index] = (int) z;
            rows[index] = checkedCount(input.readVarUnsigned() + 1);
        }
        return new ChunkList(worldIds, chunkXs, chunkZs, rows, unlocated);
    }

    private static int checkedCount(long value) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Segment frame contains a malformed count");
        }
        return (int) value;
    }

    /**
     * Counts how many rows of a whole segment lie in each chunk, in the form stored beside it.
     *
     * @param rows
     *            the rows of the segment
     * @param location
     *            the indexes of the world, x and z columns, or null when the rows have no place
     * @return the stored form; never null, so a segment whose rows have no place still records
     *         that it has been counted
     */
    static byte[] chunkCounts(List<Object[]> rows, int[] location) {
        ChunkList chunks;
        if (location == null) {
            chunks = new ChunkList(new int[0], new int[0], new int[0], new int[0], rows.size());
        }
        else {
            chunks = tally(rows, 0, rows.size(), location, Integer.MAX_VALUE);
        }
        return encodeChunkCounts(chunks);
    }

    /**
     * @param chunks
     *            every chunk of a segment and its row count
     * @return the form stored beside the segment
     */
    static byte[] encodeChunkCounts(ChunkList chunks) {
        Output output = new Output();
        output.write(CHUNK_COUNTS_FORMAT);
        output.writeVarUnsigned(chunks.size());
        writeChunkEntries(output, chunks);
        return output.toByteArray();
    }

    /**
     * @param encoded
     *            the chunk counts stored beside a segment, may be null
     * @return the counts, or null when there are none or they are of a form this build does not know
     */
    static ChunkList decodeChunkCounts(byte[] encoded) {
        if (encoded == null || encoded.length < 2 || encoded[0] != CHUNK_COUNTS_FORMAT) {
            return null;
        }
        Input input = new Input(encoded);
        input.read();
        ChunkList chunks = readChunkEntries(input, checkedCount(input.readVarUnsigned()));
        if (input.offset != encoded.length) {
            throw new IllegalArgumentException("Segment chunk counts have " + (encoded.length - input.offset) + " unread bytes");
        }
        return chunks;
    }

    /**
     * Encodes a block of rows in the first format, one body for the whole segment. Kept so that the
     * reading of segments written before groups existed stays tested.
     *
     * @param types
     *            the column kind of each column
     * @param rowIds
     *            the row id of each row
     * @param rows
     *            the column values of each row
     * @return the encoded frames, with a scalar frame that still has to be compressed for storage
     */
    static Frames encodeVersionOne(int[] types, long[] rowIds, List<Object[]> rows) {
        int rowCount = rows.size();
        if (rowCount != rowIds.length) {
            throw new IllegalArgumentException("Row id count does not match the row count");
        }
        checkTypes(types);

        Output scalars = new Output();
        ByteArrayOutputStream payload = new ByteArrayOutputStream(Math.max(64, rowCount * 8));
        scalars.write(MAGIC_FIRST);
        scalars.write(MAGIC_SECOND);
        scalars.write(VERSION_ONE);
        scalars.writeVarUnsigned(types.length);
        for (int type : types) {
            scalars.write(type);
        }
        scalars.writeVarUnsigned(rowCount);
        writeBody(scalars, payload, types, rowIds, rows);
        return new Frames(scalars.toByteArray(), payload.toByteArray());
    }

    private static void checkTypes(int[] types) {
        for (int type : types) {
            if (type != TYPE_INTEGER && type != TYPE_REAL && type != TYPE_TEXT && type != TYPE_BLOB) {
                throw new IllegalArgumentException("Unsupported column kind " + type);
            }
        }
    }

    /** Writes row ids and every column of a run of rows, the body of the first format. */
    private static void writeBody(Output output, ByteArrayOutputStream payload, int[] types, long[] rowIds, List<Object[]> rows) {
        writeRowIds(output, rowIds, 0, rowIds.length);
        for (int column = 0; column < types.length; column++) {
            writeColumn(output, payload, types[column], rows, column);
        }
    }

    private static void writeRowIds(Output output, long[] rowIds, int from, int to) {
        long previousRowId = 0;
        for (int row = from; row < to; row++) {
            output.writeZigZag(rowIds[row] - previousRowId);
            previousRowId = rowIds[row];
        }
    }

    /** Writes one column of a run of rows: which rows hold a value, then the values. */
    private static void writeColumn(Output output, ByteArrayOutputStream payload, int type, List<Object[]> rows, int column) {
        writeNullBitmap(output, rows, column);
        switch (type) {
            case TYPE_INTEGER:
                writeIntegerPlane(output, rows, column);
                break;
            case TYPE_REAL:
                writeRealPlane(output, rows, column);
                break;
            default:
                writeBytePlane(output, payload, rows, column, type == TYPE_TEXT);
                break;
        }
    }

    /**
     * Records the range of every integer column of a group. A range is only recorded when every row
     * of the group has a value, because a row without one is accepted by any filter on that column,
     * and a range that left it out would let the group be skipped with that row in it.
     */
    private static void writeZones(Output output, int[] types, List<Object[]> rows) {
        for (int column = 0; column < types.length; column++) {
            if (types[column] != TYPE_INTEGER) {
                output.write(0);
                continue;
            }
            long minimum = Long.MAX_VALUE;
            long maximum = Long.MIN_VALUE;
            boolean complete = !rows.isEmpty();
            for (Object[] row : rows) {
                Object value = row[column];
                if (value == null) {
                    complete = false;
                    break;
                }
                long number = ((Number) value).longValue();
                minimum = Math.min(minimum, number);
                maximum = Math.max(maximum, number);
            }
            if (!complete) {
                output.write(0);
                continue;
            }
            output.write(1);
            output.writeZigZag(minimum);
            output.writeZigZag(maximum);
        }
    }

    public static Rows decode(byte[] scalars, byte[] payload) {
        return decode(scalars, payload, null);
    }

    /**
     * Decodes a segment, keeping only the rows a filter accepts.
     *
     * @param scalars
     *            the scalar frame, as the codec produced it
     * @param payload
     *            the decompressed payload frame, may be empty
     * @param filter
     *            decides which rows to build, or null to build all of them
     * @return the decoded rows
     */
    public static Rows decode(byte[] scalars, byte[] payload, RowFilter filter) {
        return decode(scalars, null, payload, filter);
    }

    /**
     * Decodes a segment whose unit bodies may be stored apart from its directory, keeping only the
     * rows a filter accepts.
     *
     * @param scalars
     *            the scalar frame: a directory, or a frame with its bodies in line
     * @param units
     *            the stored body of each unit of a directory, by unit number, with null for a unit
     *            that was not read; ignored for a frame with its bodies in line
     * @param payload
     *            the decompressed payload frame, may be empty
     * @param filter
     *            decides which rows to build, or null to build all of them
     * @return the decoded rows
     */
    public static Rows decode(byte[] scalars, byte[][] units, byte[] payload, RowFilter filter) {
        return decode(scalars, units, payload, filter, null);
    }

    /**
     * Decodes a segment as {@link #decode(byte[], byte[][], byte[], RowFilter)} does, showing it with
     * the columns its table has now.
     *
     * @param columns
     *            the table's columns, which a segment packed before some were added is read as, or
     *            null to read the segment with the columns it was packed with
     */
    public static Rows decode(byte[] scalars, byte[][] units, byte[] payload, RowFilter filter, Columns columns) {
        return select(scalars, units, filter, columns).build(payload == null ? new byte[0] : payload);
    }

    /**
     * Decodes a segment, keeping only the rows a filter accepts and asking for the payload frame
     * only when one of them has a text or blob value to build.
     *
     * @param scalars
     *            the scalar frame, as the codec produced it
     * @param payload
     *            supplies the decompressed payload frame
     * @param filter
     *            decides which rows to build, or null to build all of them
     * @return the decoded rows
     */
    public static Rows decode(byte[] scalars, Payload payload, RowFilter filter) {
        return decode(scalars, null, payload, filter);
    }

    /**
     * Decodes a segment whose unit bodies may be stored apart from its directory, keeping only the
     * rows a filter accepts and asking for the payload frame only when one of them needs it.
     *
     * <p>
     * Only the units holding a group the filter may match are looked at, which are the units
     * {@link #neededUnits} names for the same filter. A unit that holds such a group and was not
     * supplied is an error rather than a gap: skipping it would quietly leave rows out.
     * </p>
     *
     * @param scalars
     *            the scalar frame: a directory, or a frame with its bodies in line
     * @param units
     *            the stored body of each unit of a directory, by unit number, with null for a unit
     *            that was not read; ignored for a frame with its bodies in line
     * @param payload
     *            supplies the decompressed payload frame
     * @param filter
     *            decides which rows to build, or null to build all of them
     * @return the decoded rows
     */
    public static Rows decode(byte[] scalars, byte[][] units, Payload payload, RowFilter filter) {
        Selection selection = select(scalars, units, filter);
        if (!selection.needsPayload()) {
            return selection.build(null);
        }
        return selection.build(payload == null ? new byte[0] : payload.get());
    }

    /**
     * Decodes the rows of a segment a filter keeps, building everything but their text and blob
     * values, which lie in the payload frame. Whether that frame is needed at all is only known
     * once the kept rows are, so this is the first of two steps: the caller reads the payload only
     * if {@link Selection#needsPayload()} says a kept row has such a value, and then finishes the
     * rows with {@link Selection#build(byte[])}. Units are handled as in
     * {@link #decode(byte[], byte[][], Payload, RowFilter)}.
     *
     * @param scalars
     *            the scalar frame: a directory, or a frame with its bodies in line
     * @param units
     *            the stored body of each unit of a directory, by unit number, with null for a unit
     *            that was not read; ignored for a frame with its bodies in line
     * @param filter
     *            decides which rows to build, or null to build all of them
     * @return the kept rows, waiting for their text and blob values
     */
    public static Selection select(byte[] scalars, byte[][] units, RowFilter filter) {
        return select(scalars, units, filter, null);
    }

    /**
     * Selects rows as {@link #select(byte[], byte[][], RowFilter)} does, showing the segment with
     * the columns its table has now.
     *
     * @param columns
     *            the table's columns, or null to read the segment with the columns it was packed with
     */
    public static Selection select(byte[] scalars, byte[][] units, RowFilter filter, Columns columns) {
        return collect(scalars, units, filter, false, null, columns).selection();
    }

    /**
     * Counts how many rows a filter accepts, without building any of them.
     *
     * @param scalars
     *            the scalar frame
     * @param payload
     *            ignored; kept so existing callers need not change
     * @param filter
     *            decides which rows count, or null to count them all
     * @return the number of accepted rows
     */
    public static int count(byte[] scalars, byte[] payload, RowFilter filter) {
        return count(scalars, filter);
    }

    /**
     * Counts the rows a filter accepts from the scalar frame alone. A count never looks at text or
     * blob values, so the payload frame is never needed and never decompressed.
     *
     * @param scalars
     *            the scalar frame
     * @param filter
     *            decides which rows count, or null to count them all
     * @return the number of accepted rows
     */
    public static int count(byte[] scalars, RowFilter filter) {
        return count(scalars, (byte[][]) null, filter);
    }

    /**
     * Counts the rows a filter accepts from a scalar frame whose unit bodies may be stored apart.
     *
     * @param scalars
     *            the scalar frame: a directory, or a frame with its bodies in line
     * @param units
     *            the stored body of each unit of a directory, by unit number, with null for a unit
     *            that was not read; ignored for a frame with its bodies in line
     * @param filter
     *            decides which rows count, or null to count them all
     * @return the number of accepted rows
     */
    public static int count(byte[] scalars, byte[][] units, RowFilter filter) {
        return count(scalars, units, filter, null);
    }

    /**
     * Counts rows as {@link #count(byte[], byte[][], RowFilter)} does, showing the filter the segment
     * with the columns its table has now.
     *
     * @param columns
     *            the table's columns, or null to read the segment with the columns it was packed with
     */
    public static int count(byte[] scalars, byte[][] units, RowFilter filter, Columns columns) {
        return collect(scalars, units, filter, true, null, columns).size;
    }

    /**
     * The ids and times of the rows a filter accepts, and nothing else of them.
     *
     * <p>
     * Working out where a page of a long history starts only needs to know where each matching
     * row stands in the page's order, which its time and id decide. Like a count, this reads the
     * scalar frame alone and never builds a row.
     * </p>
     */
    public static final class KeyRows {
        private final int timeColumn;
        private long[] rowIds = new long[64];
        private long[] times = new long[64];
        private int size;

        KeyRows(int timeColumn) {
            this.timeColumn = timeColumn;
        }

        private void add(long rowId, long time) {
            if (size == rowIds.length) {
                rowIds = Arrays.copyOf(rowIds, size * 2);
                times = Arrays.copyOf(times, size * 2);
            }
            rowIds[size] = rowId;
            times[size] = time;
            size++;
        }

        public int size() {
            return size;
        }

        public long rowId(int row) {
            return rowIds[row];
        }

        /**
         * @return the row's time, or {@link Long#MIN_VALUE} when it has none or none was asked for
         */
        public long time(int row) {
            return times[row];
        }
    }

    /**
     * Collects the id and time of every row a filter accepts, from the scalar frame alone.
     *
     * @param scalars
     *            the scalar frame: a directory, or a frame with its bodies in line
     * @param units
     *            the stored body of each unit of a directory, by unit number, with null for a unit
     *            that was not read; ignored for a frame with its bodies in line
     * @param filter
     *            decides which rows are collected, or null to collect them all
     * @param timeColumn
     *            the column holding each row's time, or -1 to collect ids alone
     * @return the ids and times of the accepted rows, in the order they are stored
     */
    public static KeyRows keys(byte[] scalars, byte[][] units, RowFilter filter, int timeColumn) {
        return keys(scalars, units, filter, timeColumn, null);
    }

    /**
     * Collects ids and times as {@link #keys(byte[], byte[][], RowFilter, int)} does, showing the
     * filter the segment with the columns its table has now.
     *
     * @param columns
     *            the table's columns, or null to read the segment with the columns it was packed with
     */
    public static KeyRows keys(byte[] scalars, byte[][] units, RowFilter filter, int timeColumn, Columns columns) {
        KeyRows keys = new KeyRows(timeColumn);
        collect(scalars, units, filter, true, keys, columns);
        return keys;
    }

    /**
     * @param scalars
     *            a stored scalar frame
     * @return true if the frame is stored as the codec produced it, rather than compressed as a
     *         whole, which is the case for every frame since version 2, whatever later version
     *         wrote it
     */
    public static boolean isSelfContained(byte[] scalars) {
        return scalars != null && scalars.length >= 3 && scalars[0] == MAGIC_FIRST && scalars[1] == MAGIC_SECOND && scalars[2] >= VERSION_GROUPS;
    }

    /**
     * @param scalars
     *            a stored scalar frame
     * @return true if the frame is a directory whose unit bodies are stored apart from it, and so
     *         have to be read before the groups in them can be decoded
     */
    public static boolean isSplit(byte[] scalars) {
        return scalars != null && scalars.length >= 3 && scalars[0] == MAGIC_FIRST && scalars[1] == MAGIC_SECOND && scalars[2] == VERSION_SPLIT;
    }

    /**
     * @param scalars
     *            a scalar frame of the grouped format, split or not
     * @return how many units the frame's groups are compressed in
     */
    public static int unitCount(byte[] scalars) {
        // Only the header is read: the count comes before the directory of groups, which a reader
        // that already knows which units it wants has no reason to walk again.
        try {
            Input input = new Input(scalars);
            if (input.read() != MAGIC_FIRST || input.read() != MAGIC_SECOND) {
                throw new IllegalArgumentException("Segment does not use the CoreProtect segment format");
            }
            int version = input.read();
            if (version == VERSION_ONE) {
                return 0;
            }
            int columnCount = checkedCount(input.readVarUnsigned());
            for (int column = 0; column < columnCount; column++) {
                input.read();
            }
            input.readVarUnsigned();
            input.readVarUnsigned();
            return checkedCount(input.readVarUnsigned());
        }
        catch (ArrayIndexOutOfBoundsException truncated) {
            throw new IllegalArgumentException("Segment frame ended unexpectedly", truncated);
        }
    }

    /**
     * Works out which units a decode with a filter would open, from the directory alone, so that a
     * reader can fetch exactly those bodies and no others. The tests are the ones the decoder makes:
     * a unit is needed when any of its groups may match, by its chunks and by its ranges.
     *
     * @param scalars
     *            a scalar frame of the grouped format, split or not
     * @param filter
     *            the filter the decode will use, or null when every row is wanted
     * @return the numbers of the needed units, in ascending order
     */
    public static int[] neededUnits(byte[] scalars, RowFilter filter) {
        return neededUnits(scalars, filter, null);
    }

    /**
     * Works out the needed units as {@link #neededUnits(byte[], RowFilter)} does, testing the
     * segment's groups with the columns its table has now.
     *
     * @param columns
     *            the table's columns, or null to read the segment with the columns it was packed with
     */
    public static int[] neededUnits(byte[] scalars, RowFilter filter, Columns columns) {
        Directory directory = readDirectory(scalars);
        directory.widen(columns);
        if (directory.version == VERSION_ONE) {
            throw new IllegalArgumentException("A segment of the first format has no units");
        }
        int[] needed = new int[directory.unitCount];
        int count = 0;
        for (int unit = 0; unit < directory.unitCount; unit++) {
            int lastGroup = Math.min(directory.groupCount, (unit + 1) * directory.unitGroups);
            for (int group = unit * directory.unitGroups; group < lastGroup; group++) {
                if (directory.mayHold(group, filter)) {
                    needed[count++] = unit;
                    break;
                }
            }
        }
        return Arrays.copyOf(needed, count);
    }

    private static Collector collect(byte[] scalars, byte[][] units, RowFilter filter, boolean countOnly, KeyRows keys, Columns columns) {
        Scratch scratch = borrowScratch();
        try {
            Directory directory = readDirectory(scalars);
            directory.widen(columns);
            Collector collector = new Collector(directory.width, directory.shownTypes, countOnly);
            collector.keys = keys;
            if (directory.version == VERSION_ONE) {
                decodeVersionOne(scalars, directory, filter, collector, scratch);
            }
            else if (directory.version == VERSION_SPLIT) {
                if (directory.end != scalars.length) {
                    throw new IllegalArgumentException("Segment directory has " + (scalars.length - directory.end) + " unread bytes");
                }
                decodeGroups(directory, null, units, filter, collector, scratch);
            }
            else {
                decodeGroups(directory, scalars, null, filter, collector, scratch);
            }
            return collector;
        }
        catch (ArrayIndexOutOfBoundsException truncated) {
            throw new IllegalArgumentException("Segment frame ended unexpectedly", truncated);
        }
        finally {
            releaseScratch(scratch);
        }
    }

    /**
     * The header and directory of a scalar frame: its columns and rows, and for the grouped formats
     * how its groups are laid out in units and what each group holds.
     */
    private static final class Directory {
        private int version;
        private int[] types;
        private int rowCount;
        private int groupCount;
        private int unitCount;
        private int unitGroups;
        private int[] kinds;
        private int[] unitRaw;
        private int[] unitStored;
        private int[] groupRows;
        private int[][] chunkLengths;
        private int[][] payloadStarts;
        private long[][] minimum;
        private long[][] maximum;
        private boolean[][] known;
        private ChunkList[] chunks;
        /** Where the directory ends: where the first body begins in a frame with its bodies in line. */
        private int end;
        /**
         * How many columns the segment is shown with: the columns it was packed with, or more when it
         * is read against a table that has gained columns since.
         */
        private int width;
        /** The kind of each column the segment is shown with; the stored kinds, then any appended. */
        private int[] shownTypes;
        /** The plain number every row shows in each appended column, by column. */
        private long[] appendedNumbers;
        /** Whether every row holds a value in each appended column, by column. */
        private boolean[] appendedPresent;

        /**
         * Shows the segment with the columns its table has now. Every column the segment lacks is
         * appended holding the table's default for it, in every row, and each group's ranges say so,
         * so a filter about such a column judges the groups of an old segment just as it judges the
         * rows in them.
         *
         * @param columns
         *            the table's columns, or null to keep the columns the segment was packed with
         * @throws IllegalArgumentException
         *             if the segment's columns are not where the table has them, or a column it lacks
         *             has a default that cannot be shown
         */
        private void widen(Columns columns) {
            int stored = types.length;
            width = stored;
            shownTypes = types;
            if (columns == null || columns.size() <= stored) {
                return;
            }
            for (int column = 0; column < stored; column++) {
                if (columns.types[column] != types[column]) {
                    throw new IllegalArgumentException("Segment column " + column + " is not of the kind its table has there");
                }
            }
            int shown = columns.size();
            long[] numbers = new long[shown];
            boolean[] present = new boolean[shown];
            for (int column = stored; column < shown; column++) {
                Object value = columns.defaults[column];
                if (value == Columns.UNSUPPORTED_DEFAULT) {
                    throw new IllegalArgumentException("Segment predates column " + column + ", whose default cannot be shown");
                }
                present[column] = value != null;
                if (value instanceof Long) {
                    numbers[column] = (Long) value;
                }
                else if (value instanceof Double) {
                    numbers[column] = Double.doubleToLongBits((Double) value);
                }
            }
            if (version != VERSION_ONE) {
                for (int group = 0; group < groupCount; group++) {
                    minimum[group] = Arrays.copyOf(minimum[group], shown);
                    maximum[group] = Arrays.copyOf(maximum[group], shown);
                    known[group] = Arrays.copyOf(known[group], shown);
                    for (int column = stored; column < shown; column++) {
                        // A range is only ever known for an integer column with a value in every row.
                        if (present[column] && columns.types[column] == TYPE_INTEGER) {
                            known[group][column] = true;
                            minimum[group][column] = numbers[column];
                            maximum[group][column] = numbers[column];
                        }
                    }
                }
            }
            width = shown;
            shownTypes = columns.types;
            appendedNumbers = numbers;
            appendedPresent = present;
        }

        /**
         * Fills the appended columns of freshly decoded rows with the table's defaults.
         */
        private void fillAppended(Scratch scratch, int rowCount) {
            for (int column = types.length; column < width; column++) {
                Arrays.fill(scratch.present[column], 0, rowCount, appendedPresent[column]);
                Arrays.fill(scratch.planes[column], 0, rowCount, appendedNumbers[column]);
                Arrays.fill(scratch.lengths[column], 0, rowCount, 0);
            }
        }

        /**
         * @return false only if no row of the group can pass the filter. The chunks are asked
         *         about first: they are exact where the ranges are not, and a group they rule out is
         *         never compared against its ranges at all.
         */
        private boolean mayHold(int group, RowFilter filter) {
            if (filter == null) {
                return true;
            }
            if (chunks[group] != null && !filter.mayMatchChunks(chunks[group])) {
                return false;
            }
            return filter.mayMatch(minimum[group], maximum[group], known[group]);
        }
    }

    private static Directory readDirectory(byte[] scalars) {
        try {
            Input input = new Input(scalars);
            if (input.read() != MAGIC_FIRST || input.read() != MAGIC_SECOND) {
                throw new IllegalArgumentException("Segment does not use the CoreProtect segment format");
            }
            Directory directory = new Directory();
            int version = input.read();
            if (version != VERSION_ONE && version != VERSION_GROUPS && version != VERSION_CHUNKS && version != VERSION_SPLIT) {
                throw new IllegalArgumentException("Unsupported segment format version " + version);
            }
            directory.version = version;

            int columnCount = (int) input.readVarUnsigned();
            int[] types = new int[columnCount];
            for (int column = 0; column < columnCount; column++) {
                types[column] = input.read();
            }
            directory.types = types;
            directory.width = columnCount;
            directory.shownTypes = types;
            directory.rowCount = (int) input.readVarUnsigned();
            if (version != VERSION_ONE) {
                readGroups(input, directory, version >= VERSION_CHUNKS);
            }
            directory.end = input.offset;
            return directory;
        }
        catch (ArrayIndexOutOfBoundsException | NegativeArraySizeException truncated) {
            throw new IllegalArgumentException("Segment frame ended unexpectedly", truncated);
        }
    }

    private static void decodeVersionOne(byte[] data, Directory directory, RowFilter filter, Collector collector, Scratch pooled) {
        int rowCount = directory.rowCount;
        Scratch scratch = pooled.prepare(directory.width, rowCount);
        int end = decodeBody(data, directory.end, directory.types, rowCount, 0, scratch);
        if (end != data.length) {
            throw new IllegalArgumentException("Segment frame has " + (data.length - end) + " unread bytes");
        }
        directory.fillAppended(scratch, rowCount);
        collector.collect(scratch, rowCount, filter);
    }

    private static void readGroups(Input input, Directory directory, boolean listsChunks) {
        int[] types = directory.types;
        int columnCount = types.length;
        int groupCount = (int) input.readVarUnsigned();
        int unitCount = (int) input.readVarUnsigned();
        int unitGroups = Math.max(1, (int) input.readVarUnsigned());
        int[] kinds = new int[unitCount];
        int[] unitRaw = new int[unitCount];
        int[] unitStored = new int[unitCount];
        for (int unit = 0; unit < unitCount; unit++) {
            kinds[unit] = input.read();
            unitRaw[unit] = (int) input.readVarUnsigned();
            unitStored[unit] = (int) input.readVarUnsigned();
        }

        int[] groupRows = new int[groupCount];
        int[][] chunkLengths = new int[groupCount][columnCount + 1];
        int[][] payloadStarts = new int[groupCount][columnCount];
        long[][] minimum = new long[groupCount][columnCount];
        long[][] maximum = new long[groupCount][columnCount];
        boolean[][] known = new boolean[groupCount][columnCount];
        ChunkList[] chunks = new ChunkList[groupCount];
        int total = 0;
        for (int group = 0; group < groupCount; group++) {
            groupRows[group] = (int) input.readVarUnsigned();
            for (int chunk = 0; chunk <= columnCount; chunk++) {
                chunkLengths[group][chunk] = (int) input.readVarUnsigned();
            }
            for (int column = 0; column < columnCount; column++) {
                if (types[column] == TYPE_TEXT || types[column] == TYPE_BLOB) {
                    payloadStarts[group][column] = (int) input.readVarUnsigned();
                }
            }
            for (int column = 0; column < columnCount; column++) {
                if (input.read() == 1) {
                    known[group][column] = true;
                    minimum[group][column] = input.readZigZag();
                    maximum[group][column] = input.readZigZag();
                }
            }
            if (listsChunks) {
                long listed = input.readVarUnsigned();
                if (listed > GROUP_CHUNK_LIMIT + 1L) {
                    throw new IllegalArgumentException("Segment group lists " + (listed - 1) + " chunks");
                }
                chunks[group] = listed == 0 ? null : readChunkEntries(input, (int) listed - 1);
            }
            total = total + groupRows[group];
        }
        if (total != directory.rowCount) {
            throw new IllegalArgumentException("Segment groups hold " + total + " rows but the segment has " + directory.rowCount);
        }
        if ((long) unitCount * unitGroups < groupCount) {
            throw new IllegalArgumentException("Segment units hold fewer groups than the segment has");
        }

        directory.groupCount = groupCount;
        directory.unitCount = unitCount;
        directory.unitGroups = unitGroups;
        directory.kinds = kinds;
        directory.unitRaw = unitRaw;
        directory.unitStored = unitStored;
        directory.groupRows = groupRows;
        directory.chunkLengths = chunkLengths;
        directory.payloadStarts = payloadStarts;
        directory.minimum = minimum;
        directory.maximum = maximum;
        directory.known = known;
        directory.chunks = chunks;
    }

    /**
     * Decodes the groups a filter may match, unit by unit.
     *
     * @param inline
     *            the whole frame when the unit bodies follow the directory in it, or null when they
     *            are stored apart and handed over in {@code units}
     * @param units
     *            the stored bodies of a split frame, by unit number, null where one was not read
     */
    private static void decodeGroups(Directory directory, byte[] inline, byte[][] units, RowFilter filter, Collector collector, Scratch pooled) {
        int columnCount = directory.types.length;
        int groupCount = directory.groupCount;
        int unitGroups = directory.unitGroups;
        int[][] chunkLengths = directory.chunkLengths;
        int unitOffset = directory.end;
        int[] starts = new int[columnCount + 1];
        for (int unit = 0; unit < directory.unitCount; unit++) {
            int stored = directory.unitStored[unit];
            int raw = directory.unitRaw[unit];
            if (inline != null && unitOffset + stored > inline.length) {
                throw new IllegalArgumentException("Segment frame ended unexpectedly");
            }
            int firstGroup = unit * unitGroups;
            int lastGroup = Math.min(groupCount, firstGroup + unitGroups);

            // Where each group's chunk of each column starts: chunks are laid out column by column,
            // each column's chunks in group order.
            int[][] chunkStarts = new int[Math.max(0, lastGroup - firstGroup)][columnCount + 1];
            int position = 0;
            for (int chunk = 0; chunk <= columnCount; chunk++) {
                for (int group = firstGroup; group < lastGroup; group++) {
                    chunkStarts[group - firstGroup][chunk] = position;
                    position = position + chunkLengths[group][chunk];
                }
            }
            if (position != raw) {
                throw new IllegalArgumentException("Segment unit holds " + raw + " bytes but its groups describe " + position);
            }

            byte[] body = null;
            int bodyStart = 0;
            for (int group = firstGroup; group < lastGroup; group++) {
                if (!directory.mayHold(group, filter)) {
                    continue;
                }
                if (body == null) {
                    // Only a unit holding a group that can match is ever decompressed, or, when the
                    // bodies are stored apart, ever needed at all.
                    byte[] source;
                    int sourceStart;
                    if (inline != null) {
                        source = inline;
                        sourceStart = unitOffset;
                    }
                    else {
                        source = units == null || unit >= units.length ? null : units[unit];
                        if (source == null) {
                            throw new IllegalArgumentException("Segment unit " + unit + " holds rows the lookup wants but was not read");
                        }
                        if (source.length != stored) {
                            throw new IllegalArgumentException("Segment unit " + unit + " holds " + source.length + " bytes but its directory records " + stored);
                        }
                        sourceStart = 0;
                    }
                    if (directory.kinds[unit] == BODY_ZSTD) {
                        byte[] packed = inline != null ? Arrays.copyOfRange(source, sourceStart, sourceStart + stored) : source;
                        body = Zstd.decompress(packed, raw);
                        if (body.length != raw) {
                            throw new IllegalArgumentException("Segment unit decompressed to the wrong length");
                        }
                        bodyStart = 0;
                    }
                    else if (directory.kinds[unit] == BODY_PLAIN) {
                        body = source;
                        bodyStart = sourceStart;
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported segment unit encoding " + directory.kinds[unit]);
                    }
                    ColdSegmentReadCounter.recordUnitOpened();
                }

                ColdSegmentReadCounter.recordGroupRead();
                for (int chunk = 0; chunk <= columnCount; chunk++) {
                    starts[chunk] = bodyStart + chunkStarts[group - firstGroup][chunk];
                }
                int rows = directory.groupRows[group];
                Scratch scratch = pooled.prepare(directory.width, rows);
                decodeChunks(body, starts, chunkLengths[group], directory.types, rows, directory.payloadStarts[group], scratch);
                directory.fillAppended(scratch, rows);
                collector.collect(scratch, rows, filter);
            }
            unitOffset = unitOffset + stored;
        }

        if (inline != null && directory.unitCount > 0 && unitOffset != inline.length) {
            throw new IllegalArgumentException("Segment frame does not end where its units do");
        }
    }

    /**
     * Where each unit's share of the payload frame begins, from the directory alone.
     *
     * <p>
     * The payload frame holds the text and blob bytes of one unit after another, so the share of a
     * unit begins where its first group's first text or blob column does, and ends where the next
     * unit's begins. A segment that keeps each unit's share apart is read this way: a value's
     * offset in the directory says which share holds it.
     * </p>
     *
     * @param scalars
     *            a directory of the grouped format
     * @return the offset each unit's share begins at, by unit number; empty when the rows have no
     *         text or blob column, and so no payload
     */
    static int[] unitPayloadStarts(byte[] scalars) {
        return unitPayloadStarts(readDirectory(scalars));
    }

    private static int[] unitPayloadStarts(Directory directory) {
        if (directory.version == VERSION_ONE) {
            throw new IllegalArgumentException("A segment of the first format has no units");
        }
        int firstColumn = -1;
        for (int column = 0; column < directory.types.length && firstColumn < 0; column++) {
            if (directory.types[column] == TYPE_TEXT || directory.types[column] == TYPE_BLOB) {
                firstColumn = column;
            }
        }
        if (firstColumn < 0) {
            return new int[0];
        }
        int[] starts = new int[directory.unitCount];
        for (int unit = 0; unit < directory.unitCount; unit++) {
            starts[unit] = directory.payloadStarts[unit * directory.unitGroups][firstColumn];
        }
        return starts;
    }

    /**
     * The payload of a segment that keeps each unit's share apart, holding only the shares a read
     * needed. A value is found by the share its offset falls in; asking for one in a share that was
     * not supplied is an error rather than a gap.
     */
    static final class PayloadShares {
        private final int[] starts;
        private final byte[][] shares;

        /**
         * @param starts
         *            where each unit's share begins, as {@link #unitPayloadStarts} gives them
         * @param shares
         *            the decompressed share of each unit, by unit number, with null for one that was
         *            not read
         */
        PayloadShares(int[] starts, byte[][] shares) {
            if (starts.length != shares.length) {
                throw new IllegalArgumentException("A payload has one share per unit");
            }
            this.starts = starts;
            this.shares = shares;
        }

        /**
         * @return the unit whose share holds the bytes at that offset of the payload frame
         */
        static int unitAt(int[] starts, int offset) {
            // Units with no text or blob bytes begin where the next one does, so the last unit
            // beginning at or before the offset is the one holding bytes there.
            int low = 0;
            int high = starts.length - 1;
            int found = -1;
            while (low <= high) {
                int middle = (low + high) >>> 1;
                if (starts[middle] <= offset) {
                    found = middle;
                    low = middle + 1;
                }
                else {
                    high = middle - 1;
                }
            }
            if (found < 0) {
                throw new IllegalArgumentException("Segment payload offset " + offset + " lies before every unit");
            }
            return found;
        }

        private byte[] share(int offset, int length) {
            int unit = unitAt(starts, offset);
            byte[] share = shares[unit];
            if (share == null) {
                throw new IllegalArgumentException("Segment unit " + unit + " holds text or blob values the lookup wants but its payload was not read");
            }
            if (offset - starts[unit] + length > share.length) {
                throw new IllegalArgumentException("Segment unit " + unit + " payload is shorter than the directory describes");
            }
            return share;
        }

        String text(int offset, int length) {
            byte[] share = share(offset, length);
            int unit = unitAt(starts, offset);
            return new String(share, offset - starts[unit], length, StandardCharsets.UTF_8);
        }

        byte[] bytes(int offset, int length) {
            byte[] share = share(offset, length);
            int unit = unitAt(starts, offset);
            int from = offset - starts[unit];
            return Arrays.copyOfRange(share, from, from + length);
        }
    }

    /**
     * Reads a segment of the current format one group at a time, for work that walks every row of
     * many segments at once and so cannot hold any of them whole.
     */
    static final class GroupReader {
        private final Directory directory;
        private final int[] payloadStarts;

        /**
         * @param scalars
         *            a directory whose unit bodies are stored apart
         */
        GroupReader(byte[] scalars) {
            this(scalars, null);
        }

        /**
         * @param scalars
         *            a directory whose unit bodies are stored apart
         * @param columns
         *            the columns the segment's table has now, which its rows are shown with, or null
         *            for the columns it was packed with
         */
        GroupReader(byte[] scalars, Columns columns) {
            this.directory = readDirectory(scalars);
            directory.widen(columns);
            if (directory.version != VERSION_SPLIT) {
                throw new IllegalArgumentException("Only a segment whose units are stored apart can be read a group at a time, not version " + directory.version);
            }
            if (directory.end != scalars.length) {
                throw new IllegalArgumentException("Segment directory has " + (scalars.length - directory.end) + " unread bytes");
            }
            this.payloadStarts = unitPayloadStarts(directory);
        }

        int unitCount() {
            return directory.unitCount;
        }

        int groupCount() {
            return directory.groupCount;
        }

        int unitOf(int group) {
            return group / directory.unitGroups;
        }

        int rowCount() {
            return directory.rowCount;
        }

        int columnCount() {
            return directory.width;
        }

        int firstGroup(int unit) {
            return unit * directory.unitGroups;
        }

        int endGroup(int unit) {
            return Math.min(directory.groupCount, (unit + 1) * directory.unitGroups);
        }

        int groupRows(int group) {
            return directory.groupRows[group];
        }

        /**
         * @return where the unit's share of the payload frame begins, or 0 when the rows have no
         *         text or blob column
         */
        int payloadStart(int unit) {
            return payloadStarts.length == 0 ? 0 : payloadStarts[unit];
        }

        /**
         * @param payloadSize
         *            how many bytes the whole payload frame holds
         * @return where the unit's share of the payload frame ends
         */
        int payloadEnd(int unit, long payloadSize) {
            if (payloadStarts.length == 0) {
                return 0;
            }
            return unit + 1 < payloadStarts.length ? payloadStarts[unit + 1] : Math.toIntExact(payloadSize);
        }

        /**
         * @param unit
         *            a unit number
         * @param stored
         *            the unit's body as stored
         * @return the body ready to decode groups from
         */
        byte[] openUnit(int unit, byte[] stored) {
            if (stored == null || stored.length != directory.unitStored[unit]) {
                throw new IllegalArgumentException("Segment unit " + unit + " does not hold the " + directory.unitStored[unit] + " bytes its directory records");
            }
            byte[] body;
            if (directory.kinds[unit] == BODY_ZSTD) {
                body = Zstd.decompress(stored, directory.unitRaw[unit]);
            }
            else if (directory.kinds[unit] == BODY_PLAIN) {
                body = stored;
            }
            else {
                throw new IllegalArgumentException("Unsupported segment unit encoding " + directory.kinds[unit]);
            }
            if (body.length != directory.unitRaw[unit]) {
                throw new IllegalArgumentException("Segment unit decompressed to the wrong length");
            }
            return body;
        }

        /**
         * Decodes every row of one group, text and blob values included.
         *
         * @param group
         *            the group
         * @param body
         *            the opened body of the unit holding it
         * @param payload
         *            the unit's share of the payload frame, or anything when the group holds no text
         *            or blob bytes
         * @param payloadOffset
         *            where that share begins in the payload frame
         * @param rowIds
         *            receives the row ids, and must have room for the group
         * @param rows
         *            receives the rows, and must have room for the group
         * @return how many rows the group holds
         */
        int decodeGroup(int group, byte[] body, byte[] payload, int payloadOffset, long[] rowIds, Object[][] rows) {
            int columnCount = directory.types.length;
            int width = directory.width;
            int[] shownTypes = directory.shownTypes;
            int unit = group / directory.unitGroups;
            int firstGroup = firstGroup(unit);
            int lastGroup = endGroup(unit);
            int[][] chunkLengths = directory.chunkLengths;
            int[] starts = new int[columnCount + 1];
            int position = 0;
            for (int chunk = 0; chunk <= columnCount; chunk++) {
                for (int member = firstGroup; member < lastGroup; member++) {
                    if (member == group) {
                        starts[chunk] = position;
                    }
                    position = position + chunkLengths[member][chunk];
                }
            }
            if (position != body.length) {
                throw new IllegalArgumentException("Segment unit holds " + body.length + " bytes but its groups describe " + position);
            }

            int count = directory.groupRows[group];
            Scratch scratch = borrowScratch();
            try {
                scratch.prepare(width, count);
                decodeChunks(body, starts, chunkLengths[group], directory.types, count, directory.payloadStarts[group], scratch);
                directory.fillAppended(scratch, count);
                for (int row = 0; row < count; row++) {
                    rowIds[row] = scratch.rowIds[row];
                    Object[] built = new Object[width];
                    for (int column = 0; column < width; column++) {
                        if (!scratch.present[column][row]) {
                            continue;
                        }
                        int type = shownTypes[column];
                        if (type == TYPE_INTEGER) {
                            built[column] = Long.valueOf(scratch.planes[column][row]);
                        }
                        else if (type == TYPE_REAL) {
                            built[column] = Double.valueOf(Double.longBitsToDouble(scratch.planes[column][row]));
                        }
                        else {
                            int length = scratch.lengths[column][row];
                            if (length == 0) {
                                // An empty value has no bytes in the payload frame to read.
                                built[column] = type == TYPE_TEXT ? "" : new byte[0];
                                continue;
                            }
                            int from = scratch.offsets[column][row] - payloadOffset;
                            if (payload == null || from < 0 || from + length > payload.length) {
                                throw new IllegalArgumentException("Segment unit " + unit + " payload does not hold the values its directory describes");
                            }
                            built[column] = type == TYPE_TEXT ? new String(payload, from, length, StandardCharsets.UTF_8) : Arrays.copyOfRange(payload, from, from + length);
                        }
                    }
                    rows[row] = built;
                }
            }
            finally {
                releaseScratch(scratch);
            }
            return count;
        }
    }

    /**
     * Decodes one group from its chunks: its row ids, then every column's presence and values, each
     * read from wherever that column's chunk for the group lies.
     */
    private static void decodeChunks(byte[] data, int[] starts, int[] lengths, int[] types, int rowCount, int[] payloadStarts, Scratch scratch) {
        scratch.payloadEnd = 0;
        int end = readDeltas(data, starts[0], scratch.rowIds, null, rowCount);
        requireChunkEnd(end, starts[0], lengths[0]);
        for (int column = 0; column < types.length; column++) {
            int chunk = column + 1;
            boolean[] columnPresent = scratch.present[column];
            int position = readBitmap(data, starts[chunk], columnPresent, rowCount);
            int type = types[column];
            if (type == TYPE_INTEGER) {
                position = readDeltas(data, position, scratch.planes[column], columnPresent, rowCount);
            }
            else if (type == TYPE_REAL) {
                position = readFixed(data, position, scratch.planes[column], columnPresent, rowCount);
            }
            else if (type == TYPE_TEXT || type == TYPE_BLOB) {
                int[] offsets = scratch.offsets[column];
                int[] valueLengths = scratch.lengths[column];
                long[] plane = scratch.planes[column];
                position = readVarints(data, position, valueLengths, columnPresent, rowCount);
                int payloadOffset = payloadStarts[column];
                for (int row = 0; row < rowCount; row++) {
                    // A filter is handed each value's length in bytes, which is all it can learn of
                    // text without the payload, and enough to tell an empty value from one with text.
                    plane[row] = valueLengths[row];
                    if (columnPresent[row]) {
                        offsets[row] = payloadOffset;
                        payloadOffset = payloadOffset + valueLengths[row];
                    }
                }
                scratch.payloadEnd = Math.max(scratch.payloadEnd, payloadOffset);
            }
            else {
                throw new IllegalArgumentException("Unsupported column kind " + type);
            }
            requireChunkEnd(position, starts[chunk], lengths[chunk]);
        }
    }

    private static void requireChunkEnd(int position, int start, int length) {
        if (position != start + length) {
            throw new IllegalArgumentException("Segment group chunk has " + (start + length - position) + " unread bytes");
        }
    }

    /**
     * Decodes one body into scratch space: its row ids, then every column's presence and values, or
     * for text and blob columns where each value lies in the payload frame.
     *
     * @return the position just past the body
     */
    private static int decodeBody(byte[] data, int position, int[] types, int rowCount, int payloadStart, Scratch scratch) {
        position = readDeltas(data, position, scratch.rowIds, null, rowCount);
        int payloadOffset = payloadStart;
        for (int column = 0; column < types.length; column++) {
            boolean[] columnPresent = scratch.present[column];
            position = readBitmap(data, position, columnPresent, rowCount);

            int type = types[column];
            if (type == TYPE_INTEGER) {
                position = readDeltas(data, position, scratch.planes[column], columnPresent, rowCount);
            }
            else if (type == TYPE_REAL) {
                position = readFixed(data, position, scratch.planes[column], columnPresent, rowCount);
            }
            else if (type == TYPE_TEXT || type == TYPE_BLOB) {
                int[] offsets = scratch.offsets[column];
                int[] lengths = scratch.lengths[column];
                long[] plane = scratch.planes[column];
                position = readVarints(data, position, lengths, columnPresent, rowCount);
                for (int row = 0; row < rowCount; row++) {
                    // The value's length in bytes, as in decodeChunks.
                    plane[row] = lengths[row];
                    if (columnPresent[row]) {
                        offsets[row] = payloadOffset;
                        payloadOffset = payloadOffset + lengths[row];
                    }
                }
            }
            else {
                throw new IllegalArgumentException("Unsupported column kind " + type);
            }
        }
        scratch.payloadEnd = payloadOffset;
        return position;
    }

    /**
     * The rows of one segment a filter kept, built except for their text and blob values.
     *
     * <p>
     * Those values lie in the payload frame, which is usually most of what a segment stores. Which
     * of them a lookup needs is only known once its rows are, and often it needs none: the lookup
     * kept no row of the segment, or kept rows whose text and blob columns are empty. So the rows
     * are decoded first, and each value that has to come from the payload is noted by where it
     * lies. The reader then fetches the payload only for the segments that need one, and fills the
     * values in.
     * </p>
     */
    public static final class Selection {
        private final int[] types;
        private final long[] rowIds;
        private final Object[][] values;
        private final int[] pendingRows;
        private final int[] pendingColumns;
        private final int[] pendingOffsets;
        private final int[] pendingLengths;
        private final int pendingCount;
        private final int payloadEnd;

        private Selection(int[] types, long[] rowIds, Object[][] values, int[] pendingRows, int[] pendingColumns, int[] pendingOffsets, int[] pendingLengths, int pendingCount,
                int payloadEnd) {
            this.types = types;
            this.rowIds = rowIds;
            this.values = values;
            this.pendingRows = pendingRows;
            this.pendingColumns = pendingColumns;
            this.pendingOffsets = pendingOffsets;
            this.pendingLengths = pendingLengths;
            this.pendingCount = pendingCount;
            this.payloadEnd = payloadEnd;
        }

        /**
         * @return how many rows the filter kept
         */
        public int size() {
            return rowIds.length;
        }

        /**
         * @return true if a kept row has a text or blob value whose bytes lie in the payload frame
         */
        public boolean needsPayload() {
            return pendingCount > 0;
        }

        /**
         * Fills in the text and blob values of the kept rows.
         *
         * @param payload
         *            the decompressed payload frame; ignored, and may be null, when
         *            {@link #needsPayload()} is false
         * @return the finished rows
         * @throws IllegalArgumentException
         *             if the payload is shorter than the scalar frame says it is
         */
        public Rows build(byte[] payload) {
            if (pendingCount > 0) {
                if (payload == null || payload.length < payloadEnd) {
                    throw new IllegalArgumentException("Segment payload frame is shorter than the scalar frame describes");
                }
                for (int index = 0; index < pendingCount; index++) {
                    int column = pendingColumns[index];
                    int offset = pendingOffsets[index];
                    int length = pendingLengths[index];
                    values[pendingRows[index]][column] = types[column] == TYPE_TEXT ? new String(payload, offset, length, StandardCharsets.UTF_8)
                            : Arrays.copyOfRange(payload, offset, offset + length);
                }
            }
            return new Rows(rowIds, values);
        }

        /**
         * @param starts
         *            where each unit's share of the payload frame begins, as
         *            {@link ColdSegmentCodec#unitPayloadStarts} gives them
         * @return the units whose share holds a text or blob value of a kept row, in ascending
         *         order, which are the only shares a segment keeping them apart has to read
         */
        public int[] payloadUnits(int[] starts) {
            if (pendingCount == 0 || starts.length == 0) {
                return new int[0];
            }
            boolean[] wanted = new boolean[starts.length];
            int count = 0;
            for (int index = 0; index < pendingCount; index++) {
                int unit = PayloadShares.unitAt(starts, pendingOffsets[index]);
                if (!wanted[unit]) {
                    wanted[unit] = true;
                    count++;
                }
            }
            int[] units = new int[count];
            int position = 0;
            for (int unit = 0; unit < wanted.length; unit++) {
                if (wanted[unit]) {
                    units[position++] = unit;
                }
            }
            return units;
        }

        /**
         * Fills in the text and blob values of the kept rows from a payload whose units' shares are
         * kept apart.
         *
         * @param shares
         *            the shares of the units that hold the values
         * @return the finished rows
         */
        Rows buildFromShares(PayloadShares shares) {
            for (int index = 0; index < pendingCount; index++) {
                int column = pendingColumns[index];
                values[pendingRows[index]][column] = types[column] == TYPE_TEXT ? shares.text(pendingOffsets[index], pendingLengths[index])
                        : shares.bytes(pendingOffsets[index], pendingLengths[index]);
            }
            return new Rows(rowIds, values);
        }
    }

    /** Gathers the accepted rows of every body decoded, in order. */
    private static final class Collector {
        private final int columnCount;
        private final int[] types;
        private final boolean countOnly;
        /** Where the ids and times of accepted rows go when only those are wanted, or null. */
        private KeyRows keys;
        private long[] rowIds = new long[16];
        private Object[][] values = new Object[16][];
        private int size;
        /** Where each text and blob value of a kept row lies in the payload frame, row by row. */
        private int[] pendingRows = new int[0];
        private int[] pendingColumns = new int[0];
        private int[] pendingOffsets = new int[0];
        private int[] pendingLengths = new int[0];
        private int pendingCount;
        /** How long the payload frame has to be for every group a pending value came from. */
        private int payloadEnd;

        private Collector(int columnCount, int[] types, boolean countOnly) {
            this.columnCount = columnCount;
            this.types = types;
            this.countOnly = countOnly;
        }

        private void collect(Scratch scratch, int rowCount, RowFilter filter) {
            int[] accepted = scratch.accepted;
            int kept;
            if (filter == null) {
                for (int row = 0; row < rowCount; row++) {
                    accepted[row] = row;
                }
                kept = rowCount;
            }
            else {
                kept = applyFilter(filter, scratch.rowIds, scratch.planes, scratch.present, columnCount, rowCount, scratch.rowValues, scratch.rowPresent, accepted);
            }

            if (keys != null) {
                // Only where each row stands in a page's order is kept: its id and its time.
                for (int index = 0; index < kept; index++) {
                    int row = accepted[index];
                    boolean timed = keys.timeColumn >= 0 && scratch.present[keys.timeColumn][row];
                    keys.add(scratch.rowIds[row], timed ? scratch.planes[keys.timeColumn][row] : Long.MIN_VALUE);
                }
                size = size + kept;
                return;
            }

            if (countOnly) {
                size = size + kept;
                return;
            }

            ensure(size + kept);
            for (int index = 0; index < kept; index++) {
                int row = accepted[index];
                Object[] built = new Object[columnCount];
                for (int column = 0; column < columnCount; column++) {
                    if (!scratch.present[column][row]) {
                        continue;
                    }
                    switch (types[column]) {
                        case TYPE_INTEGER:
                            built[column] = Long.valueOf(scratch.planes[column][row]);
                            break;
                        case TYPE_REAL:
                            built[column] = Double.valueOf(Double.longBitsToDouble(scratch.planes[column][row]));
                            break;
                        default: {
                            int length = scratch.lengths[column][row];
                            if (length == 0) {
                                // An empty value has no bytes in the payload frame to read.
                                built[column] = types[column] == TYPE_TEXT ? "" : new byte[0];
                            }
                            else {
                                defer(size, column, scratch.offsets[column][row], length);
                                payloadEnd = Math.max(payloadEnd, scratch.payloadEnd);
                            }
                            break;
                        }
                    }
                }
                rowIds[size] = scratch.rowIds[row];
                values[size] = built;
                size++;
            }
        }

        private void defer(int row, int column, int offset, int length) {
            if (pendingCount == pendingRows.length) {
                int grown = Math.max(16, pendingCount * 2);
                pendingRows = Arrays.copyOf(pendingRows, grown);
                pendingColumns = Arrays.copyOf(pendingColumns, grown);
                pendingOffsets = Arrays.copyOf(pendingOffsets, grown);
                pendingLengths = Arrays.copyOf(pendingLengths, grown);
            }
            pendingRows[pendingCount] = row;
            pendingColumns[pendingCount] = column;
            pendingOffsets[pendingCount] = offset;
            pendingLengths[pendingCount] = length;
            pendingCount++;
        }

        private void ensure(int capacity) {
            if (capacity <= rowIds.length) {
                return;
            }
            int grown = Math.max(capacity, rowIds.length * 2);
            rowIds = Arrays.copyOf(rowIds, grown);
            values = Arrays.copyOf(values, grown);
        }

        private Selection selection() {
            return new Selection(types, Arrays.copyOf(rowIds, size), Arrays.copyOf(values, size), pendingRows, pendingColumns, pendingOffsets, pendingLengths, pendingCount,
                    payloadEnd);
        }
    }

    /** Reads a presence bitmap into one flag per row. */
    private static int readBitmap(byte[] data, int position, boolean[] present, int rowCount) {
        int bitmapBytes = (rowCount + 7) >>> 3;
        if (position + bitmapBytes > data.length) {
            throw new IllegalArgumentException("Segment frame ended unexpectedly");
        }
        for (int row = 0; row < rowCount; row++) {
            present[row] = (data[position + (row >>> 3)] & (1 << (row & 7))) != 0;
        }
        return position + bitmapBytes;
    }

    /**
     * Reads a plane of zig-zag varint deltas. Kept to a small loop of its own, which the compiler
     * turns into far tighter code than the same loop inlined into a large method.
     */
    private static int readDeltas(byte[] data, int position, long[] plane, boolean[] present, int rowCount) {
        long previous = 0;
        for (int row = 0; row < rowCount; row++) {
            if (present != null && !present[row]) {
                plane[row] = 0;
                continue;
            }
            long value = 0;
            int shift = 0;
            int current;
            do {
                current = data[position++];
                value |= ((long) (current & 0x7F)) << shift;
                shift += 7;
            }
            while (current < 0);
            previous = previous + ((value >>> 1) ^ -(value & 1));
            plane[row] = previous;
        }
        return position;
    }

    /** Reads a plane of plain varints, the lengths of text and blob values. */
    private static int readVarints(byte[] data, int position, int[] plane, boolean[] present, int rowCount) {
        for (int row = 0; row < rowCount; row++) {
            if (!present[row]) {
                plane[row] = 0;
                continue;
            }
            long value = 0;
            int shift = 0;
            int current;
            do {
                current = data[position++];
                value |= ((long) (current & 0x7F)) << shift;
                shift += 7;
            }
            while (current < 0);
            plane[row] = (int) value;
        }
        return position;
    }

    /** Reads a plane of fixed eight byte values, the bits of floating point numbers. */
    private static int readFixed(byte[] data, int position, long[] plane, boolean[] present, int rowCount) {
        for (int row = 0; row < rowCount; row++) {
            if (!present[row]) {
                plane[row] = 0;
                continue;
            }
            long value = 0;
            for (int shift = 0; shift < 64; shift += 8) {
                value |= ((long) (data[position++] & 0xFF)) << shift;
            }
            plane[row] = value;
        }
        return position;
    }

    /** Applies a row filter to every row, returning how many it accepted. */
    private static int applyFilter(RowFilter filter, long[] rowIds, long[][] planes, boolean[][] present, int columnCount, int rowCount, long[] rowValues, boolean[] rowPresent, int[] accepted) {
        int kept = 0;
        for (int row = 0; row < rowCount; row++) {
            for (int column = 0; column < columnCount; column++) {
                rowPresent[column] = present[column][row];
                rowValues[column] = planes[column][row];
            }
            if (filter.acceptRow(rowIds[row], rowValues, rowPresent)) {
                accepted[kept++] = row;
            }
        }
        return kept;
    }

    /**
     * Scratch space kept between decodes. A few are enough for the decode pool and the threads that
     * read alongside it; beyond that a decode allocates its own and lets it go, so the memory held
     * here never grows with the number of threads that have ever read a segment.
     */
    private static final int POOLED_SCRATCH = 6;

    private static final ConcurrentLinkedQueue<Scratch> SCRATCH = new ConcurrentLinkedQueue<>();

    private static final AtomicInteger SCRATCH_POOLED = new AtomicInteger();

    private static Scratch borrowScratch() {
        Scratch scratch = SCRATCH.poll();
        if (scratch == null) {
            return new Scratch();
        }
        SCRATCH_POOLED.decrementAndGet();
        return scratch;
    }

    private static void releaseScratch(Scratch scratch) {
        if (SCRATCH_POOLED.incrementAndGet() <= POOLED_SCRATCH) {
            SCRATCH.offer(scratch);
        }
        else {
            SCRATCH_POOLED.decrementAndGet();
        }
    }

    /** The working arrays of one decode, grown only when a body needs more. */
    private static final class Scratch {
        private long[] rowIds = new long[0];
        private long[][] planes = new long[0][];
        private boolean[][] present = new boolean[0][];
        private int[][] offsets = new int[0][];
        private int[][] lengths = new int[0][];
        private int[] accepted = new int[0];
        private long[] rowValues = new long[0];
        private boolean[] rowPresent = new boolean[0];
        private int rows;
        private int payloadEnd;

        private Scratch prepare(int columnCount, int rowCount) {
            if (rowCount > rows) {
                rows = rowCount;
                rowIds = new long[rowCount];
                accepted = new int[rowCount];
                planes = new long[0][];
                present = new boolean[0][];
                offsets = new int[0][];
                lengths = new int[0][];
            }
            if (columnCount > planes.length) {
                long[][] grownPlanes = new long[columnCount][];
                boolean[][] grownPresent = new boolean[columnCount][];
                int[][] grownOffsets = new int[columnCount][];
                int[][] grownLengths = new int[columnCount][];
                for (int column = 0; column < columnCount; column++) {
                    grownPlanes[column] = column < planes.length ? planes[column] : new long[rows];
                    grownPresent[column] = column < present.length ? present[column] : new boolean[rows];
                    grownOffsets[column] = column < offsets.length ? offsets[column] : new int[rows];
                    grownLengths[column] = column < lengths.length ? lengths[column] : new int[rows];
                }
                planes = grownPlanes;
                present = grownPresent;
                offsets = grownOffsets;
                lengths = grownLengths;
            }
            if (rowValues.length < columnCount) {
                rowValues = new long[columnCount];
                rowPresent = new boolean[columnCount];
            }
            return this;
        }
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

    /** Reader for the headers {@link Output} writes. */
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
     * @param frames
     *            a freshly encoded segment
     * @return the individual payload values
     */
    public static List<byte[]> payloadSamples(Frames frames) {
        Rows rows = decode(frames.getDirectory(), frames.getUnits(), frames.getPayload(), null);
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
