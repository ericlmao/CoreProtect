package net.coreprotect.database;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.coreprotect.config.ConfigHandler;

/**
 * Holds the blobs of consecutive rows together, compressed as one frame per small group.
 *
 * <p>
 * Entity data is read a row at a time by row id, so it cannot be packed into the segments the rest of
 * the data uses: unpacking tens of thousands of rows to show one entity would cost far more than the
 * space it saves. Compressed one blob at a time it barely shrinks, because what repeats in this data
 * repeats between rows rather than within any one of them.
 * </p>
 *
 * <p>
 * Groups are the middle ground, and the measurements are lopsided enough to make the choice easy.
 * With a trained dictionary, one blob to a frame is worth about forty times; sixty four blobs to a
 * frame is worth about a hundred and thirty five times and takes six microseconds to unpack, which is
 * less than the cost of fetching the row that asked for it. Larger groups keep gaining size slowly
 * and losing speed quickly, so this stops where the curve bends.
 * </p>
 *
 * <p>
 * Nothing is searched for. A row's group is {@code (rowid / 64) * 64}, which is also the group's key,
 * so finding it is one seek on the primary key; and the offset of a blob inside the group is the sum
 * of the lengths before it, which are stored as a short list of varints beside the data. Both are
 * arithmetic on numbers already in hand.
 * </p>
 */
public final class ColdBlobStore {

    /** Blobs per group. Where the gain in size stops being worth the cost of unpacking. */
    static final int GROUP_ROWS = 64;

    /** Decoded groups kept to hand, in bytes. A group of entity data unpacks to roughly 128 KB. */
    private static final long CACHE_BYTES = 32L * 1024 * 1024;

    /** Table ids, matching the segment table ids where they overlap and continuing past them. */
    private static final Map<String, Integer> TABLES = tables();

    private static final Map<Long, byte[]> CACHE = new LinkedHashMap<Long, byte[]>(64, 0.75f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, byte[]> eldest) {
            if (cachedBytes <= CACHE_BYTES) {
                return false;
            }
            cachedBytes = cachedBytes - eldest.getValue().length;
            return true;
        }
    };

    private static long cachedBytes;

    private ColdBlobStore() {
        throw new IllegalStateException("Store class");
    }

    private static Map<String, Integer> tables() {
        Map<String, Integer> tables = new HashMap<>();
        tables.put("entity", 0);
        tables.put("entity_spawn", 1);
        return tables;
    }

    /**
     * @param table
     *            an unprefixed table name
     * @return the id groups of that table are stored under, or null when it has none
     */
    static Integer tableId(String table) {
        return TABLES.get(table);
    }

    /**
     * @param rowId
     *            a row id
     * @return the row id the group holding it begins at
     */
    static long groupOf(long rowId) {
        return (rowId / GROUP_ROWS) * GROUP_ROWS;
    }

    /**
     * Forgets the decoded groups, which is required when the database is reopened.
     */
    public static void clearCache() {
        synchronized (CACHE) {
            CACHE.clear();
            cachedBytes = 0;
        }
    }

    /**
     * Packs a run of rows into one group.
     *
     * @param blobs
     *            the blobs by row id; row ids outside the group are ignored and missing ones are
     *            recorded as empty so that what follows them still lands at the right offset
     * @param firstRowId
     *            the row id the group begins at
     * @return the packed group, ready to be compressed and stored
     */
    static Group pack(Map<Long, byte[]> blobs, long firstRowId) {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        boolean any = false;

        // The lengths go at the head of the same frame as the blobs rather than beside it, so that
        // they are compressed along with everything else. Kept out, they cost a byte or two a row of
        // plain storage, which on data that compresses this well is a noticeable share of the total.
        for (int index = 0; index < GROUP_ROWS; index++) {
            byte[] blob = blobs.get(firstRowId + index);
            int length = blob == null ? 0 : blob.length;
            writeVarint(frame, length);
            if (length > 0) {
                body.write(blob, 0, length);
                any = true;
            }
        }

        if (!any) {
            return null;
        }
        byte[] bytes = body.toByteArray();
        frame.write(bytes, 0, bytes.length);
        return new Group(firstRowId, frame.toByteArray());
    }

    /**
     * Pulls one blob out of a decoded group.
     *
     * @param sizes
     *            the group's length list
     * @param payload
     *            the group's decoded contents
     * @param firstRowId
     *            the row id the group begins at
     * @param rowId
     *            the row wanted
     * @return the blob, or null when that row has none
     */
    static byte[] extract(byte[] sizes, byte[] payload, long firstRowId, long rowId) {
        int wanted = (int) (rowId - firstRowId);
        if (wanted < 0 || wanted >= GROUP_ROWS) {
            return null;
        }

        // The lengths are at the head of the frame, unless the group was written when they were kept
        // in a column of their own, in which case they are there. Nothing in that column means the
        // frame carries them; a real list is sixty four lengths and can never be empty.
        boolean inFrame = sizes == null || sizes.length == 0;
        byte[] lengths = inFrame ? payload : sizes;
        int[] cursor = { 0 };
        int offset = 0;
        int length = 0;
        for (int index = 0; index < GROUP_ROWS; index++) {
            int current = readVarint(lengths, cursor);
            if (index < wanted) {
                offset = offset + current;
            }
            else if (index == wanted) {
                length = current;
            }
        }

        // Where the blobs start: after the lengths when they share the frame, at the beginning when
        // they do not.
        int start = (inFrame ? cursor[0] : 0) + offset;
        if (length == 0 || start + length > payload.length) {
            return null;
        }
        byte[] blob = new byte[length];
        System.arraycopy(payload, start, blob, 0, length);
        return blob;
    }

    /**
     * Reads blobs that have been packed away.
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param rowIds
     *            the rows wanted
     * @return the blobs found, by row id
     * @throws SQLException
     *             if the groups cannot be read
     */
    public static Map<Long, byte[]> load(Connection connection, String table, Collection<Long> rowIds) throws SQLException {
        Map<Long, byte[]> found = new HashMap<>();
        Integer tableId = TABLES.get(table);
        if (tableId == null || rowIds.isEmpty()) {
            return found;
        }

        // Grouped by the run they sit in, so a page of results that came from nearby rows unpacks
        // each run once however many rows of it are wanted.
        Map<Long, List<Long>> byGroup = new TreeMap<>();
        for (Long rowId : rowIds) {
            byGroup.computeIfAbsent(groupOf(rowId), key -> new ArrayList<>()).add(rowId);
        }

        String query = "SELECT sizes, data, raw_size, dict_id FROM " + ConfigHandler.prefix + "blob_group WHERE table_id = ? AND first_rowid = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            for (Map.Entry<Long, List<Long>> entry : byGroup.entrySet()) {
                long firstRowId = entry.getKey();
                byte[] sizes;
                byte[] payload;

                byte[] cached = cached(tableId, firstRowId);
                statement.setInt(1, tableId);
                statement.setLong(2, firstRowId);
                try (ResultSet results = statement.executeQuery()) {
                    if (!results.next()) {
                        continue;
                    }
                    sizes = results.getBytes(1);
                    if (cached != null) {
                        payload = cached;
                    }
                    else {
                        payload = SegmentDictionary.decompress(results.getBytes(2), results.getInt(3), results.getInt(4), connection);
                        cache(tableId, firstRowId, payload);
                    }
                }

                for (Long rowId : entry.getValue()) {
                    byte[] blob = extract(sizes, payload, firstRowId, rowId);
                    if (blob != null) {
                        found.put(rowId, blob);
                    }
                }
            }
        }

        return found;
    }

    /**
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param rowId
     *            the row wanted
     * @return the blob, or null when it is not packed away
     * @throws SQLException
     *             if the group cannot be read
     */
    public static byte[] load(Connection connection, String table, long rowId) throws SQLException {
        return load(connection, table, java.util.Collections.singletonList(rowId)).get(rowId);
    }

    private static byte[] cached(int tableId, long firstRowId) {
        synchronized (CACHE) {
            return CACHE.get(key(tableId, firstRowId));
        }
    }

    private static void cache(int tableId, long firstRowId, byte[] payload) {
        synchronized (CACHE) {
            byte[] previous = CACHE.put(key(tableId, firstRowId), payload);
            cachedBytes = cachedBytes + payload.length - (previous == null ? 0 : previous.length);
        }
    }

    private static long key(int tableId, long firstRowId) {
        return (firstRowId * 8L) + tableId;
    }

    /** One group of blobs, their lengths at its head, packed but not yet compressed. */
    static final class Group {
        final long firstRowId;
        final byte[] frame;

        private Group(long firstRowId, byte[] frame) {
            this.firstRowId = firstRowId;
            this.frame = frame;
        }
    }

    private static void writeVarint(ByteArrayOutputStream out, int value) {
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            out.write((remaining & 0x7F) | 0x80);
            remaining = remaining >>> 7;
        }
        out.write(remaining);
    }

    private static int readVarint(byte[] data, int[] cursor) {
        int value = 0;
        int shift = 0;
        while (cursor[0] < data.length) {
            int current = data[cursor[0]++] & 0xFF;
            value = value | ((current & 0x7F) << shift);
            if ((current & 0x80) == 0) {
                return value;
            }
            shift = shift + 7;
        }
        return value;
    }
}
