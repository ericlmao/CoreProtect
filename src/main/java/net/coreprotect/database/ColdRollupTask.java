package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.serialize.BlobCompression;

/**
 * Packs logged activity out of the live tables and into compressed segments.
 *
 * <p>
 * There is no split between recent and old data. The live tables are only where rows wait until
 * there are enough of them to pack: a segment is written as soon as a day has logged a segment's
 * worth of rows, and whatever a day logged beyond its last full segment is packed once the day is
 * over. Lookups read the live rows and the segments together, so where a row happens to be is never
 * visible to anyone.
 * </p>
 *
 * <p>
 * Rows are sealed oldest first, in blocks of consecutive row ids that never span a day. Each block
 * is encoded and compressed outside any transaction, then written and removed from the live table
 * in a single short transaction, so an interrupted roll-up either leaves the rows where they were or
 * moves them completely. The live table shrinks by deleting a contiguous range of rows; the database
 * file is never rewritten or copied.
 * </p>
 */
public final class ColdRollupTask {

    /**
     * Segments read ahead of the one being written, so that several can be encoded and compressed at
     * once. Held down because a segment's rows are in memory until it is written.
     */
    private static final int READ_AHEAD_SEGMENTS = 4;

    /** Rows per segment. Small enough to decode quickly, large enough to compress well. */
    static final int SEGMENT_ROWS = 65536;

    /**
     * The rows per segment packing actually uses: {@link #SEGMENT_ROWS}, unless a test or benchmark
     * has lowered it to get many segments out of a manageable number of rows.
     */
    private static volatile int segmentRows = SEGMENT_ROWS;

    private static final long SECONDS_PER_DAY = 86400L;

    /** Payload bytes collected before a dictionary is trained for a table. */
    private static final int TRAINING_TARGET_BYTES = 8 * 1024 * 1024;

    /**
     * How long after midnight a day is still treated as open. Activity is stamped when it happens
     * and written a moment later, so the last rows of a day can arrive just after it ends; packing
     * the day's tail a little later keeps them in the same segment rather than a segment of their
     * own. Nothing breaks if one arrives later still: it is packed on its own.
     */
    static final long DAY_CLOSE_GRACE_SECONDS = 600L;

    /** The longest a segment waits for lookups in progress before trying again later. */
    private static final long SEAL_WAIT_MILLIS = 2000L;

    /**
     * The longest a segment waits for lookups while it holds the database's write lock. Most of the
     * waiting is done before that lock is taken, so that logging is not held up by a lookup.
     */
    private static final long SEAL_LOCKED_WAIT_MILLIS = 250L;

    /**
     * Only one roll-up runs at a time, whoever started it. Two at once would read the same rows and
     * write them into two segments. Rewriting segments, and compacting them, take it too, so none
     * of the three ever works on a segment another is replacing.
     */
    static final ReentrantLock SEALING = new ReentrantLock();

    /** Tables whose lookups are about a player rather than a place. */
    private static final Set<String> PLAYER_ORDERED = new java.util.HashSet<>(Arrays.asList("chat", "command", "session"));

    /** Thrown when a segment could not be committed yet because lookups were still reading. */
    static final class SealDeferred extends SQLException {
        private static final long serialVersionUID = 1L;

        SealDeferred() {
            super("Packing deferred while lookups finish");
        }
    }

    private ColdRollupTask() {
        throw new IllegalStateException("Task class");
    }

    /**
     * @return the rows per segment packing writes
     */
    static int segmentRows() {
        return segmentRows;
    }

    /**
     * Changes how many rows packing puts in a segment. Tests and benchmarks lower it to get the
     * hundreds of segments a busy day produces out of a number of rows they can write quickly.
     *
     * @param rows
     *            rows per segment, or 0 to go back to {@link #SEGMENT_ROWS}
     */
    static void setSegmentRowsForTesting(int rows) {
        segmentRows = rows <= 0 ? SEGMENT_ROWS : rows;
    }

    /** Called between segments so a roll-up can stop when the database is needed elsewhere. */
    @FunctionalInterface
    public interface Callback {
        void beforeSegment() throws Exception;
    }

    /**
     * Packs everything that is ready to be packed: every full segment's worth of rows, and the rest
     * of every day that is over.
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment; throwing stops the roll-up
     * @return the number of rows moved into segments
     * @throws Exception
     *             if the roll-up fails or is stopped
     */
    public static long rollUp(Connection connection, Callback callback) throws Exception {
        SEALING.lockInterruptibly();
        try {
            return rollUpTables(connection, callback, Long.MAX_VALUE, Long.MAX_VALUE, openDay());
        }
        finally {
            SEALING.unlock();
        }
    }

    /**
     * Packs whatever is ready, unless another roll-up is already running, in which case that one
     * packs it instead. Used by the packing that runs continuously in the background.
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment; throwing stops the roll-up
     * @return the number of rows moved into segments, or 0 when another roll-up was running
     * @throws Exception
     *             if the roll-up fails or is stopped
     */
    public static long rollUpIfIdle(Connection connection, Callback callback) throws Exception {
        if (!SEALING.tryLock(0, TimeUnit.MILLISECONDS)) {
            return 0;
        }
        try {
            return rollUpTables(connection, callback, Long.MAX_VALUE, Long.MAX_VALUE, openDay());
        }
        finally {
            SEALING.unlock();
        }
    }

    /**
     * Tells cheaply whether anything is ready to be packed, so the background packing can look often
     * without reading rows it would only put back.
     *
     * @param connection
     *            an open connection
     * @return true if at least one table holds a full segment's worth of rows, or rows of a day
     *         that is over
     * @throws SQLException
     *             if the database cannot be read
     */
    public static boolean hasWork(Connection connection) throws SQLException {
        SQLiteColdIndex.ensureLoaded(connection);
        long openDay = openDay();
        for (String table : SQLiteColdIndex.getSegmentedTables()) {
            if (SQLiteColdIndex.layout(connection, table) == null) {
                continue;
            }
            long cursor = SQLiteColdIndex.coldHighWaterMark(table);
            long ceiling = liveMaximumRowId(connection, table) - 1;
            if (ceiling <= cursor) {
                continue;
            }

            try (PreparedStatement statement = connection.prepareStatement("SELECT time FROM " + ConfigHandler.prefix + table + " WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT 1")) {
                statement.setLong(1, cursor);
                statement.setLong(2, ceiling);
                try (ResultSet results = statement.executeQuery()) {
                    if (results.next() && results.getLong(1) / SECONDS_PER_DAY < openDay) {
                        return true;
                    }
                }
            }

            // Rows of two days within one block's reach: the block ends at the day change, and is
            // packed however few rows it holds.
            try (PreparedStatement statement = connection.prepareStatement("SELECT MIN(time / " + SECONDS_PER_DAY + "), MAX(time / " + SECONDS_PER_DAY + ") FROM (SELECT time FROM "
                    + ConfigHandler.prefix + table + " WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT " + segmentRows + ")")) {
                statement.setLong(1, cursor);
                statement.setLong(2, ceiling);
                try (ResultSet results = statement.executeQuery()) {
                    if (results.next() && results.getLong(1) != results.getLong(2)) {
                        return true;
                    }
                }
            }

            // Row ids are handed out one after another, so a span narrower than a segment cannot
            // hold one, and the table does not have to be walked to find that out.
            if (ceiling - cursor < segmentRows) {
                continue;
            }
            try (PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM " + ConfigHandler.prefix + table + " WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT 1 OFFSET " + (segmentRows - 1))) {
                statement.setLong(1, cursor);
                statement.setLong(2, ceiling);
                try (ResultSet results = statement.executeQuery()) {
                    if (results.next()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * @return the first day that may still receive rows, and so only has full segments packed
     */
    static long openDay() {
        long now = System.currentTimeMillis() / 1000L;
        return (now - DAY_CLOSE_GRACE_SECONDS) / SECONDS_PER_DAY;
    }

    /**
     * Seals every row older than an explicit cutoff, including the unfinished tail of the current
     * day. Used by the manual compact command, where the operator has asked for everything logged
     * so far to be packed now.
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment; throwing stops the roll-up
     * @param sealBefore
     *            rows with a timestamp below this are packed
     * @return the number of rows moved into segments
     * @throws Exception
     *             if the roll-up fails or is stopped
     */
    public static long rollUp(Connection connection, Callback callback, long sealBefore) throws Exception {
        return rollUp(connection, callback, sealBefore, Long.MAX_VALUE);
    }

    /**
     * Seals every row older than an explicit cutoff, ignoring rows above a row id.
     *
     * <p>
     * The row id ceiling exists for the legacy import, which fills the live tables from the bottom
     * up while the server is logging above it. Sealing has to stay behind the point the import has
     * reached: rows that have not been copied yet are not there to be sealed, and the topmost row of
     * each table is holding the row id space open so live writes land above the imported range. If
     * that row were sealed it would be deleted from the live table and its row id handed out again.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment; throwing stops the roll-up
     * @param sealBefore
     *            rows with a timestamp below this are packed
     * @param maxRowId
     *            rows above this row id are left alone
     * @return the number of rows moved into segments
     * @throws Exception
     *             if the roll-up fails or is stopped
     */
    public static long rollUp(Connection connection, Callback callback, long sealBefore, long maxRowId) throws Exception {
        SEALING.lockInterruptibly();
        try {
            return rollUpTables(connection, callback, sealBefore, maxRowId, Long.MAX_VALUE);
        }
        finally {
            SEALING.unlock();
        }
    }

    private static long rollUpTables(Connection connection, Callback callback, long sealBefore, long maxRowId, long openDay) throws Exception {
        long sealed = 0;

        for (String table : SQLiteColdIndex.getSegmentedTables()) {
            sealed = sealed + rollUpTable(connection, table, sealBefore, maxRowId, openDay, callback);
        }

        if (sealed > 0) {
            try (Statement statement = connection.createStatement()) {
                // Hand the pages freed by the deletes back to the file system, a little at a time.
                statement.executeUpdate("PRAGMA incremental_vacuum(4000)");
            }
        }

        return sealed;
    }

    /**
     * Rewrites segments written in an older format into the current one.
     *
     * <p>
     * The current format keeps each segment's rows ordered by place, in groups whose ranges let a
     * lookup about one place decode a small part of a segment, and stores item and block data plain so
     * the segment compresses it as a whole. Measured on production segments that halves the space the
     * busiest table takes and makes a lookup about one place several times faster. The rows, their
     * ids and everything recorded about the segment stay exactly as they were; only how they are
     * encoded changes. Each segment is rewritten in its own short transaction, checked before it is
     * written, and a segment that is being rewritten reads the same either way.
     * </p>
     *
     * <p>
     * Segments of the grouped format are rewritten too, so that each of their groups lists the
     * chunks it lies in and the segment records its rows per chunk; a lookup about one place then
     * skips every segment and every group that holds nothing there. Their rows are already in the
     * current order, so only the directory of the scalar frame grows.
     * </p>
     *
     * <p>
     * Segments of version 3 already hold everything the current format does, with their unit
     * bodies inside the scalar frame. They are split rather than rewritten: the directory stays in
     * the segment row and each body moves, byte for byte, into a unit row of its own, so a lookup
     * can read the units it needs without the rest. Nothing is decoded or compressed again, and
     * the payload is left as it is.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment; throwing stops the rewrite
     * @param limit
     *            the most segments to rewrite, or 0 for all of them
     * @return the number of segments rewritten
     * @throws Exception
     *             if the rewrite fails or is stopped
     */
    public static long repackOldSegments(Connection connection, Callback callback, int limit) throws Exception {
        if (!SEALING.tryLock(0, TimeUnit.MILLISECONDS)) {
            return 0;
        }
        try {
            SQLiteColdIndex.ensureLoaded(connection);
            List<Long> pending = new ArrayList<>();
            String setAside = SegmentQuarantine.sqlList();
            try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM " + ConfigHandler.prefix + "segment WHERE codec_version < ?"
                    + (setAside == null ? "" : " AND id NOT IN (" + setAside + ")") + " ORDER BY id" + (limit > 0 ? " LIMIT " + limit : ""))) {
                statement.setInt(1, ColdSegmentCodec.CODEC_VERSION);
                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        pending.add(results.getLong(1));
                    }
                }
            }

            Map<Integer, String> tables = new HashMap<>();
            for (Map.Entry<String, Integer> entry : SQLiteColdIndex.tableIds().entrySet()) {
                tables.put(entry.getValue(), entry.getKey());
            }

            long rewritten = 0;
            List<Long> refreshed = new ArrayList<>();
            try {
                for (Long id : pending) {
                    callback.beforeSegment();
                    CompactProgress.set("updating compressed storage", rewritten, pending.size());
                    SQLiteColdIndex.ColdSegment segment = SQLiteColdIndex.segmentById(connection, id);
                    String table = segment == null ? null : tables.get(segment.getTableId());
                    SQLiteColdIndex.TableLayout layout = table == null ? null : SQLiteColdIndex.layout(connection, table);
                    if (layout == null) {
                        continue;
                    }
                    try {
                        if (repackSegment(connection, table, layout, segment)) {
                            rewritten++;
                            refreshed.add(id);
                        }
                    }
                    catch (SealDeferred deferred) {
                        // Lookups were still reading. Nothing was changed, and the rest is rewritten
                        // on a later run.
                        SQLiteColdIndex.debug("repack: deferred while lookups finish");
                        break;
                    }
                    catch (Exception failure) {
                        if (!SegmentQuarantine.concernsSegment(failure)) {
                            throw failure;
                        }
                        // This segment could not be rewritten, and would fail the same way on every
                        // pass. It is left as it is and the rest are rewritten.
                        SegmentQuarantine.add(java.util.Collections.singletonList(id), "updating the format", failure);
                    }
                }
            }
            finally {
                // The rewritten segments now record their rows per chunk, which lookups can only use
                // once the metadata in memory says so; that holds even when the rewrite was stopped.
                try {
                    SQLiteColdIndex.refreshSegments(connection, refreshed);
                }
                catch (SQLException failure) {
                    // Whatever stopped the rewrite is the failure worth reporting. The metadata is
                    // forgotten instead, and the next lookup reads all of it afresh.
                    SQLiteColdIndex.invalidate();
                }
            }
            return rewritten;
        }
        finally {
            SEALING.unlock();
        }
    }

    private static boolean repackSegment(Connection connection, String table, SQLiteColdIndex.TableLayout layout, SQLiteColdIndex.ColdSegment segment) throws SQLException {
        byte[] stored = storedScalars(connection, segment);
        if (stored == null) {
            return false;
        }
        if (ColdSegmentCodec.isSelfContained(stored) && stored[2] == ColdSegmentCodec.VERSION_CHUNKS) {
            return splitSegment(connection, segment, stored);
        }

        ColdSegmentCodec.Rows decoded = SQLiteColdIndex.readRows(connection, segment);
        List<Long> rowIds = new ArrayList<>(decoded.size());
        List<Object[]> rows = new ArrayList<>(decoded.size());
        for (int row = 0; row < decoded.size(); row++) {
            Object[] values = decoded.getValues(row);
            for (int column = 0; column < values.length; column++) {
                if (values[column] instanceof byte[]) {
                    values[column] = unpackBlob((byte[]) values[column]);
                }
            }
            rowIds.add(decoded.getRowId(row));
            rows.add(values);
        }
        if (rowIds.size() != segment.getRowCount()) {
            throw new SQLException("Segment " + segment.getId() + " holds " + rowIds.size() + " rows but records " + segment.getRowCount());
        }

        orderForLookup(table, layout, rowIds, rows);
        long[] ids = toArray(rowIds);
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(layout.types, ids, rows, layout.chunkColumns());

        // The same rows, under the same ids, or nothing is written.
        ColdSegmentCodec.Rows check = ColdSegmentCodec.decode(frames.getDirectory(), frames.getUnits(), frames.getPayload(), null);
        if (check.size() != ids.length) {
            throw new SQLException("Rewritten segment " + segment.getId() + " holds " + check.size() + " rows instead of " + ids.length);
        }
        for (int row = 0; row < ids.length; row++) {
            if (check.getRowId(row) != ids[row]) {
                throw new SQLException("Rewritten segment " + segment.getId() + " does not hold the rows it was made from");
            }
        }

        int tableId = segment.getTableId();
        int dictionaryId = SegmentDictionary.currentDictionary(connection, tableId);
        SegmentDictionary.warm(dictionaryId, connection);
        byte[] payload = frames.getPayload().length == 0 ? null : SegmentDictionary.compressWith(frames.getPayload(), dictionaryId);

        int[] updated = new int[1];
        rewriteInPlace(connection, segment, () -> {
            try (PreparedStatement statement = connection.prepareStatement("UPDATE " + ConfigHandler.prefix
                    + "segment SET scalars = ?, scalars_size = ?, payload = ?, payload_size = ?, dict_id = ?, codec_version = ?, chunk_counts = ? WHERE id = ? AND start_rowid = ? AND codec_version < ?")) {
                statement.setBytes(1, frames.getDirectory());
                statement.setInt(2, frames.getDirectory().length);
                statement.setBytes(3, payload);
                statement.setInt(4, frames.getPayload().length);
                statement.setInt(5, dictionaryId);
                statement.setInt(6, ColdSegmentCodec.CODEC_VERSION);
                statement.setBytes(7, frames.getChunkCounts());
                statement.setLong(8, segment.getId());
                statement.setLong(9, segment.getStartRowId());
                statement.setInt(10, ColdSegmentCodec.CODEC_VERSION);
                updated[0] = statement.executeUpdate();
            }
            if (updated[0] == 1) {
                writeUnits(connection, segment.getId(), frames.getUnits(), null);
            }
            return updated[0] == 1;
        });
        return updated[0] == 1;
    }

    /**
     * Rewrites a segment where it is stored, without any lookup straddling the change.
     *
     * <p>
     * A lookup reads a segment's directory in one statement and its units in others, and holds the
     * directory in memory for the lookups after it. Were a segment rewritten between the two, a
     * lookup would pair the old directory with units of the new encoding, which describe other
     * bytes. So the rewrite commits only while no lookup is running, the way packing commits a new
     * segment, and whatever was held in memory of the old encoding is let go of before any lookup
     * can start again.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param segment
     *            the segment being rewritten
     * @param write
     *            the writes, run inside the transaction; returns true if it changed the segment
     * @throws SealDeferred
     *             if lookups kept reading for too long, in which case nothing was changed
     * @throws SQLException
     *             if the rewrite fails
     */
    private static void rewriteInPlace(Connection connection, SQLiteColdIndex.ColdSegment segment, InPlaceWrite write) throws SQLException {
        boolean[] locked = new boolean[1];
        awaitLookups();
        try {
            Database.inTransaction(connection, () -> {
                // The update has taken the database's write lock, so no writer can be inside a
                // transaction now, and waiting for lookups here cannot wait on one that waits for us.
                if (write.run()) {
                    holdOffLookups(locked);
                }
            });
            // Whatever a lookup held of the old encoding described the same rows, but the next one
            // reads the new encoding.
            SegmentFrameCache.forget(segment);
        }
        finally {
            if (locked[0]) {
                SQLiteColdIndex.unlockAfterSeal();
            }
        }
    }

    /** The writes of a rewrite in place. */
    @FunctionalInterface
    private interface InPlaceWrite {
        boolean run() throws SQLException;
    }

    /**
     * Keeps lookups from starting until {@link SQLiteColdIndex#unlockAfterSeal()}, waiting a short
     * while for those in progress. Called while the database's write lock is held, so that the wait
     * cannot be on a lookup that is itself waiting for this writer.
     *
     * @param locked
     *            set once the lock is held, so the caller releases it
     * @throws SealDeferred
     *             if lookups are still reading after the wait
     * @throws SQLException
     *             if the wait is interrupted
     */
    static void holdOffLookups(boolean[] locked) throws SQLException {
        try {
            if (!SQLiteColdIndex.lockForSeal(SEAL_LOCKED_WAIT_MILLIS)) {
                throw new SealDeferred();
            }
        }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for lookups to finish", interrupted);
        }
        locked[0] = true;
    }

    /**
     * @return the stored scalar frame of a segment, or null if the segment is gone
     */
    private static byte[] storedScalars(Connection connection, SQLiteColdIndex.ColdSegment segment) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT scalars FROM " + ConfigHandler.prefix + "segment WHERE id = ? AND start_rowid = ?")) {
            statement.setLong(1, segment.getId());
            statement.setLong(2, segment.getStartRowId());
            try (ResultSet results = statement.executeQuery()) {
                return results.next() ? results.getBytes(1) : null;
            }
        }
    }

    /**
     * Moves the unit bodies of a version 3 segment out of its scalar frame into unit rows, leaving
     * the directory in the segment row. The bodies are copied as they are, and the split segment is
     * decoded in full and must hold every row the segment records before anything is written.
     */
    private static boolean splitSegment(Connection connection, SQLiteColdIndex.ColdSegment segment, byte[] stored) throws SQLException {
        ColdSegmentCodec.Frames frames;
        int rows;
        try {
            frames = ColdSegmentCodec.split(stored);
            rows = ColdSegmentCodec.count(frames.getDirectory(), frames.getUnits(), null);
        }
        catch (IllegalArgumentException malformed) {
            throw new SQLException("Segment " + segment.getId() + " cannot be split", malformed);
        }
        if (rows != segment.getRowCount()) {
            throw new SQLException("Split segment " + segment.getId() + " holds " + rows + " rows but records " + segment.getRowCount());
        }

        int[] updated = new int[1];
        rewriteInPlace(connection, segment, () -> {
            try (PreparedStatement statement = connection.prepareStatement("UPDATE " + ConfigHandler.prefix
                    + "segment SET scalars = ?, scalars_size = ?, codec_version = ? WHERE id = ? AND start_rowid = ? AND codec_version < ?")) {
                statement.setBytes(1, frames.getDirectory());
                statement.setInt(2, frames.getDirectory().length);
                statement.setInt(3, ColdSegmentCodec.CODEC_VERSION);
                statement.setLong(4, segment.getId());
                statement.setLong(5, segment.getStartRowId());
                statement.setInt(6, ColdSegmentCodec.CODEC_VERSION);
                updated[0] = statement.executeUpdate();
            }
            if (updated[0] == 1) {
                writeUnits(connection, segment.getId(), frames.getUnits(), null);
            }
            return updated[0] == 1;
        });
        return updated[0] == 1;
    }

    /**
     * Records the per player and per block type row counts, and the rows per chunk, of segments
     * that were written before those counts existed.
     *
     * <p>
     * Without them a lookup cannot tell which segments hold a player's rows, so it has to open all
     * of them whenever a result is smaller than a page, and a lookup about one place has to open
     * every segment its chunk filter cannot rule out. Each segment's scalar frame is read once here,
     * whatever format it was written in, and the counts are written back; nothing else about the
     * segment changes. Only the numbers are read: the payload frame is never decompressed.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked before each segment; throwing stops the backfill
     * @return the number of segments that gained counts
     * @throws Exception
     *             if the backfill fails or is stopped
     */
    public static long backfillStatistics(Connection connection, Callback callback) throws Exception {
        List<Long> pending = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM " + ConfigHandler.prefix + "segment WHERE user_stats IS NULL OR action_stats IS NULL OR chunk_counts IS NULL"
                        + " OR (spawn_filter IS NULL AND table_id IN(" + SQLiteColdIndex.entityKeyedTableIds() + ")) ORDER BY id");
                ResultSet results = statement.executeQuery()) {
            while (results.next()) {
                pending.add(results.getLong(1));
            }
        }

        if (pending.isEmpty()) {
            return 0;
        }

        Map<Integer, SQLiteColdIndex.TableLayout> layouts = new HashMap<>();
        Map<Integer, String> tables = new HashMap<>();
        for (Map.Entry<String, Integer> entry : SQLiteColdIndex.tableIds().entrySet()) {
            tables.put(entry.getValue(), entry.getKey());
        }

        long updated = 0;
        long examined = 0;
        String update = "UPDATE " + ConfigHandler.prefix + "segment SET user_stats = ?, type_stats = ?, action_stats = ?, spawn_filter = ?, chunk_counts = ? WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(update)) {
            for (Long id : pending) {
                callback.beforeSegment();
                CompactProgress.set("checking compressed storage", ++examined, pending.size());

                SQLiteColdIndex.ColdSegment segment = SQLiteColdIndex.segmentById(connection, id);
                if (segment == null) {
                    continue;
                }

                String table = tables.get(segment.getTableId());
                if (table == null) {
                    continue;
                }

                SQLiteColdIndex.TableLayout layout = layouts.computeIfAbsent(segment.getTableId(), key -> {
                    try {
                        return SQLiteColdIndex.layout(connection, table);
                    }
                    catch (SQLException exception) {
                        return null;
                    }
                });
                if (layout == null) {
                    continue;
                }

                StatisticsTally tally = new StatisticsTally(layout);
                SQLiteColdIndex.scanRows(connection, segment, tally);
                if (tally.rows != segment.getRowCount()) {
                    throw new SQLException("Segment " + segment.getId() + " holds " + tally.rows + " rows but records " + segment.getRowCount());
                }

                statement.setBytes(1, SegmentStatistics.encode(tally.userCounts));
                statement.setBytes(2, SegmentStatistics.encode(tally.typeCounts));
                statement.setBytes(3, SegmentStatistics.encode(tally.actionCounts));
                statement.setBytes(4, tally.spawnColumn < 0 ? null : SegmentMembership.encode(toArray(tally.spawnIds)));
                statement.setBytes(5, tally.chunkCounts());
                statement.setLong(6, id);
                statement.executeUpdate();
                updated++;
            }
        }

        if (updated > 0) {
            SQLiteColdIndex.reload(connection);
        }
        return updated;
    }

    /**
     * Collects the counts a segment records about itself from the plain numbers of each of its
     * rows, as the decoder hands them over, building no row and so never needing the payload.
     */
    private static final class StatisticsTally implements ColdSegmentCodec.RowFilter {
        private final int userColumn;
        private final int typeColumn;
        private final int actionColumn;
        private final int spawnColumn;
        private final int[] location;
        private final Map<Long, Integer> userCounts = new HashMap<>();
        private final Map<Long, Integer> typeCounts = new HashMap<>();
        private final Map<Long, Integer> actionCounts = new HashMap<>();
        private final Set<Long> spawnIds = new LinkedHashSet<>();
        private final ColdSegmentCodec.ChunkTally chunks = new ColdSegmentCodec.ChunkTally(Integer.MAX_VALUE);
        private int rows;

        private StatisticsTally(SQLiteColdIndex.TableLayout layout) {
            this.userColumn = columnIndex(layout, "user");
            this.typeColumn = columnIndex(layout, "type");
            this.actionColumn = columnIndex(layout, "action");
            this.spawnColumn = columnIndex(layout, "entity_spawn_rowid");
            this.location = layout.chunkColumns();
        }

        @Override
        public boolean accept(long[] columns, boolean[] present) {
            rows++;
            if (spawnColumn >= 0 && present[spawnColumn]) {
                spawnIds.add(columns[spawnColumn]);
            }
            if (userColumn >= 0 && present[userColumn] && userCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                userCounts.merge(columns[userColumn], 1, Integer::sum);
            }
            if (typeColumn >= 0 && present[typeColumn] && typeCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                typeCounts.merge(columns[typeColumn], 1, Integer::sum);
            }
            if (actionColumn >= 0 && present[actionColumn] && actionCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                actionCounts.merge(columns[actionColumn], 1, Integer::sum);
            }
            if (location != null) {
                boolean located = present[location[0]] && present[location[1]] && present[location[2]];
                chunks.add(located, columns[location[0]], columns[location[1]], columns[location[2]]);
            }
            return false;
        }

        /**
         * @return the rows per chunk in the form stored beside the segment; a table whose rows have
         *         no place records every row as unlocated, so the segment is not counted again
         */
        private byte[] chunkCounts() {
            if (location == null) {
                return ColdSegmentCodec.encodeChunkCounts(new ColdSegmentCodec.ChunkList(new int[0], new int[0], new int[0], new int[0], rows));
            }
            return ColdSegmentCodec.encodeChunkCounts(chunks.toList());
        }
    }

    /**
     * Seals one table's rows, up to a cutoff and a row id ceiling.
     *
     * <p>
     * The legacy import fills one table at a time, so it seals one table at a time rather than
     * sweeping all of them after every batch. The row id ceiling is the point the import has reached
     * in that table.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param maxRowId
     *            rows above this row id are left alone
     * @param callback
     *            invoked before each segment; throwing stops the roll-up
     * @return the number of rows moved into segments
     * @throws Exception
     *             if the roll-up fails or is stopped
     */
    public static long sealTable(Connection connection, String table, long maxRowId, Callback callback) throws Exception {
        long sealed;
        SEALING.lockInterruptibly();
        try {
            sealed = rollUpTable(connection, table, Long.MAX_VALUE, maxRowId, openDay(), callback);
        }
        finally {
            SEALING.unlock();
        }
        if (sealed > 0) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA incremental_vacuum(4000)");
            }
        }
        return sealed;
    }

    private static long rollUpTable(Connection connection, String table, long sealBefore, long maxRowId, long openDay, Callback callback) throws Exception {
        Integer tableId = SQLiteColdIndex.tableId(table);
        if (tableId == null) {
            return 0;
        }

        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, table);
        if (layout == null || layout.timeColumn < 0) {
            return 0;
        }

        long sealed = 0;
        List<byte[]> trainingSamples = new ArrayList<>();
        long trainingBytes = 0;
        int dictionaryId = SegmentDictionary.currentDictionary(connection, tableId);

        // The newest row is never sealed, so the live table never empties. SQLite gives a new row the
        // highest row id in the table plus one, and it only knows about the rows still there: empty
        // the table and the next row written starts again at one, on top of row ids the segments are
        // already using. Everything here relies on a segment's row ids being below the live table's,
        // so one row is left behind to hold the numbering up. It is sealed by a later run, once newer
        // rows have taken its place.
        long ceiling = Math.min(maxRowId, liveMaximumRowId(connection, table) - 1);

        // Where reading has reached, kept here rather than asked of the index each time round, so
        // that the next segments can be read while the last ones are still being encoded.
        long cursor = SQLiteColdIndex.coldHighWaterMark(table);

        while (true) {
            callback.beforeSegment();

            // Until a dictionary has been trained the segments have to be encoded one at a time,
            // since each one contributes to what the dictionary is trained on and the ones after it
            // are compressed against the result.
            int depth = dictionaryId == 0 ? 1 : Math.min(CompactWorkers.threads(), READ_AHEAD_SEGMENTS);
            List<Block> blocks = new ArrayList<>(depth);
            for (int index = 0; index < depth; index++) {
                Block block = readBlock(connection, table, layout, cursor, sealBefore, ceiling);
                if (block == null || block.rowIds.isEmpty()) {
                    break;
                }
                // A block that is neither full nor the end of a day that is over is still filling
                // up. It is left where it is and packed once one or the other is true, which keeps
                // segments full and whole days intact for retention. A block that ended because the
                // row after it belongs to another day can never grow either, and is packed as it is,
                // or nothing behind it could ever be.
                if (block.rowIds.size() < segmentRows && block.minTime / SECONDS_PER_DAY >= openDay && !block.closedByDayChange) {
                    break;
                }
                cursor = block.maxRowId;
                blocks.add(block);
            }

            if (blocks.isEmpty()) {
                break;
            }

            if (dictionaryId == 0 && trainingBytes < TRAINING_TARGET_BYTES) {
                Block block = blocks.get(0);
                ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(layout.types, toArray(block.rowIds), block.rows, layout.chunkColumns());
                if (frames.getPayload().length > 0) {
                    trainingSamples.addAll(ColdSegmentCodec.payloadSamples(frames));
                    trainingBytes = trainingBytes + frames.getPayload().length;
                    if (trainingBytes >= TRAINING_TARGET_BYTES) {
                        dictionaryId = SegmentDictionary.train(connection, tableId, trainingSamples);
                        trainingSamples.clear();
                    }
                }
            }

            // Encoding, checking and compressing a segment is the slow part of sealing and touches
            // nothing but the rows it was handed, so the segments in hand are prepared on as many
            // cores as are spare. They are written afterwards, in order, on this thread, because the
            // database takes one writer and their row ids have to stay in order.
            SegmentDictionary.warm(dictionaryId, connection);
            int dictionary = dictionaryId;
            List<Sealed> prepared;
            try {
                prepared = CompactWorkers.map(blocks, block -> {
                    ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(layout.types, toArray(block.rowIds), block.rows, layout.chunkColumns());
                    verify(frames, layout, block);
                    // The scalar frame compresses each of its units itself, so a lookup can open one
                    // unit without the rest; compressing it again as a whole would undo that.
                    byte[] payload = frames.getPayload().length == 0 ? null : SegmentDictionary.compressWith(frames.getPayload(), dictionary);
                    return new Sealed(block, frames, payload);
                });
            }
            catch (SQLException failure) {
                throw failure;
            }
            catch (RuntimeException failure) {
                throw failure;
            }
            catch (Exception failure) {
                throw new SQLException(failure);
            }

            for (Sealed segment : prepared) {
                try {
                    writeSegment(connection, tableId, table, segment.block, layout, segment.frames, segment.payload, dictionaryId);
                }
                catch (SealDeferred deferred) {
                    // Lookups were still reading. Nothing was changed, and the rows are packed on
                    // a later run.
                    SQLiteColdIndex.debug("pack " + table + ": deferred while lookups finish");
                    return sealed;
                }
                sealed = sealed + segment.block.rowIds.size();
                CompactProgress.set("packing " + table + " into compressed storage", sealed, 0);
            }
        }

        return sealed;
    }

    /** A segment that has been encoded, checked and compressed, and is waiting to be written. */
    private static final class Sealed {
        private final Block block;
        private final ColdSegmentCodec.Frames frames;
        private final byte[] payload;

        private Sealed(Block block, ColdSegmentCodec.Frames frames, byte[] payload) {
            this.block = block;
            this.frames = frames;
            this.payload = payload;
        }
    }

    /**
     * Decodes a freshly encoded segment and checks it against what was read, so no rows are deleted
     * from the live table unless they can be read back.
     */
    private static void verify(ColdSegmentCodec.Frames frames, SQLiteColdIndex.TableLayout layout, Block block) throws SQLException {
        // Decoded from the directory and the unit bodies apart, exactly as they are stored.
        ColdSegmentCodec.Rows rows = ColdSegmentCodec.decode(frames.getDirectory(), frames.getUnits(), frames.getPayload(), null);
        if (rows.size() != block.rowIds.size()) {
            throw new SQLException("Encoded segment holds " + rows.size() + " rows but " + block.rowIds.size() + " were read");
        }
        for (int row = 0; row < rows.size(); row++) {
            if (rows.getRowId(row) != block.rowIds.get(row)) {
                throw new SQLException("Encoded segment does not cover the rows that were read");
            }
        }
        if (layout.columns.length != rows.getValues(0).length) {
            throw new SQLException("Encoded segment has the wrong number of columns");
        }
    }

    private static void writeSegment(Connection connection, int tableId, String table, Block block, SQLiteColdIndex.TableLayout layout, ColdSegmentCodec.Frames frames, byte[] payloadFrame, int dictionaryId) throws SQLException {
        // Started from a known transaction state rather than from whatever the driver believes it
        // to be. The two can come apart, and when they do the writes below go in one at a time and
        // the commit fails with "cannot commit - no transaction is active", taking the compact down.
        boolean[] locked = new boolean[1];
        boolean committed = false;
        awaitLookups();
        try {
            Database.inTransaction(connection, () -> {
                long segmentId = insertSegmentRow(connection, tableId, block, block.minRowId, block.maxRowId, frames.getDirectory(), payloadFrame, frames.getPayload().length,
                        frames.getChunkCounts(), dictionaryId, 0);
                writeUnits(connection, segmentId, frames.getUnits(), null);

                // The insert above has taken the database's write lock, so no writer can be inside a
                // transaction now, and waiting for lookups here cannot wait on one that waits for us.
                // From the moment the rows leave the live table until the index in memory says where
                // they went, no lookup may start, or it would find them in neither place.
                holdOffLookups(locked);

                recordLateRollbackState(connection, tableId, table, block, layout);

                try (PreparedStatement statement = connection.prepareStatement("DELETE FROM " + ConfigHandler.prefix + table + " WHERE rowid BETWEEN ? AND ?")) {
                    statement.setLong(1, block.minRowId);
                    statement.setLong(2, block.maxRowId);
                    int deleted = statement.executeUpdate();
                    if (deleted != block.rowIds.size()) {
                        throw new SQLException("Sealed " + block.rowIds.size() + " rows of " + table + " but removed " + deleted);
                    }
                }
            });
            committed = true;
            SQLiteColdIndex.loadNewSegments(connection, table);
        }
        catch (SQLException failure) {
            if (committed) {
                // The rows have moved but the index does not know where to. Forgetting it makes the
                // next lookup read it afresh rather than miss them.
                SQLiteColdIndex.invalidate();
            }
            throw failure;
        }
        finally {
            if (locked[0]) {
                SQLiteColdIndex.unlockAfterSeal();
            }
        }
    }

    /**
     * Inserts the row describing one segment, with everything recorded about its rows.
     *
     * @param connection
     *            the connection holding the transaction
     * @param tableId
     *            the segment table id
     * @param summary
     *            what the segment's rows hold
     * @param startRowId
     *            the lowest row id the segment claims
     * @param endRowId
     *            the highest row id the segment claims
     * @param directory
     *            the directory of the scalar frame
     * @param payloadFrame
     *            the stored payload frame, or null when there is none or each unit keeps its share
     * @param payloadSize
     *            the size of the payload frame before compression
     * @param chunkCounts
     *            the rows per chunk, in stored form
     * @param dictionaryId
     *            the dictionary the payload was compressed with, or 0
     * @param compacted
     *            the compaction state to record
     * @return the new segment's id
     * @throws SQLException
     *             if the row cannot be written
     */
    static long insertSegmentRow(Connection connection, int tableId, Summary summary, long startRowId, long endRowId, byte[] directory, byte[] payloadFrame, long payloadSize,
            byte[] chunkCounts, int dictionaryId, int compacted) throws SQLException {
        String insert = "INSERT INTO " + ConfigHandler.prefix + "segment (table_id,start_rowid,end_rowid,row_count,min_time,max_time,day,"
                + "wid_set,chunk_filter,user_filter,type_filter,action_bits,dict_id,codec_version,scalars,scalars_size,payload,payload_size,"
                + "user_stats,type_stats,action_stats,spawn_filter,chunk_counts,compacted) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            statement.setInt(1, tableId);
            statement.setLong(2, startRowId);
            statement.setLong(3, endRowId);
            statement.setInt(4, summary.rowCount);
            statement.setLong(5, summary.minTime);
            statement.setLong(6, summary.maxTime);
            statement.setLong(7, summary.minTime / SECONDS_PER_DAY);
            statement.setBytes(8, SQLiteColdIndex.writeWorldIds(new ArrayList<>(summary.worldIds)));
            statement.setBytes(9, SegmentMembership.encode(toArray(summary.chunkKeys), SegmentFilter.CHUNK_BYTES));
            statement.setBytes(10, SegmentMembership.encode(toArray(summary.userIds)));
            statement.setBytes(11, SegmentMembership.encode(toArray(summary.typeIds)));
            statement.setLong(12, summary.actionBits);
            statement.setInt(13, dictionaryId);
            statement.setInt(14, ColdSegmentCodec.CODEC_VERSION);
            statement.setBytes(15, directory);
            statement.setInt(16, directory.length);
            statement.setBytes(17, payloadFrame);
            statement.setLong(18, payloadSize);
            statement.setBytes(19, SegmentStatistics.encode(summary.userCounts));
            statement.setBytes(20, SegmentStatistics.encode(summary.typeCounts));
            statement.setBytes(21, SegmentStatistics.encode(summary.actionCounts));
            statement.setBytes(22, SegmentMembership.encode(toArray(summary.spawnIds)));
            statement.setBytes(23, chunkCounts);
            statement.setInt(24, compacted);
            statement.executeUpdate();
        }
        return insertedId(connection);
    }

    /**
     * @return the id of the row the connection inserted last
     */
    private static long insertedId(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT last_insert_rowid()")) {
            if (!results.next() || results.getLong(1) <= 0) {
                throw new SQLException("The new segment's id could not be read");
            }
            return results.getLong(1);
        }
    }

    /**
     * Stores the unit bodies of a segment, one row each, in place of any stored under its id before.
     * Called inside the transaction that writes the segment row, so a segment and its units are
     * only ever committed together. Nothing should be stored under a new segment's id, but ids can
     * be handed out again, and a stale unit left by a failure elsewhere must never be read as part
     * of the segment that reuses the id.
     *
     * @param connection
     *            the connection holding the transaction
     * @param segmentId
     *            the segment
     * @param units
     *            its unit bodies, by unit number
     * @param payloads
     *            each unit's stored share of the payload, by unit number, for a segment that keeps
     *            them apart; null for one whose payload is in its segment row
     * @throws SQLException
     *             if they cannot be written
     */
    static void writeUnits(Connection connection, long segmentId, byte[][] units, byte[][] payloads) throws SQLException {
        if (payloads != null && payloads.length != units.length) {
            throw new SQLException("Segment " + segmentId + " has " + units.length + " units but " + payloads.length + " payload shares");
        }
        try (PreparedStatement statement = connection.prepareStatement("DELETE FROM " + ConfigHandler.prefix + "segment_unit WHERE segment_id = ?")) {
            statement.setLong(1, segmentId);
            statement.executeUpdate();
        }
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO " + ConfigHandler.prefix + "segment_unit (segment_id,unit,body,payload) VALUES (?,?,?,?)")) {
            for (int unit = 0; unit < units.length; unit++) {
                statement.setLong(1, segmentId);
                statement.setInt(2, unit);
                statement.setBytes(3, units[unit]);
                statement.setBytes(4, payloads == null ? null : payloads[unit]);
                statement.executeUpdate();
            }
        }
    }

    /**
     * Waits, before the database's write lock is taken, for lookups in progress to finish, so that
     * the wait for them inside the transaction is short and logging is not held up by a lookup.
     */
    static void awaitLookups() throws SQLException {
        long deadline = System.nanoTime() + SEAL_WAIT_MILLIS * 1_000_000L;
        while (SQLiteColdIndex.lookupsInProgress() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while waiting for lookups to finish", interrupted);
            }
        }
    }

    /**
     * Carries over rollback state that changed while the block was being encoded.
     *
     * <p>
     * The rows were read outside the transaction, and encoding takes a while. A rollback that ran in
     * between changed the live rows, and what was encoded no longer says so. The segment is never
     * rewritten, so the change is recorded beside it, where a rollback of an already packed row
     * would have recorded it.
     * </p>
     */
    private static void recordLateRollbackState(Connection connection, int tableId, String table, Block block, SQLiteColdIndex.TableLayout layout) throws SQLException {
        if (layout.rolledBackColumn < 0) {
            return;
        }
        Map<Long, Integer> encoded = new HashMap<>(block.rowIds.size() * 2);
        for (int row = 0; row < block.rowIds.size(); row++) {
            Object value = block.rows.get(row)[layout.rolledBackColumn];
            encoded.put(block.rowIds.get(row), value == null ? 0 : ((Number) value).intValue());
        }

        List<long[]> changed = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT rowid, rolled_back FROM " + ConfigHandler.prefix + table + " WHERE rowid BETWEEN ? AND ?")) {
            statement.setLong(1, block.minRowId);
            statement.setLong(2, block.maxRowId);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    long rowId = results.getLong(1);
                    int current = results.getInt(2);
                    Integer was = encoded.get(rowId);
                    if (was != null && was != current) {
                        changed.add(new long[] { rowId, current });
                    }
                }
            }
        }
        if (changed.isEmpty()) {
            return;
        }

        try (PreparedStatement statement = connection.prepareStatement("INSERT OR REPLACE INTO " + ConfigHandler.prefix + "cold_flag (table_id,rowid_ref,rolled_back) VALUES (?,?,?)")) {
            for (long[] change : changed) {
                statement.setInt(1, tableId);
                statement.setLong(2, change[0]);
                statement.setInt(3, (int) change[1]);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        SQLiteColdIndex.debug("pack " + table + ": " + changed.size() + " rows were rolled back while being packed; recorded beside the segment");
    }

    /**
     * What the rows of one segment hold, gathered row by row: the range of their times and row ids,
     * the worlds, chunks, players, block types and entities they mention, and how many rows each
     * player, block type and action accounts for. It is what the segment row records, so packing
     * and compaction record exactly the same things from the same rows.
     *
     * <p>
     * The sets of distinct values are collected in full rather than stopping at the point where they
     * would no longer be listed exactly. What is stored is decided when the segment is written, and
     * deciding it while reading would build a filter that knew about only the values seen up to the
     * cut off: a lookup for any of the rest would be told the segment does not hold them, and its
     * rows would go missing. The sets hold distinct values, so even a segment of a million rows
     * holds a few thousand of each.
     * </p>
     */
    static class Summary {
        final Set<Integer> worldIds = new LinkedHashSet<>();
        final Set<Long> userIds = new LinkedHashSet<>();
        final Set<Long> typeIds = new LinkedHashSet<>();
        final Set<Long> spawnIds = new LinkedHashSet<>();
        final Map<Long, Integer> userCounts = new HashMap<>();
        final Map<Long, Integer> typeCounts = new HashMap<>();
        final Map<Long, Integer> actionCounts = new HashMap<>();
        final Set<Long> chunkKeys = new LinkedHashSet<>();
        long actionBits;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        long minRowId = Long.MAX_VALUE;
        long maxRowId = Long.MIN_VALUE;
        int rowCount;

        /**
         * Counts one row.
         *
         * @param rowId
         *            the row's id
         * @param values
         *            the row's values, in the table's column order
         * @param layout
         *            the table's layout
         */
        void add(long rowId, Object[] values, SQLiteColdIndex.TableLayout layout) {
            rowCount++;
            // A missing time reads as zero, as it does from the live table.
            long time = layout.timeColumn >= 0 && values[layout.timeColumn] != null ? ((Number) values[layout.timeColumn]).longValue() : 0;
            minRowId = Math.min(minRowId, rowId);
            maxRowId = Math.max(maxRowId, rowId);
            minTime = Math.min(minTime, time);
            maxTime = Math.max(maxTime, time);

            if (layout.worldColumn >= 0 && values[layout.worldColumn] != null) {
                int worldId = ((Number) values[layout.worldColumn]).intValue();
                worldIds.add(worldId);
                if (layout.xColumn >= 0 && layout.zColumn >= 0 && values[layout.xColumn] != null && values[layout.zColumn] != null) {
                    int x = ((Number) values[layout.xColumn]).intValue();
                    int z = ((Number) values[layout.zColumn]).intValue();
                    chunkKeys.add(SegmentFilter.chunkKey(worldId, SegmentFilter.chunkOf(x), SegmentFilter.chunkOf(z)));
                }
            }

            if (layout.userColumn >= 0 && values[layout.userColumn] != null) {
                long userId = ((Number) values[layout.userColumn]).longValue();
                userIds.add(userId);
                if (userCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                    userCounts.merge(userId, 1, Integer::sum);
                }
            }

            if (layout.typeColumn >= 0 && values[layout.typeColumn] != null) {
                long typeId = ((Number) values[layout.typeColumn]).longValue();
                typeIds.add(typeId);
                if (typeCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                    typeCounts.merge(typeId, 1, Integer::sum);
                }
            }

            if (layout.spawnColumn >= 0 && values[layout.spawnColumn] != null) {
                spawnIds.add(((Number) values[layout.spawnColumn]).longValue());
            }

            if (layout.actionColumn >= 0 && values[layout.actionColumn] != null) {
                int action = ((Number) values[layout.actionColumn]).intValue();
                if (action >= 0 && action < 64) {
                    actionBits |= (1L << action);
                }
                if (actionCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                    actionCounts.merge((long) action, 1, Integer::sum);
                }
            }
        }
    }

    /** One block of consecutive rows, ready to be encoded. */
    private static final class Block extends Summary {
        private final List<Long> rowIds = new ArrayList<>();
        private final List<Object[]> rows = new ArrayList<>();
        /** Whether the block ended because the next row belongs to a later day. */
        private boolean closedByDayChange;
    }

    /**
     * @return the highest row id the live table holds, or 0 when it holds nothing
     */
    private static long liveMaximumRowId(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(MAX(rowid),0) FROM " + ConfigHandler.prefix + table)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    /**
     * Moves live rows back above the segments when their row ids have run into them.
     *
     * <p>
     * A table that was emptied by an earlier build started numbering again from one, over row ids its
     * segments already hold. Those rows can never be sealed, because sealing only ever looks above
     * the highest row id already sealed, and while they are there a lookup reading the live rows and
     * the segments together sees two different rows under one number. Moving them above everything
     * restores the order the rest of the design depends on.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked per table; throwing stops the repair
     * @return the number of rows moved
     * @throws Exception
     *             if the rows cannot be moved
     */
    public static long repairRowIds(Connection connection, Callback callback) throws Exception {
        // Which row ids the segments hold is read from the index, and the index is empty until it has
        // been read from the database. Without this the repair sees no segments, decides there is
        // nothing underneath them, and quietly does nothing.
        SQLiteColdIndex.reload(connection);

        long moved = 0;
        for (String table : SQLiteColdIndex.getSegmentedTables()) {
            callback.beforeSegment();

            CompactProgress.set("checking row numbering", 0, 0);
            long highWater = SQLiteColdIndex.coldHighWaterMark(table);
            if (highWater == 0) {
                continue;
            }

            long lowest;
            long highest;
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COALESCE(MIN(rowid),0), COALESCE(MAX(rowid),0) FROM " + ConfigHandler.prefix + table)) {
                results.next();
                lowest = results.getLong(1);
                highest = results.getLong(2);
            }
            if (lowest == 0 || lowest > highWater) {
                continue;
            }

            // Above the segments and above anything the table already holds, so nothing is moved onto
            // a row id that is in use.
            long offset = Math.max(highWater, highest) - lowest + 1;
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE " + ConfigHandler.prefix + table + " SET rowid = rowid + ? WHERE rowid <= ?")) {
                statement.setLong(1, offset);
                statement.setLong(2, highWater);
                int updated = statement.executeUpdate();
                moved = moved + updated;
                if (updated > 0) {
                    Chat.console("Moved " + updated + " rows of " + table + " above compressed storage, where their numbering had run into it.");
                }
            }
        }
        return moved;
    }

    private static Block readBlock(Connection connection, String table, SQLiteColdIndex.TableLayout layout, long afterRowId, long sealBefore, long maxRowId) throws SQLException {
        StringBuilder columns = new StringBuilder("rowid");
        for (String column : layout.columns) {
            columns.append(',').append(column);
        }

        // Rows are taken strictly in row id order and the block stops at the first one that is too
        // new. Skipping over it instead would put it inside the range the block deletes.
        String query = "SELECT " + columns + " FROM " + ConfigHandler.prefix + table
                + " WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT " + segmentRows;

        Block block = new Block();
        long day = -1;

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, afterRowId);
            statement.setLong(2, maxRowId);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    long time = results.getLong(layout.timeColumn + 2);
                    if (time >= sealBefore) {
                        break;
                    }
                    long rowDay = time / SECONDS_PER_DAY;
                    if (day < 0) {
                        day = rowDay;
                    }
                    else if (rowDay != day) {
                        // A segment never spans a day, so whole days can be dropped at purge time.
                        block.closedByDayChange = true;
                        break;
                    }

                    Object[] values = new Object[layout.columns.length];
                    for (int column = 0; column < layout.columns.length; column++) {
                        values[column] = readValue(results, column + 2, layout.types[column]);
                    }

                    long rowId = results.getLong(1);
                    block.rowIds.add(rowId);
                    block.rows.add(values);
                    block.add(rowId, values, layout);
                }
            }
        }

        if (block.rowIds.isEmpty()) {
            return null;
        }

        orderForLookup(table, layout, block.rowIds, block.rows);
        return block;
    }

    /**
     * The order the rows of a segment are kept in, so that the rows a lookup asks for together sit
     * together.
     *
     * <p>
     * A segment is decoded a group of rows at a time, and each group records the range of every
     * column it holds, so a lookup skips every group whose range cannot hold what it wants. That only
     * helps if a group's rows are alike. Lookups of activity with a location are nearly always about
     * a place, the inspector, a radius, a rollback, so those rows are ordered by where they happened,
     * along a curve that keeps neighbouring chunks close together; within a chunk by player and time.
     * Chat, commands and sessions are looked up by player, and are ordered by player and then time.
     * The row id comes last, so no two rows are ever equal and the order is the same however the rows
     * arrive: packing sorts one block of rows by it, and compaction merges already sorted segments by
     * it into one order across a whole day. Row ids travel with their rows, so nothing about identity
     * changes, and lookups sort their results anyway.
     * </p>
     */
    static final class LookupOrder {
        private final int worldColumn;
        private final int xColumn;
        private final int zColumn;
        private final int userColumn;
        private final int timeColumn;
        private final boolean byPlace;
        private final int length;

        private LookupOrder(String table, SQLiteColdIndex.TableLayout layout) {
            this.byPlace = !PLAYER_ORDERED.contains(table) && layout.worldColumn >= 0 && layout.xColumn >= 0 && layout.zColumn >= 0;
            this.worldColumn = layout.worldColumn;
            this.xColumn = layout.xColumn;
            this.zColumn = layout.zColumn;
            this.userColumn = layout.userColumn;
            this.timeColumn = layout.timeColumn;
            int columns = 1;
            if (byPlace) {
                columns = columns + 2;
            }
            if (byPlace || userColumn >= 0) {
                columns = columns + (userColumn >= 0 ? 1 : 0) + (timeColumn >= 0 ? 1 : 0);
            }
            this.length = columns;
        }

        /**
         * @param table
         *            the unprefixed table the rows belong to
         * @param layout
         *            the table's layout
         * @return the order that table's rows are kept in
         */
        static LookupOrder of(String table, SQLiteColdIndex.TableLayout layout) {
            return new LookupOrder(table, layout);
        }

        /**
         * @return false for a table whose rows are kept in row id order, having neither a place nor
         *         a player to be ordered by
         */
        boolean sorts() {
            return length > 1;
        }

        /**
         * @return how many numbers a key of this order holds
         */
        int keyLength() {
            return length;
        }

        /**
         * Writes the key a row sorts by.
         *
         * @param rowId
         *            the row's id
         * @param values
         *            the row's values
         * @param key
         *            receives the key, {@link #keyLength()} numbers
         */
        void key(long rowId, Object[] values, long[] key) {
            int position = 0;
            if (byPlace) {
                Object x = values[xColumn];
                Object z = values[zColumn];
                key[position++] = numeric(values[worldColumn]);
                key[position++] = x == null || z == null ? Long.MAX_VALUE
                        : SegmentFilter.chunkCurve(SegmentFilter.chunkOf(((Number) x).intValue()), SegmentFilter.chunkOf(((Number) z).intValue()));
            }
            if (byPlace || userColumn >= 0) {
                if (userColumn >= 0) {
                    key[position++] = numeric(values[userColumn]);
                }
                if (timeColumn >= 0) {
                    key[position++] = numeric(values[timeColumn]);
                }
            }
            key[position] = rowId;
        }

        /**
         * @return how two keys compare, which is never equal for two different rows
         */
        static int compare(long[] first, long[] second) {
            for (int index = 0; index < first.length; index++) {
                int compared = Long.compare(first[index], second[index]);
                if (compared != 0) {
                    return compared;
                }
            }
            return 0;
        }
    }

    /**
     * Orders a segment's rows the way packing does, for anything else that writes segments.
     *
     * @param table
     *            the unprefixed table the rows belong to
     * @param layout
     *            the table's layout
     * @param rowIds
     *            the rows' ids, reordered in place
     * @param rows
     *            the rows, reordered in place alongside their ids
     */
    static void orderForLookup(String table, SQLiteColdIndex.TableLayout layout, List<Long> rowIds, List<Object[]> rows) {
        LookupOrder order = LookupOrder.of(table, layout);
        int size = rowIds.size();
        if (!order.sorts() || size < 2) {
            return;
        }

        long[][] keys = new long[size][order.keyLength()];
        Integer[] positions = new Integer[size];
        for (int index = 0; index < size; index++) {
            order.key(rowIds.get(index), rows.get(index), keys[index]);
            positions[index] = index;
        }
        Arrays.sort(positions, (first, second) -> LookupOrder.compare(keys[first], keys[second]));

        List<Long> sortedIds = new ArrayList<>(size);
        List<Object[]> sortedRows = new ArrayList<>(size);
        for (Integer index : positions) {
            sortedIds.add(rowIds.get(index));
            sortedRows.add(rows.get(index));
        }
        rowIds.clear();
        rowIds.addAll(sortedIds);
        rows.clear();
        rows.addAll(sortedRows);
    }

    private static long numeric(Object value) {
        return value == null ? Long.MIN_VALUE : ((Number) value).longValue();
    }

    private static Object readValue(ResultSet results, int index, int type) throws SQLException {
        switch (type) {
            case ColdSegmentCodec.TYPE_TEXT: {
                String value = results.getString(index);
                return results.wasNull() ? null : value;
            }
            case ColdSegmentCodec.TYPE_BLOB: {
                byte[] value = results.getBytes(index);
                return results.wasNull() ? null : unpackBlob(value);
            }
            case ColdSegmentCodec.TYPE_REAL: {
                double value = results.getDouble(index);
                return results.wasNull() ? null : Double.valueOf(value);
            }
            default: {
                long value = results.getLong(index);
                return results.wasNull() ? null : Long.valueOf(value);
            }
        }
    }

    private static int columnIndex(SQLiteColdIndex.TableLayout layout, String name) {
        for (int index = 0; index < layout.columns.length; index++) {
            if (layout.columns[index].equalsIgnoreCase(name)) {
                return index;
            }
        }
        return -1;
    }

    private static long[] toArray(Set<Long> values) {
        long[] array = new long[values.size()];
        int index = 0;
        for (Long value : values) {
            array[index++] = value;
        }
        return array;
    }

    private static long[] toArray(List<Long> values) {
        long[] array = new long[values.size()];
        for (int index = 0; index < array.length; index++) {
            array[index] = values.get(index);
        }
        return array;
    }

    /**
     * Undoes the compression a blob was given when it was logged, before it goes into a segment.
     *
     * <p>
     * A blob is compressed on its own as it is logged, so the live table stays small while rows wait
     * there. Inside a segment that compression only gets in the way: the segment's payload is
     * compressed as a whole, against a dictionary trained on this server's data, and that finds the
     * repetition between rows that compressing each blob separately cannot. Data that is already
     * compressed has none left to find. Readers undo the compression only when a blob carries it,
     * so a blob stored plain reads back exactly as one stored compressed.
     * </p>
     *
     * @param value
     *            a blob as it is stored in the live table
     * @return the blob as it is best stored in a segment
     */
    static byte[] unpackBlob(byte[] value) {
        if (!BlobCompression.isCompressed(value)) {
            return value;
        }
        try {
            return BlobCompression.decompress(value);
        }
        catch (RuntimeException failure) {
            return value; // stored as it was, which reads back just the same
        }
    }
}
