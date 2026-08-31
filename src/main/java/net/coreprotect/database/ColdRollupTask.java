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

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.Chat;

/**
 * Moves activity older than the hot window out of the live tables and into compressed cold
 * segments.
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

    private static final long SECONDS_PER_DAY = 86400L;

    /** Payload bytes collected before a dictionary is trained for a table. */
    private static final int TRAINING_TARGET_BYTES = 8 * 1024 * 1024;

    private ColdRollupTask() {
        throw new IllegalStateException("Task class");
    }

    /** Called between segments so a roll-up can stop when the database is needed elsewhere. */
    @FunctionalInterface
    public interface Callback {
        void beforeSegment() throws Exception;
    }

    /**
     * Seals every table's rows that are older than the hot window.
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
        long now = System.currentTimeMillis() / 1000L;
        // The scheduled run leaves the current day alone, so a day is only packed once it can no
        // longer receive rows and its segments are a stable unit for retention to drop.
        long sealBefore = Math.min(now - hotWindowSeconds(), (now / SECONDS_PER_DAY) * SECONDS_PER_DAY);
        return rollUp(connection, callback, sealBefore);
    }

    /**
     * Seals every row older than an explicit cutoff, ignoring the hot window. Used by the manual
     * compact command, where the operator has asked for the packing to happen now.
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
        long sealed = 0;

        for (String table : SQLiteColdIndex.getSegmentedTables()) {
            sealed = sealed + rollUpTable(connection, table, sealBefore, maxRowId, callback);
        }

        if (sealed > 0) {
            SQLiteColdIndex.reload(connection);
            try (Statement statement = connection.createStatement()) {
                // Hand the pages freed by the deletes back to the file system, a little at a time.
                statement.executeUpdate("PRAGMA incremental_vacuum(4000)");
            }
        }

        return sealed;
    }

    /**
     * Records the per player and per block type row counts of segments that were written before
     * those counts existed.
     *
     * <p>
     * Without them a lookup cannot tell which segments hold a player's rows, so it has to open all
     * of them whenever a result is smaller than a page. Each segment is decoded once here, and the
     * counts are written back; nothing else about the segment changes.
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
        try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM " + ConfigHandler.prefix + "segment WHERE user_stats IS NULL OR action_stats IS NULL"
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
        String update = "UPDATE " + ConfigHandler.prefix + "segment SET user_stats = ?, type_stats = ?, action_stats = ?, spawn_filter = ? WHERE id = ?";
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

                int userColumn = columnIndex(layout, "user");
                int typeColumn = columnIndex(layout, "type");
                int actionColumn = columnIndex(layout, "action");
                int spawnColumn = columnIndex(layout, "entity_spawn_rowid");
                if (userColumn < 0 && typeColumn < 0 && actionColumn < 0 && spawnColumn < 0) {
                    continue;
                }

                ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
                Map<Long, Integer> userCounts = new HashMap<>();
                Map<Long, Integer> typeCounts = new HashMap<>();
                Map<Long, Integer> actionCounts = new HashMap<>();
                Set<Long> spawnIds = new LinkedHashSet<>();
                for (int row = 0; row < rows.size(); row++) {
                    Object[] values = rows.getValues(row);
                    if (spawnColumn >= 0 && values[spawnColumn] != null) {
                        spawnIds.add(((Number) values[spawnColumn]).longValue());
                    }
                    if (userColumn >= 0 && values[userColumn] != null && userCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                        userCounts.merge(((Number) values[userColumn]).longValue(), 1, Integer::sum);
                    }
                    if (typeColumn >= 0 && values[typeColumn] != null && typeCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                        typeCounts.merge(((Number) values[typeColumn]).longValue(), 1, Integer::sum);
                    }
                    if (actionColumn >= 0 && values[actionColumn] != null && actionCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                        actionCounts.merge(((Number) values[actionColumn]).longValue(), 1, Integer::sum);
                    }
                }

                statement.setBytes(1, SegmentStatistics.encode(userCounts));
                statement.setBytes(2, SegmentStatistics.encode(typeCounts));
                statement.setBytes(3, SegmentStatistics.encode(actionCounts));
                statement.setBytes(4, spawnColumn < 0 ? null : SegmentMembership.encode(toArray(spawnIds)));
                statement.setLong(5, id);
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
     * @param sealBefore
     *            rows with a timestamp below this are packed
     * @param maxRowId
     *            rows above this row id are left alone
     * @param callback
     *            invoked before each segment; throwing stops the roll-up
     * @return the number of rows moved into segments
     * @throws Exception
     *             if the roll-up fails or is stopped
     */
    public static long sealTable(Connection connection, String table, long sealBefore, long maxRowId, Callback callback) throws Exception {
        long sealed = rollUpTable(connection, table, sealBefore, maxRowId, callback);
        if (sealed > 0) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA incremental_vacuum(4000)");
            }
        }
        return sealed;
    }

    private static long rollUpTable(Connection connection, String table, long sealBefore, long maxRowId, Callback callback) throws Exception {
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
                cursor = block.maxRowId;
                blocks.add(block);
            }

            if (blocks.isEmpty()) {
                break;
            }

            if (dictionaryId == 0 && trainingBytes < TRAINING_TARGET_BYTES) {
                Block block = blocks.get(0);
                ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(layout.types, toArray(block.rowIds), block.rows);
                if (frames.getPayload().length > 0) {
                    trainingSamples.addAll(ColdSegmentCodec.payloadSamples(frames.getScalars(), frames.getPayload()));
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
                    ColdSegmentCodec.Frames frames = ColdSegmentCodec.encode(layout.types, toArray(block.rowIds), block.rows);
                    verify(frames, layout, block);
                    byte[] scalars = SegmentDictionary.compressWith(frames.getScalars(), 0);
                    byte[] payload = frames.getPayload().length == 0 ? null : SegmentDictionary.compressWith(frames.getPayload(), dictionary);
                    return new Sealed(block, frames, scalars, payload);
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
                writeSegment(connection, tableId, table, segment.block, layout, segment.frames, segment.scalars, segment.payload, dictionaryId);
                sealed = sealed + segment.block.rowIds.size();
                CompactProgress.set("packing " + table + " into compressed storage", sealed, 0);
            }
            SQLiteColdIndex.reload(connection);
        }

        return sealed;
    }

    /** A segment that has been encoded, checked and compressed, and is waiting to be written. */
    private static final class Sealed {
        private final Block block;
        private final ColdSegmentCodec.Frames frames;
        private final byte[] scalars;
        private final byte[] payload;

        private Sealed(Block block, ColdSegmentCodec.Frames frames, byte[] scalars, byte[] payload) {
            this.block = block;
            this.frames = frames;
            this.scalars = scalars;
            this.payload = payload;
        }
    }

    /**
     * Decodes a freshly encoded segment and checks it against what was read, so no rows are deleted
     * from the live table unless they can be read back.
     */
    private static void verify(ColdSegmentCodec.Frames frames, SQLiteColdIndex.TableLayout layout, Block block) throws SQLException {
        ColdSegmentCodec.Rows rows = ColdSegmentCodec.decode(frames.getScalars(), frames.getPayload());
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

    private static void writeSegment(Connection connection, int tableId, String table, Block block, SQLiteColdIndex.TableLayout layout, ColdSegmentCodec.Frames frames, byte[] scalarFrame, byte[] payloadFrame, int dictionaryId) throws SQLException {
        String insert = "INSERT INTO " + ConfigHandler.prefix + "segment (table_id,start_rowid,end_rowid,row_count,min_time,max_time,day,"
                + "wid_set,chunk_filter,user_filter,type_filter,action_bits,dict_id,codec_version,scalars,scalars_size,payload,payload_size,"
                + "user_stats,type_stats,action_stats,spawn_filter) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        // Started from a known transaction state rather than from whatever the driver believes it
        // to be. The two can come apart, and when they do the writes below go in one at a time and
        // the commit fails with "cannot commit - no transaction is active", taking the compact down.
        Database.inTransaction(connection, () -> {
            try (PreparedStatement statement = connection.prepareStatement(insert)) {
                statement.setInt(1, tableId);
                statement.setLong(2, block.minRowId);
                statement.setLong(3, block.maxRowId);
                statement.setInt(4, block.rowIds.size());
                statement.setLong(5, block.minTime);
                statement.setLong(6, block.maxTime);
                statement.setLong(7, block.minTime / SECONDS_PER_DAY);
                statement.setBytes(8, SQLiteColdIndex.writeWorldIds(new ArrayList<>(block.worldIds)));
                statement.setBytes(9, SegmentMembership.encode(toArray(block.chunkKeys), SegmentFilter.CHUNK_BYTES));
                statement.setBytes(10, SegmentMembership.encode(toArray(block.userIds)));
                statement.setBytes(11, SegmentMembership.encode(toArray(block.typeIds)));
                statement.setLong(12, block.actionBits);
                statement.setInt(13, dictionaryId);
                statement.setInt(14, ColdSegmentCodec.CODEC_VERSION);
                statement.setBytes(15, scalarFrame);
                statement.setInt(16, frames.getScalars().length);
                statement.setBytes(17, payloadFrame);
                statement.setInt(18, frames.getPayload().length);
                statement.setBytes(19, SegmentStatistics.encode(block.userCounts));
                statement.setBytes(20, SegmentStatistics.encode(block.typeCounts));
                statement.setBytes(21, SegmentStatistics.encode(block.actionCounts));
                statement.setBytes(22, SegmentMembership.encode(toArray(block.spawnIds)));
                statement.executeUpdate();
            }

            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM " + ConfigHandler.prefix + table + " WHERE rowid BETWEEN ? AND ?")) {
                statement.setLong(1, block.minRowId);
                statement.setLong(2, block.maxRowId);
                int deleted = statement.executeUpdate();
                if (deleted != block.rowIds.size()) {
                    throw new SQLException("Sealed " + block.rowIds.size() + " rows of " + table + " but removed " + deleted);
                }
            }
        });
    }

    /**
     * One block of consecutive rows, ready to be encoded.
     *
     * <p>
     * The sets of distinct values are collected in full rather than stopping at the point where they
     * would no longer be listed exactly. What is stored is decided when the segment is written, and
     * deciding it while reading would build a filter that knew about only the values seen up to the
     * cut off: a lookup for any of the rest would be told the segment does not hold them, and its
     * rows would go missing. A segment holds at most 65,536 rows, so the sets are bounded anyway.
     * </p>
     */
    private static final class Block {
        private final List<Long> rowIds = new ArrayList<>();
        private final List<Object[]> rows = new ArrayList<>();
        private final Set<Integer> worldIds = new LinkedHashSet<>();
        private final Set<Long> userIds = new LinkedHashSet<>();
        private final Set<Long> typeIds = new LinkedHashSet<>();
        private final Set<Long> spawnIds = new LinkedHashSet<>();
        private final Map<Long, Integer> userCounts = new HashMap<>();
        private final Map<Long, Integer> typeCounts = new HashMap<>();
        private final Map<Long, Integer> actionCounts = new HashMap<>();
        private final Set<Long> chunkKeys = new LinkedHashSet<>();
        private long actionBits;
        private long minTime = Long.MAX_VALUE;
        private long maxTime = Long.MIN_VALUE;
        private long minRowId = Long.MAX_VALUE;
        private long maxRowId = Long.MIN_VALUE;
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

        String query = "SELECT " + columns + " FROM " + ConfigHandler.prefix + table
                + " WHERE rowid > ? AND rowid <= ? AND " + layout.columns[layout.timeColumn] + " < ? ORDER BY rowid LIMIT " + SEGMENT_ROWS;

        Block block = new Block();
        int userColumn = columnIndex(layout, "user");
        int typeColumn = columnIndex(layout, "type");
        int actionColumn = columnIndex(layout, "action");
        int spawnColumn = columnIndex(layout, "entity_spawn_rowid");
        long day = -1;

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, afterRowId);
            statement.setLong(2, maxRowId);
            statement.setLong(3, sealBefore);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    long time = results.getLong(layout.timeColumn + 2);
                    long rowDay = time / SECONDS_PER_DAY;
                    if (day < 0) {
                        day = rowDay;
                    }
                    else if (rowDay != day) {
                        // A segment never spans a day, so whole days can be dropped at purge time.
                        break;
                    }

                    Object[] values = new Object[layout.columns.length];
                    for (int column = 0; column < layout.columns.length; column++) {
                        values[column] = readValue(results, column + 2, layout.types[column]);
                    }

                    long rowId = results.getLong(1);
                    block.rowIds.add(rowId);
                    block.rows.add(values);
                    block.minRowId = Math.min(block.minRowId, rowId);
                    block.maxRowId = Math.max(block.maxRowId, rowId);
                    block.minTime = Math.min(block.minTime, time);
                    block.maxTime = Math.max(block.maxTime, time);

                    if (layout.worldColumn >= 0 && values[layout.worldColumn] != null) {
                        int worldId = ((Number) values[layout.worldColumn]).intValue();
                        block.worldIds.add(worldId);
                        if (layout.xColumn >= 0 && layout.zColumn >= 0 && values[layout.xColumn] != null && values[layout.zColumn] != null) {

                            int x = ((Number) values[layout.xColumn]).intValue();
                            int z = ((Number) values[layout.zColumn]).intValue();
                            block.chunkKeys.add(SegmentFilter.chunkKey(worldId, SegmentFilter.chunkOf(x), SegmentFilter.chunkOf(z)));
                        }
                    }

                    if (userColumn >= 0 && values[userColumn] != null) {
                        long userId = ((Number) values[userColumn]).longValue();
                        block.userIds.add(userId);
                        if (block.userCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                            block.userCounts.merge(userId, 1, Integer::sum);
                        }
                    }

                    if (typeColumn >= 0 && values[typeColumn] != null) {
                        long typeId = ((Number) values[typeColumn]).longValue();
                        block.typeIds.add(typeId);
                        if (block.typeCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                            block.typeCounts.merge(typeId, 1, Integer::sum);
                        }
                    }

                    if (spawnColumn >= 0 && values[spawnColumn] != null) {
                        block.spawnIds.add(((Number) values[spawnColumn]).longValue());
                    }

                    if (actionColumn >= 0 && values[actionColumn] != null) {
                        int action = ((Number) values[actionColumn]).intValue();
                        if (action >= 0 && action < 64) {
                            block.actionBits |= (1L << action);
                        }
                        if (block.actionCounts.size() <= SegmentStatistics.MAXIMUM_VALUES) {
                            block.actionCounts.merge((long) action, 1, Integer::sum);
                        }
                    }
                }
            }
        }

        if (block.rowIds.isEmpty()) {
            return null;
        }

        clusterByUser(block, userColumn, layout.timeColumn);
        return block;
    }

    /**
     * Groups the rows of a segment by player before they are encoded.
     *
     * <p>
     * Rows that belong together compress better than rows interleaved by chance: one player's
     * coordinates, block types and actions repeat far more than a mixture of everyone's do. Row ids
     * travel with their rows, so nothing about identity or ordering changes; only the order the rows
     * happen to sit in inside the compressed block does, and lookups sort their results anyway.
     * </p>
     *
     * @param block
     *            the rows about to be encoded
     * @param userColumn
     *            the index of the user column, or -1 when the table has none
     * @param timeColumn
     *            the index of the time column
     */
    private static void clusterByUser(Block block, int userColumn, int timeColumn) {
        if (userColumn < 0 || block.rowIds.size() < 2) {
            return;
        }

        Integer[] order = new Integer[block.rowIds.size()];
        for (int index = 0; index < order.length; index++) {
            order[index] = index;
        }

        Arrays.sort(order, (first, second) -> {
            long userFirst = numeric(block.rows.get(first)[userColumn]);
            long userSecond = numeric(block.rows.get(second)[userColumn]);
            if (userFirst != userSecond) {
                return Long.compare(userFirst, userSecond);
            }
            if (timeColumn >= 0) {
                int byTime = Long.compare(numeric(block.rows.get(first)[timeColumn]), numeric(block.rows.get(second)[timeColumn]));
                if (byTime != 0) {
                    return byTime;
                }
            }
            return Long.compare(block.rowIds.get(first), block.rowIds.get(second));
        });

        List<Long> rowIds = new ArrayList<>(order.length);
        List<Object[]> rows = new ArrayList<>(order.length);
        for (Integer index : order) {
            rowIds.add(block.rowIds.get(index));
            rows.add(block.rows.get(index));
        }

        block.rowIds.clear();
        block.rowIds.addAll(rowIds);
        block.rows.clear();
        block.rows.addAll(rows);
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
                return results.wasNull() ? null : value;
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
     * @return how much recent history stays in the live tables, in seconds
     */
    static long hotWindowSeconds() {
        long configured = Config.getGlobal().HOT_WINDOW_SECONDS;
        return configured > 0 ? configured : 604800L;
    }
}
