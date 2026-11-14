package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.coreprotect.config.ConfigHandler;

/**
 * Shared purge logic for the relational databases.
 *
 * <p>
 * Rows are always deleted in place, in small batches, so a purge never needs to build a second
 * copy of the database and never requires additional disk space beyond the journal for one
 * batch. Both the manual {@code /co purge} command and the automatic purge use this class, so
 * they behave identically.
 * </p>
 */
public final class PurgeExecutor {

    /** The savepoint a segment is dropped under, so its rows all go in one transaction. */
    private static final String PURGE_SAVEPOINT = "cp_purge_segment";

    /** Rows deleted per statement before pausing to let other database work through. */
    public static final int DEFAULT_BATCH_SIZE = 10000;

    /**
     * Creates the statements used by a purge. Callers supply their own factory so that
     * statements can be tracked and cancelled while a purge is running.
     */
    @FunctionalInterface
    public interface StatementFactory {
        PreparedStatement prepare(Connection connection, String query) throws Exception;
    }

    /**
     * Invoked before every batch. Throwing from this callback stops the purge, which is how
     * cancellation during a shutdown or a competing database operation is handled.
     */
    @FunctionalInterface
    public interface BatchCallback {
        void beforeBatch() throws Exception;
    }

    private PurgeExecutor() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Deletes the rows of a single table that a purge selects.
     *
     * <p>
     * Entity data has no world or type of its own; it belongs to the kill row that references it.
     * When a purge removes only some kill rows, the entity data of those kills is deleted before
     * the kill rows themselves, while the references are still there to follow. On MySQL both
     * deletes run in one transaction, so a failure part way through never leaves a kept kill row
     * without the data its rollback needs.
     * </p>
     *
     * @param connection
     *            an open connection to the database being purged
     * @param table
     *            the unprefixed table name
     * @param filter
     *            selects the rows the purge removes
     * @param batchSize
     *            the maximum number of rows deleted per statement
     * @param factory
     *            the statement factory to use
     * @param callback
     *            invoked before every batch
     * @return the number of rows deleted
     * @throws Exception
     *             if the purge fails or is cancelled
     */
    public static long purgeTable(Connection connection, String table, PurgeFilter filter, int batchSize, StatementFactory factory, BatchCallback callback) throws Exception {
        String condition = filter.deleteCondition(table, ConfigHandler.prefix);
        if (condition == null) {
            return 0;
        }

        boolean deleteEntities = table.equals("block") && filter.removesKills() && !filter.purgesEntitiesByTime();
        if (!deleteEntities) {
            return deleteInBatches(connection, ConfigHandler.prefix + table, condition, batchSize, factory, callback);
        }

        boolean atomic = ConfigHandler.databaseType.isMySQL();
        if (atomic) {
            connection.setAutoCommit(false);
        }
        try {
            callback.beforeBatch();
            long removed = execute(connection, factory, filter.deleteEntitiesOfPurgedKills(ConfigHandler.databaseType, ConfigHandler.prefix));
            removed = removed + deleteInBatches(connection, ConfigHandler.prefix + table, condition, batchSize, factory, callback);
            if (atomic) {
                connection.commit();
                connection.setAutoCommit(true);
            }
            return removed;
        }
        catch (Exception e) {
            if (atomic) {
                try {
                    connection.rollback();
                    connection.setAutoCommit(true);
                }
                catch (SQLException rollbackException) {
                    e.addSuppressed(rollbackException);
                }
            }
            throw e;
        }
    }

    /**
     * Deletes every row of a table older than a cutoff, as the automatic purge does.
     *
     * @param connection
     *            an open connection to the database being purged
     * @param table
     *            the unprefixed table name
     * @param cutoff
     *            rows older than this timestamp are removed
     * @param batchSize
     *            the maximum number of rows deleted per statement
     * @param factory
     *            the statement factory to use
     * @param callback
     *            invoked before every batch
     * @return the number of rows deleted
     * @throws Exception
     *             if the purge fails or is cancelled
     */
    public static long purgeTable(Connection connection, String table, long cutoff, int batchSize, StatementFactory factory, BatchCallback callback) throws Exception {
        PurgeFilter filter = new PurgeFilter(0, cutoff, 0, null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false);
        return purgeTable(connection, table, filter, batchSize, factory, callback);
    }

    /**
     * Drops whole cold segments whose newest row is older than the purge cutoff, along with any
     * rollback state recorded for them. Segments never span a day, so retention lands on segment
     * boundaries and the rows inside a dropped segment never have to be examined. A day that has
     * been compacted is ordered by place rather than by time, so each of its segments spans the
     * whole day, and it is dropped once the cutoff has passed its end.
     *
     * @param connection
     *            an open connection to the database being purged
     * @param cutoff
     *            rows older than this timestamp are being removed
     * @param factory
     *            the statement factory to use
     * @param callback
     *            invoked before the work starts
     * @return the number of rows the dropped segments held
     * @throws Exception
     *             if the purge fails or is cancelled
     */
    public static long purgeColdSegments(Connection connection, long cutoff, StatementFactory factory, BatchCallback callback) throws Exception {
        return purgeColdSegments(connection, 0, cutoff, factory, callback);
    }

    /**
     * Drops whole cold segments that lie entirely inside a purged time window.
     *
     * @param connection
     *            an open connection to the database being purged
     * @param timeStart
     *            rows older than this timestamp are kept, or 0 for no lower bound
     * @param cutoff
     *            rows older than this timestamp are being removed
     * @param factory
     *            the statement factory to use
     * @param callback
     *            invoked before the work starts
     * @return the number of rows the dropped segments held
     * @throws Exception
     *             if the purge fails or is cancelled
     */
    public static long purgeColdSegments(Connection connection, long timeStart, long cutoff, StatementFactory factory, BatchCallback callback) throws Exception {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return 0;
        }

        callback.beforeBatch();
        long removed = 0;
        String prefix = ConfigHandler.prefix;

        // The segments compaction made out of one run all span the run's whole range of row ids, and
        // the rollback state of the run's rows is recorded against that range. They are dropped
        // together or not at all: dropping one would take the rollback state of rows the others
        // still hold. Segments compaction is still writing are left to it; they hold rows that are
        // also still in the segments they are being merged from, and a compaction whose inputs were
        // purged meanwhile gives up without swapping anything in. A purge with a lower bound only
        // drops segments lying wholly inside its window.
        String expiredWindow = "max_time < " + cutoff + (timeStart > 0 ? " AND min_time >= " + timeStart : "");
        String keptWindow = "kept.max_time >= " + cutoff + (timeStart > 0 ? " OR kept.min_time < " + timeStart : "");
        String query = "SELECT id,table_id,start_rowid,end_rowid,row_count FROM " + prefix + "segment AS expired WHERE " + expiredWindow + " AND compacted >= 0"
                + " AND NOT EXISTS (SELECT 1 FROM " + prefix + "segment AS kept WHERE kept.table_id = expired.table_id AND kept.start_rowid = expired.start_rowid"
                + " AND kept.end_rowid = expired.end_rowid AND kept.compacted >= 0 AND (" + keptWindow + ")) ORDER BY table_id,start_rowid,id";
        try (PreparedStatement statement = factory.prepare(connection, query)) {
            List<List<long[]>> expired = new ArrayList<>();
            try (ResultSet results = statement.executeQuery()) {
                List<long[]> range = null;
                while (results.next()) {
                    long[] segment = new long[] { results.getLong("id"), results.getLong("table_id"), results.getLong("start_rowid"), results.getLong("end_rowid"), results.getLong("row_count") };
                    if (range == null || range.get(0)[1] != segment[1] || range.get(0)[2] != segment[2] || range.get(0)[3] != segment[3]) {
                        range = new ArrayList<>();
                        expired.add(range);
                    }
                    range.add(segment);
                }
            }

            for (List<long[]> range : expired) {
                callback.beforeBatch();
                long[] first = range.get(0);
                StringBuilder ids = new StringBuilder();
                long rows = 0;
                for (long[] segment : range) {
                    ids.append(ids.length() > 0 ? "," : "").append(segment[0]);
                    rows = rows + segment[4];
                }
                // A segment row, its unit rows and its rollback state go together or not at all. A
                // unit row outlasting its segment would be read as part of whichever segment is
                // given the id next. A savepoint opens a transaction of its own on a connection that
                // has none, and nests inside one the caller holds.
                try (Statement savepoint = connection.createStatement()) {
                    savepoint.executeUpdate("SAVEPOINT " + PURGE_SAVEPOINT);
                    boolean released = false;
                    try {
                        execute(connection, factory, "DELETE FROM " + prefix + "segment WHERE id IN (" + ids + ")");
                        execute(connection, factory, "DELETE FROM " + prefix + "segment_unit WHERE segment_id IN (" + ids + ")");
                        execute(connection, factory, "DELETE FROM " + prefix + "cold_flag WHERE table_id = " + first[1] + " AND rowid_ref BETWEEN " + first[2] + " AND " + first[3]);
                        savepoint.executeUpdate("RELEASE SAVEPOINT " + PURGE_SAVEPOINT);
                        released = true;
                    }
                    finally {
                        if (!released) {
                            try {
                                savepoint.executeUpdate("ROLLBACK TO SAVEPOINT " + PURGE_SAVEPOINT);
                                savepoint.executeUpdate("RELEASE SAVEPOINT " + PURGE_SAVEPOINT);
                            }
                            catch (SQLException undoFailure) {
                                // The failure that stopped the deletes is the one worth reporting.
                            }
                        }
                    }
                }
                removed = removed + rows;
            }

            if (!expired.isEmpty()) {
                SQLiteColdIndex.reload(connection);
            }
        }

        return removed;
    }

    /**
     * Removes references to purged rows, and any entity spawn rows that no longer have data
     * associated with them.
     *
     * @param connection
     *            an open connection to the database being purged
     * @param factory
     *            the statement factory to use
     * @param callback
     *            invoked before every statement
     * @return the number of rows deleted
     * @throws Exception
     *             if the cleanup fails or is cancelled
     */
    public static long removeOrphanedRows(Connection connection, StatementFactory factory, BatchCallback callback) throws Exception {
        String prefix = ConfigHandler.prefix;
        long removed = 0;

        callback.beforeBatch();
        execute(connection, factory, "UPDATE " + prefix + "entity_spawn SET kill_rowid=NULL WHERE kill_rowid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + prefix + "entity WHERE " + prefix + "entity.rowid=" + prefix + "entity_spawn.kill_rowid)");

        // On SQLite a block row that is not in the live table may have been packed into compressed
        // storage rather than purged, and a link to it is still a link.
        boolean packed = ConfigHandler.databaseType.isSQLite() && hasPackedRows(connection);
        String blockPacked = "";
        if (packed) {
            blockPacked = " AND NOT COALESCE((SELECT start_rowid FROM " + prefix + "segment WHERE table_id=" + SQLiteColdIndex.tableId("block")
                    + " AND end_rowid >= " + prefix + "entity_spawn.block_rowid ORDER BY end_rowid LIMIT 1) <= " + prefix + "entity_spawn.block_rowid, 0)";
        }

        callback.beforeBatch();
        execute(connection, factory, "UPDATE " + prefix + "entity_spawn SET block_rowid=NULL WHERE block_rowid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + prefix + "block WHERE " + prefix + "block.rowid=" + prefix + "entity_spawn.block_rowid)" + blockPacked);

        callback.beforeBatch();
        removed = removed + execute(connection, factory, "DELETE FROM " + prefix + "entity_interaction WHERE NOT EXISTS (SELECT 1 FROM " + prefix + "entity_spawn WHERE " + prefix + "entity_spawn.rowid=" + prefix + "entity_interaction.entity_spawn_rowid)");

        callback.beforeBatch();
        removed = removed + execute(connection, factory, "DELETE FROM " + prefix + "entity_container WHERE NOT EXISTS (SELECT 1 FROM " + prefix + "entity_spawn WHERE " + prefix + "entity_spawn.rowid=" + prefix + "entity_container.entity_spawn_rowid)");

        String unreferenced = "removed=1 AND block_rowid IS NULL AND kill_rowid IS NULL AND NOT EXISTS (SELECT 1 FROM " + prefix + "entity_container WHERE " + prefix + "entity_container.entity_spawn_rowid=" + prefix + "entity_spawn.rowid) AND NOT EXISTS (SELECT 1 FROM " + prefix + "entity_interaction WHERE " + prefix + "entity_interaction.entity_spawn_rowid=" + prefix + "entity_spawn.rowid)";
        if (!packed) {
            callback.beforeBatch();
            removed = removed + execute(connection, factory, "DELETE FROM " + prefix + "entity_spawn WHERE " + unreferenced);
            return removed;
        }

        // Rows about an entity can also be packed, and a segment only records which entities it
        // holds rows for as a set, which SQL cannot read. Each candidate is checked against those
        // sets, and one a segment may still refer to is kept.
        List<Long> doomed = new ArrayList<>();
        callback.beforeBatch();
        // Held like a lookup, so a segment cannot be committed between reading the live rows and
        // reading what the segments hold.
        SQLiteColdIndex.beginLookup(0, 0);
        try (PreparedStatement statement = factory.prepare(connection, "SELECT rowid FROM " + prefix + "entity_spawn WHERE " + unreferenced);
                ResultSet results = statement.executeQuery()) {
            while (results.next()) {
                long spawnRowId = results.getLong(1);
                if (!SQLiteColdIndex.spawnMayBeReferenced(spawnRowId)) {
                    doomed.add(spawnRowId);
                }
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        for (int start = 0; start < doomed.size(); start += 500) {
            StringBuilder ids = new StringBuilder();
            for (int index = start; index < Math.min(doomed.size(), start + 500); index++) {
                ids.append(index > start ? "," : "").append(doomed.get(index));
            }
            callback.beforeBatch();
            removed = removed + execute(connection, factory, "DELETE FROM " + prefix + "entity_spawn WHERE rowid IN(" + ids + ") AND " + unreferenced);
        }

        return removed;
    }

    private static boolean hasPackedRows(Connection connection) throws SQLException {
        SQLiteColdIndex.ensureLoaded(connection);
        return SQLiteColdIndex.hasSegments();
    }

    private static long deleteInBatches(Connection connection, String table, String condition, int batchSize, StatementFactory factory, BatchCallback callback) throws Exception {
        int limit = Math.max(1, batchSize);
        String query = buildBatchDelete(table, condition, limit);
        long removed = 0;

        while (true) {
            callback.beforeBatch();
            int deleted = execute(connection, factory, query);
            removed = removed + deleted;
            if (deleted < limit) {
                return removed;
            }
        }
    }

    private static String buildBatchDelete(String table, String condition, int limit) {
        if (ConfigHandler.databaseType.isMySQL()) {
            return "DELETE FROM " + table + " WHERE " + condition + " LIMIT " + limit;
        }
        // SQLite and DuckDB do not support a LIMIT on DELETE, so the batch is selected by row id.
        return "DELETE FROM " + table + " WHERE rowid IN(SELECT rowid FROM " + table + " WHERE " + condition + " LIMIT " + limit + ")";
    }

    private static int execute(Connection connection, StatementFactory factory, String query) throws Exception {
        try (PreparedStatement statement = factory.prepare(connection, query)) {
            return statement.executeUpdate();
        }
    }
}
