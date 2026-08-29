package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
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
     * Deletes the rows of a single table that fall inside the purged time window.
     *
     * @param connection
     *            an open connection to the database being purged
     * @param table
     *            the unprefixed table name
     * @param timeStart
     *            the inclusive lower bound of the purged window, in epoch seconds
     * @param timeEnd
     *            the exclusive upper bound of the purged window, in epoch seconds
     * @param worldId
     *            the world to restrict the purge to, or 0 for every world
     * @param blockRestriction
     *            an optional SQL fragment restricting which rows are purged, ending in "AND"
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
    public static long purgeTable(Connection connection, String table, long timeStart, long timeEnd, int worldId, String blockRestriction, int batchSize, StatementFactory factory, BatchCallback callback) throws Exception {
        if (!PurgePolicy.isPurgeable(table)) {
            return 0;
        }
        if (blockRestriction != null && !blockRestriction.isEmpty() && !PurgePolicy.supportsBlockRestriction(table)) {
            return 0;
        }

        String worldRestriction = "";
        if (worldId > 0) {
            if (!PurgePolicy.isWorldScoped(table)) {
                return 0;
            }
            if (table.equals("entity_container") || table.equals("entity_interaction")) {
                worldRestriction = " AND (wid = '" + worldId + "' OR entity_spawn_rowid IN(SELECT rowid FROM " + ConfigHandler.prefix + "entity_spawn WHERE current_wid = '" + worldId + "'))";
            }
            else {
                worldRestriction = " AND wid = '" + worldId + "'";
            }
        }

        String restriction = blockRestriction == null ? "" : blockRestriction;
        String condition = restriction + "time < '" + timeEnd + "' AND time >= '" + timeStart + "'" + worldRestriction;

        return deleteInBatches(connection, ConfigHandler.prefix + table, condition, batchSize, factory, callback);
    }

    /**
     * Drops whole cold segments whose newest row is older than the purge cutoff, along with any
     * rollback state recorded for them. Segments never span a day, so retention lands on segment
     * boundaries and the rows inside a dropped segment never have to be examined.
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
        if (!ConfigHandler.databaseType.isSQLite()) {
            return 0;
        }

        callback.beforeBatch();
        long removed = 0;
        String prefix = ConfigHandler.prefix;

        try (PreparedStatement statement = factory.prepare(connection, "SELECT id,table_id,start_rowid,end_rowid,row_count FROM " + prefix + "segment WHERE max_time < " + cutoff)) {
            List<long[]> expired = new ArrayList<>();
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    expired.add(new long[] { results.getLong("id"), results.getLong("table_id"), results.getLong("start_rowid"), results.getLong("end_rowid"), results.getLong("row_count") });
                }
            }

            for (long[] segment : expired) {
                callback.beforeBatch();
                try (PreparedStatement delete = factory.prepare(connection, "DELETE FROM " + prefix + "segment WHERE id = " + segment[0])) {
                    delete.executeUpdate();
                }
                try (PreparedStatement delete = factory.prepare(connection, "DELETE FROM " + prefix + "cold_flag WHERE table_id = " + segment[1] + " AND rowid_ref BETWEEN " + segment[2] + " AND " + segment[3])) {
                    delete.executeUpdate();
                }
                removed = removed + segment[4];
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

        callback.beforeBatch();
        execute(connection, factory, "UPDATE " + prefix + "entity_spawn SET block_rowid=NULL WHERE block_rowid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + prefix + "block WHERE " + prefix + "block.rowid=" + prefix + "entity_spawn.block_rowid)");

        callback.beforeBatch();
        removed = removed + execute(connection, factory, "DELETE FROM " + prefix + "entity_interaction WHERE NOT EXISTS (SELECT 1 FROM " + prefix + "entity_spawn WHERE " + prefix + "entity_spawn.rowid=" + prefix + "entity_interaction.entity_spawn_rowid)");

        callback.beforeBatch();
        removed = removed + execute(connection, factory, "DELETE FROM " + prefix + "entity_container WHERE NOT EXISTS (SELECT 1 FROM " + prefix + "entity_spawn WHERE " + prefix + "entity_spawn.rowid=" + prefix + "entity_container.entity_spawn_rowid)");

        callback.beforeBatch();
        removed = removed + execute(connection, factory, "DELETE FROM " + prefix + "entity_spawn WHERE removed=1 AND block_rowid IS NULL AND kill_rowid IS NULL AND NOT EXISTS (SELECT 1 FROM " + prefix + "entity_container WHERE " + prefix + "entity_container.entity_spawn_rowid=" + prefix + "entity_spawn.rowid) AND NOT EXISTS (SELECT 1 FROM " + prefix + "entity_interaction WHERE " + prefix + "entity_interaction.entity_spawn_rowid=" + prefix + "entity_spawn.rowid)");

        return removed;
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
