package net.coreprotect.database.lookup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.ColdLookupQuery;
import net.coreprotect.database.SQLiteColdIndex;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.WorldUtils;

/**
 * What the inspector reads from: one table's rows at one place, or about one entity, live rows and
 * compressed storage together.
 *
 * <p>
 * Activity is packed into compressed storage minutes after it is logged, so the inspector reads both,
 * or it would show almost nothing. A busy place can hold close to a million rows, far more than one
 * lookup may copy out of compressed storage, so nothing here reads a place's whole history at once.
 * A page is planned from the counts and the scalar frames of the segments, and only the rows it shows
 * are copied out; a count is taken without copying out any row at all; and a read of every row goes
 * a bounded window at a time. The coordinates, or the entity, narrow down which segments are opened
 * at all.
 * </p>
 *
 * <p>
 * Each call takes the WHERE clause, and the ORDER BY where rows are read, exactly as the query would
 * apply them, since the plan has to rank rows the way the query does. A clause the reader cannot test
 * itself is still answered exactly, by walking the history a window at a time.
 * </p>
 *
 * <p>
 * Whatever is opened has to be released, including when the query fails, so this is closed like any
 * other resource. The rows of one call have to be read before the next call is made.
 * </p>
 */
public final class InspectorSource implements AutoCloseable {

    private final Connection connection;
    private final String table;
    private final int worldId;
    private final Integer[] bounds;

    /** The index hint for the live table, used only when it is read on its own. */
    private final String hint;

    /** Whether a lookup of packed rows was begun, which has to be ended on close. */
    private final boolean opened;

    /** The clause and total of the last page that counted its rows while it was planned. */
    private String countedWhere;
    private long countedTotal = -1;

    /**
     * The statement packed reads are made through. A read that walks the history runs a query per
     * window as its rows are read, each timed out as this one is, so it stays open until close.
     */
    private Statement statement;

    private InspectorSource(Connection connection, String table, int worldId, Integer[] bounds, String hint, boolean opened) {
        this.connection = connection;
        this.table = table;
        this.worldId = worldId;
        this.bounds = bounds;
        this.hint = hint;
        this.opened = opened;
    }

    /**
     * Opens the rows for one place in the world.
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param worldId
     *            the world
     * @param minX
     *            the lowest x wanted
     * @param maxX
     *            the highest x wanted
     * @param minZ
     *            the lowest z wanted
     * @param maxZ
     *            the highest z wanted
     * @param since
     *            the earliest time wanted, itself included, or 0 for no limit
     * @return the source to read from, which must be closed
     */
    public static InspectorSource open(Connection connection, String table, int worldId, int minX, int maxX, int minZ, int maxZ, long since) {
        Integer[] bounds = new Integer[] { 0, minX, maxX, null, null, minZ, maxZ };
        String hint = WorldUtils.getWidIndex(table);
        if (!ConfigHandler.databaseType.isSQLite()) {
            return new InspectorSource(connection, table, worldId, bounds, hint, false);
        }
        // The reader's start is exclusive, so it starts the second before.
        SQLiteColdIndex.beginLookup(since > 0 ? since - 1 : 0, 0);
        return new InspectorSource(connection, table, worldId, bounds, hint, true);
    }

    /**
     * Opens the rows belonging to one entity.
     *
     * <p>
     * These rows are found by the entity rather than by where they happened, so there are no
     * coordinates to narrow the segments down with. Each segment records which entities it holds rows
     * for instead, which does the same job: only the segments that could hold something of this
     * entity are opened.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param spawnRowId
     *            the entity's spawn row id
     * @return the source to read from, which must be closed
     */
    public static InspectorSource openForEntity(Connection connection, String table, long spawnRowId) {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return new InspectorSource(connection, table, 0, null, "", false);
        }
        SQLiteColdIndex.beginLookup(0, 0);
        SQLiteColdIndex.setSpawnFilter(new long[] { spawnRowId });
        return new InspectorSource(connection, table, 0, null, "", true);
    }

    /**
     * Counts the rows a clause keeps, exactly and without copying any packed row out.
     *
     * <p>
     * A page read with the same clause has already counted them while it was planned, and that count
     * is used, so a lookup that shows a page and then its page count pays for counting once.
     * </p>
     *
     * @param where
     *            the WHERE clause, without the keyword
     * @return how many rows match
     * @throws Exception
     *             if the rows cannot be read
     */
    public long count(String where) throws Exception {
        if (countedTotal >= 0 && where.equals(countedWhere)) {
            return countedTotal;
        }
        ColdLookupQuery query = packedQuery();
        if (query == null) {
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + ConfigHandler.prefix + table + " " + hint + "WHERE " + where)) {
                return results.next() ? results.getLong(1) : 0;
            }
        }
        String source = query.source(table, where, 0, worldId, bounds);
        String body = "SELECT 0 AS tbl," + query.countColumn(table) + " FROM " + source + " " + query.hint(source, hint) + "WHERE " + where;
        long total = 0;
        try (ResultSet results = query.count(statement(), body, "", null)) {
            while (results.next()) {
                total = total + results.getLong("count");
            }
        }
        return total;
    }

    /**
     * Reads one page of rows.
     *
     * <p>
     * Only the page's own rows are copied out of compressed storage, however deep into the history
     * the page lies, and every matching row is counted on the way, which {@link #count} then uses.
     * </p>
     *
     * @param columns
     *            the select list
     * @param where
     *            the WHERE clause, without the keyword
     * @param order
     *            the ORDER BY, without the keywords: {@code rowid DESC}, {@code rowid ASC} or
     *            {@code time DESC,rowid DESC}
     * @param offset
     *            how many rows come before the page
     * @param limit
     *            how many rows the page shows
     * @return the page's rows, which the caller closes
     * @throws Exception
     *             if the rows cannot be read
     */
    public ResultSet page(String columns, String where, String order, long offset, int limit) throws Exception {
        ColdLookupQuery query = limit > 0 ? packedQuery() : null;
        if (query == null) {
            return plain("SELECT " + columns + " FROM " + ConfigHandler.prefix + table + " " + hint + "WHERE " + where + " ORDER BY " + order + " LIMIT " + Math.max(0, limit) + " OFFSET " + offset);
        }
        String source = query.source(table, where, 0, worldId, bounds);
        String body = "SELECT " + columns + " FROM " + source + " " + query.hint(source, hint) + "WHERE " + where;
        ResultSet results = query.page(statement(), body, " ORDER BY " + order, offset, limit, null, true);
        countedWhere = where;
        countedTotal = query.total();
        return results;
    }

    /**
     * Reads every matching row in the order given, a bounded window of the history at a time, so
     * no read ever copies more packed rows out than one lookup may hold.
     *
     * @param columns
     *            the select list
     * @param where
     *            the WHERE clause, without the keyword
     * @param order
     *            the ORDER BY, without the keywords, as for {@link #page}
     * @return every matching row, which the caller closes
     * @throws Exception
     *             if the rows cannot be read
     */
    public ResultSet all(String columns, String where, String order) throws Exception {
        ColdLookupQuery query = packedQuery();
        if (query == null) {
            return plain("SELECT " + columns + " FROM " + ConfigHandler.prefix + table + " " + hint + "WHERE " + where + " ORDER BY " + order);
        }
        String source = query.source(table, where, 0, worldId, bounds);
        String body = "SELECT " + columns + " FROM " + source + " " + query.hint(source, hint) + "WHERE " + where;
        return query.all(statement(), body, " ORDER BY " + order, null);
    }

    /**
     * @return a query that reads packed rows too, or null when there are none to read
     */
    private ColdLookupQuery packedQuery() throws SQLException {
        return opened ? ColdLookupQuery.forLookup(connection, ConfigHandler.databaseType.getUserColumn()) : null;
    }

    private Statement statement() throws SQLException {
        if (statement == null) {
            statement = connection.createStatement();
        }
        return statement;
    }

    /** Runs a query on a statement of its own, which closes with its results. */
    private ResultSet plain(String query) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);
        try {
            statement.closeOnCompletion();
            return statement.executeQuery();
        }
        catch (SQLException exception) {
            statement.close();
            throw exception;
        }
    }

    @Override
    public void close() {
        try {
            if (statement != null) {
                statement.close();
                statement = null;
            }
        }
        catch (Exception exception) {
            ErrorReporter.report(exception);
        }
        if (!opened) {
            return;
        }
        try {
            SQLiteColdIndex.endLookup(connection);
        }
        catch (Exception exception) {
            ErrorReporter.report(exception);
        }
    }
}
