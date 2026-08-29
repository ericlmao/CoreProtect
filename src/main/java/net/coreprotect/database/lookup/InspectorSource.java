package net.coreprotect.database.lookup;

import java.sql.Connection;
import java.sql.SQLException;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.SQLiteColdIndex;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.WorldUtils;

/**
 * What the inspector reads from, live rows and compressed storage together.
 *
 * <p>
 * The inspector used to read the live tables alone, so clicking a block showed only what had happened
 * to it inside the hot window and anything older looked as though it had never happened. It reads
 * both now. The cost is bounded by the coordinates: only the segments whose chunks could hold the
 * block are opened, and only the rows in them at those coordinates are handed to the query, so a
 * click costs about what a small lookup costs rather than what a full scan would.
 * </p>
 *
 * <p>
 * Whatever is opened has to be released, including when the query fails, so this is closed like any
 * other resource.
 * </p>
 */
public final class InspectorSource implements AutoCloseable {

    /** The expression to read from: the live table, or the live table and its segments together. */
    private final String table;

    /** The index hint for the live table, or empty when reading from both. */
    private final String index;

    private final Connection connection;
    private final boolean opened;

    private InspectorSource(String table, String index, Connection connection, boolean opened) {
        this.table = table;
        this.index = index;
        this.connection = connection;
        this.opened = opened;
    }

    /**
     * @return the expression a query should read from
     */
    public String table() {
        return table;
    }

    /**
     * @return the index hint to place after the table, which is empty when there is nothing to hint
     */
    public String index() {
        return index;
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
     *            the earliest time wanted, or 0 for no limit
     * @return the source to read from, which must be closed
     */
    public static InspectorSource open(Connection connection, String table, int worldId, int minX, int maxX, int minZ, int maxZ, long since) {
        String live = ConfigHandler.prefix + table;
        String hint = WorldUtils.getWidIndex(table);
        if (!ConfigHandler.databaseType.isSQLite()) {
            return new InspectorSource(live, hint, connection, false);
        }

        try {
            SQLiteColdIndex.beginLookup(since, 0);
            String expression = SQLiteColdIndex.sourceExpression(connection, table, worldId, new Integer[] { 0, minX, maxX, 0, 0, minZ, maxZ });
            // Reading from both means reading from an expression rather than a table, and an index
            // hint only names an index on the table.
            return new InspectorSource(expression, expression.equals(live) ? hint : "", connection, true);
        }
        catch (SQLException exception) {
            ErrorReporter.report(exception);
            SQLiteColdIndex.endLookup(connection);
            return new InspectorSource(live, hint, connection, false);
        }
    }

    @Override
    public void close() {
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
