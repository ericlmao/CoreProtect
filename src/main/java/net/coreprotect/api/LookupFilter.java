package net.coreprotect.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bukkit.Location;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.ColdLookupQuery;
import net.coreprotect.database.DuckDBLookupQuery;
import net.coreprotect.database.DuckDBSpatialIndex;
import net.coreprotect.database.SQLiteColdIndex;
import net.coreprotect.utility.WorldUtils;

/**
 * The user, time, place and limit of an API lookup, and how its query reads them.
 *
 * <p>
 * On SQLite most activity has been packed into compressed storage, and a place or a player can hold
 * far more packed rows than one lookup may copy out at once. So there the clause is written with its
 * values in it rather than as parameters, which lets the reader see exactly what the query keeps, and
 * the query is run through {@link #read}: a page plans exactly which rows it shows and copies out
 * only those, and a read of every row goes a bounded window of the history at a time.
 * </p>
 */
final class LookupFilter {
    private final Integer userId;
    private final int checkTime;
    private final Location location;
    private final int radius;
    private final int limitOffset;
    private final int limitCount;

    /** The connection a lookup of packed rows was opened on, or null when none was. */
    private Connection lookupConnection;

    /** The query that reads packed rows, or null when there are none to read. */
    private ColdLookupQuery packed;

    /** The placeholder handed out for each table the query reads packed rows from. */
    private final java.util.Map<String, String> sources = new java.util.HashMap<>();

    /** Reads the parameters of a query that is run with them. */
    @FunctionalInterface
    interface Binder {
        void bind(PreparedStatement statement) throws Exception;
    }

    /** Receives each row a lookup finds. */
    @FunctionalInterface
    interface RowReader {
        void read(ResultSet results) throws Exception;
    }

    private LookupFilter(Integer userId, int checkTime, Location location, int radius, int limitOffset, int limitCount) {
        this.userId = userId;
        this.checkTime = checkTime;
        this.location = location;
        this.radius = radius;
        this.limitOffset = limitOffset;
        this.limitCount = limitCount;
    }

    static LookupFilter fromOptions(Connection connection, LookupOptions options) throws Exception {
        if (options == null) {
            options = LookupOptions.builder().build();
        }

        Integer userId = MessageAPI.getUserId(connection, options.getUser());
        int checkTime = 0;
        if (options.getTime() > 0) {
            checkTime = (int) (System.currentTimeMillis() / 1000L) - options.getTime();
        }

        return new LookupFilter(userId, checkTime, options.getLocation(), options.getRadius(), options.getLimitOffset(), options.getLimitCount());
    }

    boolean hasInvalidUser() {
        return userId != null && userId == -1;
    }

    boolean hasInvalidLocation() {
        return location != null && location.getWorld() == null;
    }

    boolean hasLocation() {
        return location != null;
    }

    boolean beginDuckDBSnapshot(Connection connection) throws Exception {
        if (!ConfigHandler.databaseType.isDuckDB() || location == null || !connection.getAutoCommit()) {
            return false;
        }
        connection.setAutoCommit(false);
        return true;
    }

    void endDuckDBSnapshot(Connection connection, boolean started) throws Exception {
        if (!started) {
            return;
        }
        try {
            connection.rollback();
        }
        finally {
            connection.setAutoCommit(true);
        }
    }

    /**
     * @return true when values are written into the clause, as they are on SQLite, rather than
     *         bound as parameters
     */
    private static boolean literal() {
        return ConfigHandler.databaseType.isSQLite();
    }

    /** A value in a clause: written out on SQLite, a parameter anywhere else. */
    private static String value(long value) {
        return literal() ? Long.toString(value) : "?";
    }

    /**
     * @param alias
     *            the name the query gives the table, or empty for none
     * @return the lookup's conditions, without the WHERE keyword
     */
    String where(String alias) {
        StringBuilder query = new StringBuilder();
        String qualifier = alias.isEmpty() ? "" : alias + ".";
        query.append(qualifier).append("time > ").append(value(checkTime));

        if (userId != null) {
            query.append(" AND ").append(qualifier).append(ConfigHandler.databaseType.getUserColumn()).append(" = ").append(value(userId));
        }

        if (location != null) {
            int x = location.getBlockX();
            int y = location.getBlockY();
            int z = location.getBlockZ();
            query.append(" AND ").append(qualifier).append("wid = ").append(value(worldId()));
            if (radius > 0) {
                query.append(" AND ").append(qualifier).append("x >= ").append(value(MessageAPI.clampToInt((long) x - radius))).append(" AND ").append(qualifier).append("x <= ").append(value(MessageAPI.clampToInt((long) x + radius)))
                        .append(" AND ").append(qualifier).append("z >= ").append(value(MessageAPI.clampToInt((long) z - radius))).append(" AND ").append(qualifier).append("z <= ").append(value(MessageAPI.clampToInt((long) z + radius)));
            }
            else {
                query.append(" AND ").append(qualifier).append("x = ").append(value(x)).append(" AND ").append(qualifier).append("y = ").append(value(y)).append(" AND ").append(qualifier).append("z = ").append(value(z));
            }
        }
        return query.toString();
    }

    /**
     * @return the conditions for entity container rows, matched on where they happened or where
     *         the entity is now, without the WHERE keyword
     */
    String entityContainerWhere(String transactionAlias, String entityAlias) {
        StringBuilder query = new StringBuilder();
        String transaction = transactionAlias + ".";
        String entity = entityAlias + ".";
        query.append(transaction).append("time > ").append(value(checkTime));
        if (userId != null) {
            query.append(" AND ").append(transaction).append(ConfigHandler.databaseType.getUserColumn()).append(" = ").append(value(userId));
        }
        if (location == null) {
            return query.toString();
        }

        int x = location.getBlockX();
        int y = location.getBlockY();
        int z = location.getBlockZ();
        int worldId = worldId();
        query.append(" AND ((").append(transaction).append("wid = ").append(value(worldId));
        if (radius > 0) {
            int minimumX = MessageAPI.clampToInt((long) x - radius);
            int maximumX = MessageAPI.clampToInt((long) x + radius);
            int minimumZ = MessageAPI.clampToInt((long) z - radius);
            int maximumZ = MessageAPI.clampToInt((long) z + radius);
            query.append(" AND ").append(transaction).append("x >= ").append(value(minimumX)).append(" AND ").append(transaction).append("x <= ").append(value(maximumX))
                    .append(" AND ").append(transaction).append("z >= ").append(value(minimumZ)).append(" AND ").append(transaction).append("z <= ").append(value(maximumZ));
            query.append(") OR (").append(entity).append("current_wid = ").append(value(worldId));
            query.append(" AND ").append(entity).append("x >= ").append(value(minimumX)).append(" AND ").append(entity).append("x < ").append(value((long) maximumX + 1L))
                    .append(" AND ").append(entity).append("z >= ").append(value(minimumZ)).append(" AND ").append(entity).append("z < ").append(value((long) maximumZ + 1L));
        }
        else {
            query.append(" AND ").append(transaction).append("x = ").append(value(x)).append(" AND ").append(transaction).append("y = ").append(value(y)).append(" AND ").append(transaction).append("z = ").append(value(z));
            query.append(") OR (").append(entity).append("current_wid = ").append(value(worldId));
            query.append(" AND ").append(entity).append("x >= ").append(value(x)).append(" AND ").append(entity).append("x < ").append(value((long) x + 1L))
                    .append(" AND ").append(entity).append("y >= ").append(value(y)).append(" AND ").append(entity).append("y < ").append(value((long) y + 1L))
                    .append(" AND ").append(entity).append("z >= ").append(value(z)).append(" AND ").append(entity).append("z < ").append(value((long) z + 1L));
        }
        query.append("))");
        return query.toString();
    }

    private int worldId() {
        return WorldUtils.getWorldId(location.getWorld().getName());
    }

    /**
     * @param clause
     *            the conditions the query applies to the table, without the WHERE keyword; on
     *            SQLite they decide which packed rows a page of the lookup shows
     * @param source
     *            the number the query tells this table's rows apart by, when it reads several
     * @return what the query reads the table from, under the alias if one is given
     */
    String table(Connection connection, String table, String alias, String clause, int source) throws java.sql.SQLException {
        if (ConfigHandler.databaseType.isSQLite() && SQLiteColdIndex.isSegmented(table)) {
            return packedTable(connection, table, alias, clause, source, true);
        }
        if (location == null) {
            return ConfigHandler.prefix + table + alias(alias);
        }

        int x = location.getBlockX();
        int z = location.getBlockZ();
        int minimumX = radius > 0 ? MessageAPI.clampToInt((long) x - radius) : x;
        int maximumX = radius > 0 ? MessageAPI.clampToInt((long) x + radius) : x;
        int minimumZ = radius > 0 ? MessageAPI.clampToInt((long) z - radius) : z;
        int maximumZ = radius > 0 ? MessageAPI.clampToInt((long) z + radius) : z;
        int worldId = WorldUtils.getWorldId(location.getWorld().getName());
        return DuckDBLookupQuery.spatialTable(connection, table, worldId, minimumX, maximumX, minimumZ, maximumZ, alias);
    }

    /**
     * @param clause
     *            the conditions the query applies to the table, without the WHERE keyword, as for
     *            {@link #table(Connection, String, String, String, int)}
     * @param source
     *            the number the query tells this table's rows apart by
     */
    String entityContainerTable(Connection connection, String alias, String clause, int source) throws Exception {
        if (ConfigHandler.databaseType.isSQLite()) {
            // The rows are matched on where the entity is now as well as where they happened, so
            // the coordinates cannot narrow down which segments to read.
            return packedTable(connection, "entity_container", alias, clause, source, false);
        }
        if (location == null || !ConfigHandler.databaseType.isDuckDB()) {
            return ConfigHandler.prefix + "entity_container" + alias(alias);
        }

        int x = location.getBlockX();
        int z = location.getBlockZ();
        int minimumX = radius > 0 ? MessageAPI.clampToInt((long) x - radius) : x;
        int maximumX = radius > 0 ? MessageAPI.clampToInt((long) x + radius) : x;
        int minimumZ = radius > 0 ? MessageAPI.clampToInt((long) z - radius) : z;
        int maximumZ = radius > 0 ? MessageAPI.clampToInt((long) z + radius) : z;
        int worldId = WorldUtils.getWorldId(location.getWorld().getName());
        List<Integer> entitySpawnRowIds = loadCurrentEntitySpawnRowIds(connection);
        return DuckDBSpatialIndex.tableExpression(
                connection,
                ConfigHandler.prefix,
                "entity_container",
                worldId,
                minimumX,
                maximumX,
                minimumZ,
                maximumZ,
                entitySpawnRowIds,
                Collections.emptySet(),
                alias
        );
    }

    private List<Integer> loadCurrentEntitySpawnRowIds(Connection connection) throws Exception {
        List<Integer> rowIds = new ArrayList<>();
        StringBuilder query = new StringBuilder("SELECT rowid FROM ").append(ConfigHandler.prefix).append("entity_spawn WHERE current_wid=?");
        if (radius > 0) {
            query.append(" AND x>=? AND x<? AND z>=? AND z<?");
        }
        else {
            query.append(" AND x>=? AND x<? AND y>=? AND y<? AND z>=? AND z<?");
        }
        try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
            int parameterIndex = 1;
            int x = location.getBlockX();
            int y = location.getBlockY();
            int z = location.getBlockZ();
            statement.setInt(parameterIndex++, WorldUtils.getWorldId(location.getWorld().getName()));
            if (radius > 0) {
                int minimumX = MessageAPI.clampToInt((long) x - radius);
                int maximumX = MessageAPI.clampToInt((long) x + radius);
                int minimumZ = MessageAPI.clampToInt((long) z - radius);
                int maximumZ = MessageAPI.clampToInt((long) z + radius);
                statement.setInt(parameterIndex++, minimumX);
                statement.setLong(parameterIndex++, (long) maximumX + 1L);
                statement.setInt(parameterIndex++, minimumZ);
                statement.setLong(parameterIndex, (long) maximumZ + 1L);
            }
            else {
                statement.setInt(parameterIndex++, x);
                statement.setLong(parameterIndex++, (long) x + 1L);
                statement.setInt(parameterIndex++, y);
                statement.setLong(parameterIndex++, (long) y + 1L);
                statement.setInt(parameterIndex++, z);
                statement.setLong(parameterIndex, (long) z + 1L);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next() && rowIds.size() <= 4_096) {
                    rowIds.add(resultSet.getInt(1));
                }
            }
        }
        return rowIds;
    }

    void appendLimit(StringBuilder query) {
        if (limitOffset >= 0 && limitCount >= 0) {
            query.append(" LIMIT ").append(limitCount).append(" OFFSET ").append(limitOffset);
        }
    }

    int bind(PreparedStatement statement) throws Exception {
        return bind(statement, 1);
    }

    int bind(PreparedStatement statement, int parameterIndex) throws Exception {
        if (literal()) {
            return parameterIndex; // the values are written into the clause
        }
        statement.setInt(parameterIndex++, checkTime);

        if (userId != null) {
            statement.setInt(parameterIndex++, userId);
        }

        if (location != null) {
            int x = location.getBlockX();
            int y = location.getBlockY();
            int z = location.getBlockZ();
            statement.setInt(parameterIndex++, WorldUtils.getWorldId(location.getWorld().getName()));

            if (radius > 0) {
                statement.setInt(parameterIndex++, MessageAPI.clampToInt((long) x - radius));
                statement.setInt(parameterIndex++, MessageAPI.clampToInt((long) x + radius));
                statement.setInt(parameterIndex++, MessageAPI.clampToInt((long) z - radius));
                statement.setInt(parameterIndex++, MessageAPI.clampToInt((long) z + radius));
            }
            else {
                statement.setInt(parameterIndex++, x);
                statement.setInt(parameterIndex++, y);
                statement.setInt(parameterIndex++, z);
            }
        }

        return parameterIndex;
    }

    int bindEntityContainer(PreparedStatement statement, int parameterIndex) throws Exception {
        if (literal()) {
            return parameterIndex; // the values are written into the clause
        }
        statement.setInt(parameterIndex++, checkTime);
        if (userId != null) {
            statement.setInt(parameterIndex++, userId);
        }
        if (location == null) {
            return parameterIndex;
        }

        int x = location.getBlockX();
        int y = location.getBlockY();
        int z = location.getBlockZ();
        int worldId = WorldUtils.getWorldId(location.getWorld().getName());
        statement.setInt(parameterIndex++, worldId);
        if (radius > 0) {
            int minimumX = MessageAPI.clampToInt((long) x - radius);
            int maximumX = MessageAPI.clampToInt((long) x + radius);
            int minimumZ = MessageAPI.clampToInt((long) z - radius);
            int maximumZ = MessageAPI.clampToInt((long) z + radius);
            statement.setInt(parameterIndex++, minimumX);
            statement.setInt(parameterIndex++, maximumX);
            statement.setInt(parameterIndex++, minimumZ);
            statement.setInt(parameterIndex++, maximumZ);
            statement.setInt(parameterIndex++, worldId);
            statement.setInt(parameterIndex++, minimumX);
            statement.setLong(parameterIndex++, (long) maximumX + 1L);
            statement.setInt(parameterIndex++, minimumZ);
            statement.setLong(parameterIndex++, (long) maximumZ + 1L);
        }
        else {
            statement.setInt(parameterIndex++, x);
            statement.setInt(parameterIndex++, y);
            statement.setInt(parameterIndex++, z);
            statement.setInt(parameterIndex++, worldId);
            statement.setInt(parameterIndex++, x);
            statement.setLong(parameterIndex++, (long) x + 1L);
            statement.setInt(parameterIndex++, y);
            statement.setLong(parameterIndex++, (long) y + 1L);
            statement.setInt(parameterIndex++, z);
            statement.setLong(parameterIndex++, (long) z + 1L);
        }
        return parameterIndex;
    }

    /**
     * Returns what a query should read a table from on SQLite, where activity is packed into
     * compressed storage minutes after it is logged: a placeholder for the live rows and the packed
     * ones together, filled in when the query is run by {@link #read}. Without this a plugin asking
     * through the API would see only what has not been packed yet.
     *
     * @param useLocation
     *            whether the lookup's location can narrow down which segments are read
     */
    private String packedTable(Connection connection, String table, String alias, String clause, int source, boolean useLocation) throws java.sql.SQLException {
        if (lookupConnection == null) {
            SQLiteColdIndex.beginLookup(checkTime, 0);
            lookupConnection = connection;
            packed = ColdLookupQuery.forLookup(connection, ConfigHandler.databaseType.getUserColumn());
        }
        if (packed == null) {
            return ConfigHandler.prefix + table + alias(alias);
        }

        int worldId = 0;
        Integer[] bounds = null;
        if (useLocation && location != null && location.getWorld() != null) {
            int x = location.getBlockX();
            int z = location.getBlockZ();
            int minimumX = radius > 0 ? MessageAPI.clampToInt((long) x - radius) : x;
            int maximumX = radius > 0 ? MessageAPI.clampToInt((long) x + radius) : x;
            int minimumZ = radius > 0 ? MessageAPI.clampToInt((long) z - radius) : z;
            int maximumZ = radius > 0 ? MessageAPI.clampToInt((long) z + radius) : z;
            worldId = worldId();
            bounds = new Integer[] { radius, minimumX, maximumX, null, null, minimumZ, maximumZ };
        }

        String placeholder = packed.source(table, clause, source, worldId, bounds, alias == null || alias.isEmpty() ? null : alias);
        sources.put(table, placeholder);
        return placeholder;
    }

    /**
     * @param table
     *            the unprefixed table the query reads
     * @return the index hint for a location lookup, which on SQLite is kept only if the table turns
     *         out to be read on its own, since an index can only be hinted for a plain table
     */
    String index(String table) {
        if (location == null || ConfigHandler.databaseType.isDuckDB()) {
            return "";
        }
        String source = sources.get(table);
        if (packed != null && source != null) {
            return packed.hint(source, WorldUtils.getWidIndex(table));
        }
        return WorldUtils.getWidIndex(table);
    }

    /**
     * Runs a query built from this filter's tables and clauses and hands each row to the reader.
     *
     * <p>
     * On SQLite with packed rows, a lookup with a limit reads only the page it shows, and one
     * without reads every row a bounded window of the history at a time, so neither copies more
     * packed rows out than one lookup may hold. Anywhere else the query runs as it is, with the
     * limit added and its parameters bound.
     * </p>
     *
     * @param body
     *            the query, without its order or limit
     * @param order
     *            the ORDER BY, with a leading space
     * @param binder
     *            binds the query's parameters where they are not written into it
     * @param reader
     *            receives each row
     */
    void read(Connection connection, String body, String order, Binder binder, RowReader reader) throws Exception {
        boolean limited = limitOffset >= 0 && limitCount >= 0;
        if (packed != null) {
            if (limited && limitCount == 0) {
                return;
            }
            try (Statement statement = connection.createStatement();
                    ResultSet results = limited ? packed.page(statement, body, order, limitOffset, limitCount, null) : packed.all(statement, body, order, null)) {
                while (results.next()) {
                    reader.read(results);
                }
            }
            return;
        }

        StringBuilder query = new StringBuilder(body).append(order);
        appendLimit(query);
        try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
            binder.bind(statement);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    reader.read(results);
                }
            }
        }
    }

    /**
     * Ends the lookup of packed rows, if one was opened, and lets go of what it read.
     */
    void close() {
        if (lookupConnection != null) {
            Connection connection = lookupConnection;
            lookupConnection = null;
            packed = null;
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private static String alias(String alias) {
        return alias == null || alias.isEmpty() ? "" : " AS " + alias;
    }
}
