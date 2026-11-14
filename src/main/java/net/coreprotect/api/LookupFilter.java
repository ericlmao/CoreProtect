package net.coreprotect.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.ColdLookupQuery;
import net.coreprotect.database.DuckDBLookupQuery;
import net.coreprotect.database.DuckDBSpatialIndex;
import net.coreprotect.database.LocationQuery;
import net.coreprotect.database.SQLiteColdIndex;
import net.coreprotect.model.action.LookupActions;
import net.coreprotect.utility.ItemUtils;
import net.coreprotect.utility.MaterialUtils;
import net.coreprotect.utility.StringUtils;
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
    private final World world;
    private final int radius;
    private final int limitOffset;
    private final int limitCount;
    private final List<Material> includeMaterials;
    private final List<Material> excludeMaterials;
    private final Map<Integer, Material> materialTypes;
    private final String includeUserIds;
    private final String excludeUserIds;

    /** The connection a lookup of packed rows was opened on, or null when none was. */
    private Connection lookupConnection;

    /** The query that reads packed rows, or null when there are none to read. */
    private ColdLookupQuery packed;

    /** The placeholder handed out for each table the query reads packed rows from. */
    private final Map<String, String> sources = new HashMap<>();

    /** Reads the parameters of a query that is run with them. */
    @FunctionalInterface
    interface Binder {
        /**
         * @return the index of the next parameter
         */
        int bind(PreparedStatement statement) throws Exception;
    }

    /** Receives each row a lookup finds. */
    @FunctionalInterface
    interface RowReader {
        void read(ResultSet results) throws Exception;
    }

    private LookupFilter(Integer userId, int checkTime, Location location, World world, int radius, int limitOffset, int limitCount, List<Material> includeMaterials, List<Material> excludeMaterials, Map<Integer, Material> materialTypes, String includeUserIds, String excludeUserIds) {
        this.userId = userId;
        this.checkTime = checkTime;
        this.location = location;
        this.world = location == null ? world : location.getWorld();
        this.radius = radius;
        this.limitOffset = limitOffset;
        this.limitCount = limitCount;
        this.includeMaterials = includeMaterials;
        this.excludeMaterials = excludeMaterials;
        this.materialTypes = materialTypes;
        this.includeUserIds = includeUserIds;
        this.excludeUserIds = excludeUserIds;
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

        Map<Integer, Material> materialTypes = new HashMap<>();
        if (!options.getIncludeMaterials().isEmpty() || !options.getExcludeMaterials().isEmpty()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT id, material FROM " + ConfigHandler.prefix + "material_map");
                    ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    materialTypes.put(results.getInt("id"), MaterialUtils.getTypeFromStoredName(results.getString("material")));
                }
            }
        }

        return new LookupFilter(userId, checkTime, options.getLocation(), options.getWorld(), options.getRadius(), options.getLimitOffset(), options.getLimitCount(),
                options.getIncludeMaterials(), options.getExcludeMaterials(), materialTypes,
                userIds(connection, options.getUsers()), userIds(connection, options.getExcludeUsers()));
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

    void appendWhere(StringBuilder query) {
        appendWhere(query, "");
    }

    void appendWhere(StringBuilder query, String alias) {
        query.append("WHERE ").append(where(alias));
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
        appendUserWhere(query, alias, includeUserIds, excludeUserIds);

        if (world != null) {
            query.append(" AND ").append(LocationQuery.predicate(qualifier + "wid", " = " + value(worldId())));
        }
        if (location != null) {
            int x = location.getBlockX();
            int y = location.getBlockY();
            int z = location.getBlockZ();
            if (radius > 0) {
                query.append(" AND ").append(LocationQuery.predicate(qualifier + "x", " >= " + value(MessageAPI.clampToInt((long) x - radius))))
                        .append(" AND ").append(LocationQuery.predicate(qualifier + "x", " <= " + value(MessageAPI.clampToInt((long) x + radius))))
                        .append(" AND ").append(LocationQuery.predicate(qualifier + "z", " >= " + value(MessageAPI.clampToInt((long) z - radius))))
                        .append(" AND ").append(LocationQuery.predicate(qualifier + "z", " <= " + value(MessageAPI.clampToInt((long) z + radius))));
            }
            else {
                query.append(" AND ").append(LocationQuery.predicate(qualifier + "x", " = " + value(x)))
                        .append(" AND ").append(qualifier).append("y = ").append(value(y)).append(" AND ").append(LocationQuery.predicate(qualifier + "z", " = " + value(z)));
            }
        }
        return query.toString();
    }

    private int worldId() {
        return WorldUtils.getWorldId(world.getName());
    }

    void appendEntityContainerWhere(StringBuilder query, String transactionAlias, String entityAlias) {
        query.append("WHERE ").append(entityContainerWhere(transactionAlias, entityAlias));
    }

    /**
     * @return the conditions for entity container rows, matched on where they happened or where
     *         the entity is now, without the WHERE keyword
     */
    String entityContainerWhere(String transactionAlias, String entityAlias) {
        return trackedEntityWhere(transactionAlias, entityAlias, null);
    }

    void appendEntityWhere(Connection connection, StringBuilder query, String alias) {
        query.append("WHERE ").append(entityWhere(connection, alias));
    }

    /**
     * @return the conditions for entity spawn and kill rows, matched on where they happened or
     *         where the spawned entity is now, without the WHERE keyword
     */
    String entityWhere(Connection connection, String alias) {
        if (literal()) {
            return packedEntityWhere(alias, "spawn_rows");
        }
        return trackedEntityWhere(alias, "spawn_rows", table(connection, "block", "original_rows"));
    }

    private String trackedEntityWhere(String transactionAlias, String entityAlias, String originalTable) {
        StringBuilder query = new StringBuilder();
        String transaction = transactionAlias + ".";
        String entity = entityAlias + ".";
        query.append(transaction).append("time > ").append(value(checkTime));
        if (userId != null) {
            query.append(" AND ").append(transaction).append(ConfigHandler.databaseType.getUserColumn()).append(" = ").append(value(userId));
        }
        appendUserWhere(query, transactionAlias, includeUserIds, excludeUserIds);
        if (world == null) {
            return query.toString();
        }

        int worldId = worldId();
        String original = originalTable == null ? transaction : "original_rows.";
        String tracked = ") OR (";
        String ending = "))";
        if (originalTable != null) {
            String trackingId = ConfigHandler.databaseType.isClickHouse() ? "accurateCastOrNull(linked_rows.data, 'UInt64')" : "linked_rows.data";
            String trackedRows = "SELECT " + entity + "block_rowid FROM " + ConfigHandler.prefix + "entity_spawn " + entityAlias
                    + " INNER JOIN " + ConfigHandler.prefix + "block linked_rows ON linked_rows.rowid=" + entity + "block_rowid AND " + trackingId + "=" + entity + "rowid"
                    + " AND linked_rows.action=" + LookupActions.ENTITY_SPAWN + " WHERE (";
            if (location == null) {
                query.append(" AND (").append(LocationQuery.predicate(transaction + "wid", " = " + value(worldId)))
                        .append(" OR ").append(transaction).append("rowid IN (").append(trackedRows).append(entity).append("current_wid = ").append(value(worldId)).append(")))");
                return query.toString();
            }
            // Separate location candidates keep both spatial indexes usable, including on MySQL.
            query.append(" AND ").append(transaction).append("rowid IN (SELECT rowid FROM (SELECT original_rows.rowid FROM ").append(originalTable).append(" WHERE (");
            tracked = ") UNION ALL " + trackedRows;
            ending = ")) entity_locations)";
        }
        else {
            query.append(" AND ((");
        }

        query.append(LocationQuery.predicate(original + "wid", " = " + value(worldId)));
        if (location == null) {
            query.append(tracked).append(entity).append("current_wid = ").append(value(worldId)).append(ending);
            return query.toString();
        }

        query.append(originalLocation(original));
        query.append(tracked).append(entity).append("current_wid = ").append(value(worldId));
        query.append(currentLocation(entity));
        query.append(ending);
        return query.toString();
    }

    /**
     * The entity spawn and kill conditions for SQLite, where the block rows are packed into
     * compressed storage minutes after they are logged. The live block table cannot be joined to
     * find them, so a spawn row is tied to its entity through the spawn record alone: the record
     * names the row, and the row names the record.
     */
    private String packedEntityWhere(String transactionAlias, String entityAlias) {
        StringBuilder query = new StringBuilder();
        String transaction = transactionAlias + ".";
        String entity = entityAlias + ".";
        query.append(transaction).append("time > ").append(checkTime);
        if (userId != null) {
            query.append(" AND ").append(transaction).append(ConfigHandler.databaseType.getUserColumn()).append(" = ").append(userId);
        }
        appendUserWhere(query, transactionAlias, includeUserIds, excludeUserIds);
        if (world == null) {
            return query.toString();
        }

        int worldId = worldId();
        query.append(" AND ((").append(transaction).append("wid = ").append(worldId);
        if (location != null) {
            query.append(originalLocation(transaction));
        }
        query.append(") OR (").append(transaction).append("action = ").append(LookupActions.ENTITY_SPAWN)
                .append(" AND EXISTS (SELECT 1 FROM ").append(ConfigHandler.prefix).append("entity_spawn ").append(entityAlias)
                .append(" WHERE ").append(entity).append("block_rowid = ").append(transaction).append("rowid AND ")
                .append(entity).append("rowid = ").append(transaction).append("data AND ")
                .append(entity).append("current_wid = ").append(worldId);
        if (location != null) {
            query.append(currentLocation(entity));
        }
        query.append(")))");
        return query.toString();
    }

    /** The coordinates a row was logged at, for a location lookup. */
    private String originalLocation(String original) {
        int x = location.getBlockX();
        int y = location.getBlockY();
        int z = location.getBlockZ();
        StringBuilder query = new StringBuilder();
        if (radius > 0) {
            query.append(" AND ").append(LocationQuery.predicate(original + "x", " >= " + value(MessageAPI.clampToInt((long) x - radius))))
                    .append(" AND ").append(LocationQuery.predicate(original + "x", " <= " + value(MessageAPI.clampToInt((long) x + radius))))
                    .append(" AND ").append(LocationQuery.predicate(original + "z", " >= " + value(MessageAPI.clampToInt((long) z - radius))))
                    .append(" AND ").append(LocationQuery.predicate(original + "z", " <= " + value(MessageAPI.clampToInt((long) z + radius))));
        }
        else {
            query.append(" AND ").append(LocationQuery.predicate(original + "x", " = " + value(x)))
                    .append(" AND ").append(original).append("y = ").append(value(y)).append(" AND ").append(LocationQuery.predicate(original + "z", " = " + value(z)));
        }
        return query.toString();
    }

    /** The block an entity's persisted current position falls in, for a location lookup. */
    private String currentLocation(String entity) {
        int x = location.getBlockX();
        int y = location.getBlockY();
        int z = location.getBlockZ();
        StringBuilder query = new StringBuilder();
        if (radius > 0) {
            int minimumX = MessageAPI.clampToInt((long) x - radius);
            int maximumX = MessageAPI.clampToInt((long) x + radius);
            int minimumZ = MessageAPI.clampToInt((long) z - radius);
            int maximumZ = MessageAPI.clampToInt((long) z + radius);
            query.append(" AND ").append(entity).append("x >= ").append(value(minimumX)).append(" AND ").append(entity).append("x < ").append(value((long) maximumX + 1L))
                    .append(" AND ").append(entity).append("z >= ").append(value(minimumZ)).append(" AND ").append(entity).append("z < ").append(value((long) maximumZ + 1L));
        }
        else {
            query.append(" AND ").append(entity).append("x >= ").append(value(x)).append(" AND ").append(entity).append("x < ").append(value((long) x + 1L))
                    .append(" AND ").append(entity).append("y >= ").append(value(y)).append(" AND ").append(entity).append("y < ").append(value((long) y + 1L))
                    .append(" AND ").append(entity).append("z >= ").append(value(z)).append(" AND ").append(entity).append("z < ").append(value((long) z + 1L));
        }
        return query.toString();
    }

    static void appendActionWhere(StringBuilder query, String alias, int[] actions) {
        if (actions.length > 0) {
            StringJoiner ids = new StringJoiner(",");
            for (int action : actions) {
                ids.add(String.valueOf(action));
            }
            query.append(" AND ").append(alias.isEmpty() ? "" : alias + ".").append("action IN (").append(ids).append(")");
        }
    }

    void appendMaterialWhere(StringBuilder query) {
        appendMaterialWhere(query, "");
    }

    void appendMaterialWhere(StringBuilder query, String alias) {
        appendMaterialWhere(query, alias, false);
    }

    void appendMaterialWhere(StringBuilder query, String alias, boolean inventoryBlock) {
        String qualifier = alias.isEmpty() ? "" : alias + ".";
        if (!includeMaterials.isEmpty()) {
            query.append(" AND ").append(qualifier).append("type IN (").append(materialIds(includeMaterials, inventoryBlock)).append(")");
        }
        if (!excludeMaterials.isEmpty()) {
            query.append(" AND ").append(qualifier).append("type NOT IN (").append(materialIds(excludeMaterials, inventoryBlock)).append(")");
        }
    }

    void appendBlockMaterialWhere(StringBuilder query) {
        String entityActions = LookupActions.ENTITY_KILL + "," + LookupActions.ENTITY_SPAWN;
        if (!includeMaterials.isEmpty()) {
            query.append(" AND action NOT IN (").append(entityActions).append(") AND ").append(blockMaterialPredicate(includeMaterials));
        }
        if (!excludeMaterials.isEmpty()) {
            query.append(" AND (action IN (").append(entityActions).append(") OR NOT ").append(blockMaterialPredicate(excludeMaterials)).append(")");
        }
    }

    private String blockMaterialPredicate(List<Material> materials) {
        List<Material> blockMaterials = new ArrayList<>(materials);
        blockMaterials.removeAll(List.of(Material.STONE));
        return "(type IN (" + materialIds(blockMaterials, false) + ") OR " + legacyStonePredicate(materials) + ")";
    }

    private String legacyStonePredicate(List<Material> materials) {
        StringJoiner stoneData = new StringJoiner(",");
        for (int data = 1; data <= 6; data++) {
            Material material = Material.getMaterial(StringUtils.nameFilter("stone", data).toUpperCase(Locale.ROOT));
            if (materials.contains(material)) {
                stoneData.add(String.valueOf(data));
            }
        }

        StringJoiner predicates = new StringJoiner(" OR ");
        if (materials.contains(Material.STONE)) {
            predicates.add("COALESCE(data,0) NOT BETWEEN 1 AND 6");
        }
        if (stoneData.length() > 0) {
            predicates.add("COALESCE(data,0) IN (" + stoneData + ")");
        }
        if (predicates.length() == 0) {
            return "1 = 0";
        }

        return "(type IN (" + materialIds(List.of(Material.STONE), false) + ") AND (" + predicates + "))";
    }

    String table(Connection connection, String table, String alias) {
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
     *            the conditions the query applies to the table, without the WHERE keyword; on
     *            SQLite they decide which packed rows a page of the lookup shows
     * @param source
     *            the number the query tells this table's rows apart by, when it reads several
     * @return what the query reads the table from, under the alias if one is given
     */
    String table(Connection connection, String table, String alias, String clause, int source) throws SQLException {
        if (ConfigHandler.databaseType.isSQLite() && SQLiteColdIndex.isSegmented(table)) {
            return packedTable(connection, table, alias, clause, source, true);
        }
        return table(connection, table, alias);
    }

    String entityContainerTable(Connection connection, String alias) throws Exception {
        return trackedEntityTable(connection, "entity_container", alias);
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
        return entityContainerTable(connection, alias);
    }

    String entityTable(Connection connection, String alias) throws Exception {
        return trackedEntityTable(connection, "block", alias);
    }

    /**
     * @param clause
     *            the conditions the query applies to the table, without the WHERE keyword, as for
     *            {@link #table(Connection, String, String, String, int)}
     */
    String entityTable(Connection connection, String alias, String clause) throws Exception {
        if (ConfigHandler.databaseType.isSQLite()) {
            // A spawned entity is matched on where it is now as well as where it was spawned, so
            // the coordinates cannot narrow down which segments to read.
            return packedTable(connection, "block", alias, clause, 0, false);
        }
        return entityTable(connection, alias);
    }

    private String trackedEntityTable(Connection connection, String table, String alias) throws Exception {
        if (location == null || !ConfigHandler.databaseType.isDuckDB()) {
            return ConfigHandler.prefix + table + alias(alias);
        }

        int x = location.getBlockX();
        int z = location.getBlockZ();
        int minimumX = radius > 0 ? MessageAPI.clampToInt((long) x - radius) : x;
        int maximumX = radius > 0 ? MessageAPI.clampToInt((long) x + radius) : x;
        int minimumZ = radius > 0 ? MessageAPI.clampToInt((long) z - radius) : z;
        int maximumZ = radius > 0 ? MessageAPI.clampToInt((long) z + radius) : z;
        int worldId = WorldUtils.getWorldId(location.getWorld().getName());
        boolean block = table.equals("block");
        List<Long> rowIds = loadCurrentEntityRowIds(connection, block ? "block_rowid" : "rowid");
        return DuckDBSpatialIndex.tableExpression(
                connection,
                ConfigHandler.prefix,
                table,
                worldId,
                minimumX,
                maximumX,
                minimumZ,
                maximumZ,
                block ? Collections.emptySet() : rowIds.stream().map(Long::intValue).collect(Collectors.toList()),
                block ? rowIds : Collections.emptySet(),
                alias
        );
    }

    private List<Long> loadCurrentEntityRowIds(Connection connection, String column) throws Exception {
        List<Long> rowIds = new ArrayList<>();
        StringBuilder query = new StringBuilder("SELECT ").append(column).append(" FROM ").append(ConfigHandler.prefix).append("entity_spawn WHERE current_wid=?");
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
                    long rowId = resultSet.getLong(1);
                    if (!resultSet.wasNull()) {
                        rowIds.add(rowId);
                    }
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

        if (world != null) {
            statement.setInt(parameterIndex++, WorldUtils.getWorldId(world.getName()));
        }

        if (location != null) {
            int x = location.getBlockX();
            int y = location.getBlockY();
            int z = location.getBlockZ();
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
        return bindTrackedEntity(statement, parameterIndex);
    }

    int bindTrackedEntity(PreparedStatement statement, int parameterIndex) throws Exception {
        if (literal()) {
            return parameterIndex; // the values are written into the clause
        }
        statement.setInt(parameterIndex++, checkTime);
        if (userId != null) {
            statement.setInt(parameterIndex++, userId);
        }
        if (world == null) {
            return parameterIndex;
        }

        if (location == null) {
            int worldId = WorldUtils.getWorldId(world.getName());
            statement.setInt(parameterIndex++, worldId);
            statement.setInt(parameterIndex++, worldId);
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

    static String userIds(Connection connection, List<String> users) throws Exception {
        StringJoiner result = new StringJoiner(",");
        for (String user : users) {
            Integer id = MessageAPI.getUserId(connection, user);
            if (id == null) {
                // An empty name or #global matches every user.
                return null;
            }
            result.add(String.valueOf(id));
        }
        return result.toString();
    }

    static void appendUserWhere(StringBuilder query, String alias, String includeUserIds, String excludeUserIds) {
        String column = (alias.isEmpty() ? "" : alias + ".") + ConfigHandler.databaseType.getUserColumn();
        if (includeUserIds != null && !includeUserIds.isEmpty()) {
            query.append(" AND ").append(column).append(" IN (").append(includeUserIds).append(")");
        }
        if (excludeUserIds == null) {
            query.append(" AND 1 = 0");
        }
        else if (!excludeUserIds.isEmpty()) {
            query.append(" AND ").append(column).append(" NOT IN (").append(excludeUserIds).append(")");
        }
    }

    private String materialIds(List<Material> materials, boolean inventoryBlock) {
        StringJoiner result = new StringJoiner(",");
        for (Map.Entry<Integer, Material> entry : materialTypes.entrySet()) {
            Material material = inventoryBlock ? ItemUtils.itemFilter(entry.getValue(), true) : entry.getValue();
            if (material != null && materials.contains(material)) {
                result.add(String.valueOf(entry.getKey()));
            }
        }
        return result.length() == 0 ? "-1" : result.toString();
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
    private String packedTable(Connection connection, String table, String alias, String clause, int source, boolean useLocation) throws SQLException {
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
            worldId = WorldUtils.getWorldId(location.getWorld().getName());
            bounds = new Integer[] { radius, minimumX, maximumX, null, null, minimumZ, maximumZ };
        }
        else if (useLocation && location == null && world != null) {
            worldId = worldId();
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
        read(connection, body, order, binder, null, reader);
    }

    /**
     * Runs a query as {@link #read(Connection, String, String, Binder, RowReader)} does, for a query
     * that also has text parameters of its own, such as message filters.
     *
     * @param bindings
     *            the query's text parameters, in order, after any the binder binds; on SQLite the
     *            binder binds nothing and these are the only parameters
     */
    void read(Connection connection, String body, String order, Binder binder, List<String> bindings, RowReader reader) throws Exception {
        boolean limited = limitOffset >= 0 && limitCount >= 0;
        if (packed != null) {
            if (limited && limitCount == 0) {
                return;
            }
            try (Statement statement = connection.createStatement();
                    ResultSet results = limited ? packed.page(statement, body, order, limitOffset, limitCount, bindings) : packed.all(statement, body, order, bindings)) {
                while (results.next()) {
                    reader.read(results);
                }
            }
            return;
        }

        StringBuilder query = new StringBuilder(body).append(order);
        appendLimit(query);
        try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
            int parameterIndex = binder.bind(statement);
            if (bindings != null) {
                for (String binding : bindings) {
                    statement.setString(parameterIndex++, binding);
                }
            }
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
