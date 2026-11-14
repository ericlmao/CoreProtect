package net.coreprotect.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.bukkit.Location;
import org.bukkit.block.Block;

import net.coreprotect.api.result.BlockResult;
import net.coreprotect.api.result.ContainerResult;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.Database;
import net.coreprotect.database.DuckDBLookupQuery;
import net.coreprotect.database.lookup.InspectorSource;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.utility.BlockUtils;
import net.coreprotect.utility.DatabaseUtils;
import net.coreprotect.utility.StringUtils;
import net.coreprotect.utility.WorldUtils;
import net.coreprotect.utility.ErrorReporter;

/**
 * Provides API methods for block-related lookups in the CoreProtect database.
 */
public class BlockAPI {

    /**
     * Private constructor to prevent instantiation.
     * This is a utility class with static methods only.
     */
    private BlockAPI() {
        throw new IllegalStateException("API class");
    }

    /**
     * Performs a lookup of block-related actions at the specified block.
     * 
     * @param block
     *            The block to look up
     * @param offset
     *            Time constraint in seconds (0 means no time constraint)
     * @return List of results in a String array format
     */
    public static List<String[]> performLookup(Block block, int offset) {
        List<String[]> result = new ArrayList<>();

        if (!Config.getGlobal().API_ENABLED) {
            return result;
        }

        if (block == null) {
            return result;
        }

        try (Connection connection = Database.getConnection(false, 1000)) {
            if (connection == null) {
                return result;
            }

            int x = block.getX();
            int y = block.getY();
            int z = block.getZ();
            int time = (int) (System.currentTimeMillis() / 1000L);
            int worldId = WorldUtils.getWorldId(block.getWorld().getName());
            int checkTime = 0;

            if (offset > 0) {
                checkTime = time - offset;
            }

            // On SQLite most of a block's history has been packed into compressed storage, and is
            // read together with the live rows, the way the inspector reads it. A busy block can
            // hold more packed rows than one lookup may copy out at once, so every row is read a
            // bounded window of the history at a time.
            InspectorSource source = ConfigHandler.databaseType.isSQLite() ? InspectorSource.open(connection, "block", worldId, x, x, z, z, checkTime > 0 ? checkTime + 1 : 0) : null;
            try {
                try (Statement statement = connection.createStatement()) {
                    String columns = "time," + ConfigHandler.databaseType.getUserColumn() + ",action,type,data,blockdata,rolled_back";
                    String where = "wid = " + worldId + " AND x = " + x + " AND z = " + z + " AND y = " + y + " AND time > " + checkTime;
                    ResultSet rows;
                    if (source != null) {
                        rows = source.all(columns, where, ConfigHandler.getDescendingEventOrder());
                    }
                    else {
                        String table = DuckDBLookupQuery.spatialTable(connection, "block", worldId, x, x, z, z, "spatial_rows");
                        String index = ConfigHandler.databaseType.isDuckDB() ? "" : WorldUtils.getWidIndex("block");
                        rows = statement.executeQuery("SELECT " + columns + " FROM " + table + " " + index + "WHERE " + where + " ORDER BY " + ConfigHandler.getDescendingEventOrder());
                    }

                    try (ResultSet results = rows) {
                        while (results.next()) {
                            String resultTime = results.getString("time");
                            int resultUserId = results.getInt("user");
                            String resultAction = results.getString("action");
                            int resultType = results.getInt("type");
                            String resultData = results.getString("data");
                            byte[] resultBlockData = DatabaseUtils.getBytes(results, "blockdata");
                            String resultRolledBack = results.getString("rolled_back");

                            String resultUser = UserStatement.getName(connection, resultUserId);
                            String blockData = BlockUtils.byteDataToString(resultBlockData, resultType);

                            String[] lookupData = new String[] { resultTime, resultUser, String.valueOf(x), String.valueOf(y), String.valueOf(z), String.valueOf(resultType), resultData, resultAction, resultRolledBack, String.valueOf(worldId), blockData };

                            result.add(StringUtils.toStringArray(lookupData));
                        }
                    }
                }
            }
            finally {
                if (source != null) {
                    source.close();
                }
            }
        }
        catch (Exception e) {
            ErrorReporter.report(e);
        }

        return result;
    }

    /**
     * Performs a typed lookup of block-related actions at the specified block.
     *
     * @param block
     *            The block to look up
     * @param options
     *            Lookup options. User, time, and limit are applied; location and radius are ignored because the block supplies the exact location.
     * @return List of results in a BlockResult format
     */
    public static List<BlockResult> performLookup(Block block, LookupOptions options) {
        List<BlockResult> result = new ArrayList<>();

        if (!Config.getGlobal().API_ENABLED) {
            return result;
        }

        if (block == null || block.getWorld() == null) {
            return result;
        }

        if (options == null) {
            options = LookupOptions.builder().build();
        }

        try (Connection connection = Database.getConnection(false, 1000)) {
            if (connection == null) {
                return result;
            }

            Integer userId = MessageAPI.getUserId(connection, options.getUser());
            if (userId != null && userId == -1) {
                return result;
            }

            int checkTime = 0;
            if (options.getTime() > 0) {
                checkTime = (int) (System.currentTimeMillis() / 1000L) - options.getTime();
            }

            int x = block.getX();
            int y = block.getY();
            int z = block.getZ();
            String worldName = block.getWorld().getName();
            int worldId = WorldUtils.getWorldId(worldName);

            InspectorSource source = ConfigHandler.databaseType.isSQLite() ? InspectorSource.open(connection, "block", worldId, x, x, z, z, checkTime > 0 ? checkTime + 1 : 0) : null;
            try {
                String columns = "time," + ConfigHandler.databaseType.getUserColumn() + ",action,type,data,blockdata,rolled_back,wid,x,y,z";
                if (source != null) {
                    // The packed history is read a page at a time when a page is asked for, copying
                    // out only that page's rows, and a bounded window at a time when every row is.
                    String where = "wid = " + worldId + " AND x = " + x + " AND z = " + z + " AND y = " + y + " AND time > " + checkTime
                            + (userId != null ? " AND " + ConfigHandler.databaseType.getUserColumn() + " = " + userId : "");
                    if (!options.hasLimit() || options.getLimitCount() > 0) {
                        try (ResultSet results = options.hasLimit() ? source.page(columns, where, ConfigHandler.getDescendingEventOrder(), options.getLimitOffset(), options.getLimitCount())
                                : source.all(columns, where, ConfigHandler.getDescendingEventOrder())) {
                            while (results.next()) {
                                result.add(parseBlockResult(connection, results, worldName));
                            }
                        }
                    }
                    return result;
                }

                StringBuilder query = new StringBuilder("SELECT " + columns + " FROM ");
                query.append(DuckDBLookupQuery.spatialTable(connection, "block", worldId, x, x, z, z, "spatial_rows")).append(' ');
                if (!ConfigHandler.databaseType.isDuckDB()) {
                    query.append(WorldUtils.getWidIndex("block"));
                }
                query.append("WHERE wid = ? AND x = ? AND z = ? AND y = ? AND time > ?");
                if (userId != null) {
                    query.append(" AND ").append(ConfigHandler.databaseType.getUserColumn()).append(" = ?");
                }
                query.append(" ORDER BY ").append(ConfigHandler.getDescendingEventOrder());
                if (options.hasLimit()) {
                    query.append(" LIMIT ").append(options.getLimitCount()).append(" OFFSET ").append(options.getLimitOffset());
                }

                try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
                    statement.setInt(1, worldId);
                    statement.setInt(2, x);
                    statement.setInt(3, z);
                    statement.setInt(4, y);
                    statement.setInt(5, checkTime);
                    if (userId != null) {
                        statement.setInt(6, userId);
                    }

                    try (ResultSet results = statement.executeQuery()) {
                        while (results.next()) {
                            result.add(parseBlockResult(connection, results, worldName));
                        }
                    }
                }
            }
            finally {
                if (source != null) {
                    source.close();
                }
            }
        }
        catch (Exception e) {
            ErrorReporter.report(e);
        }

        return result;
    }

    /**
     * Performs a lookup of container transactions at the specified location.
     * 
     * @param location
     *            The location to look up
     * @param offset
     *            Time constraint in seconds (0 means no time constraint)
     * @return List of results in a ContainerResult format
     */
    public static List<ContainerResult> performContainerLookup(Location location, int offset) {
        if (location == null) {
            return new ArrayList<>();
        }

        return performContainerLookup(LookupOptions.builder().time(offset).location(location).build());
    }

    /**
     * Performs a lookup of container transactions using shared lookup options.
     *
     * @param options
     *            Lookup options. Tracked entity-container rows match their original or persisted current/final location and return the persisted
     *            current/final location.
     * @return List of results in a ContainerResult format
     */
    public static List<ContainerResult> performContainerLookup(LookupOptions options) {
        List<ContainerResult> result = new ArrayList<>();

        if (!Config.getGlobal().API_ENABLED) {
            return result;
        }

        try (Connection connection = Database.getConnection(false, 1000)) {
            if (connection == null) {
                return result;
            }

            LookupFilter filter = LookupFilter.fromOptions(connection, options);
            if (filter.hasInvalidUser() || filter.hasInvalidLocation()) {
                return result;
            }

            try {
                boolean snapshot = filter.beginDuckDBSnapshot(connection);
                try {
                    String containerWhere = filter.where("container_rows");
                    String entityWhere = filter.entityContainerWhere("entity_rows", "spawn_rows");

                    StringBuilder query = new StringBuilder("SELECT * FROM (");
                    query.append("SELECT 0 AS source,container_rows.rowid AS id,container_rows.time,container_rows.").append(ConfigHandler.databaseType.getUserColumn()).append(",container_rows.wid,container_rows.x,container_rows.y,container_rows.z,container_rows.action,container_rows.type,container_rows.data,container_rows.amount,container_rows.metadata,container_rows.rolled_back FROM ")
                            .append(filter.table(connection, "container", "container_rows", containerWhere, 0)).append(" WHERE ").append(containerWhere);
                    query.append(" UNION ALL ");
                    query.append("SELECT 1 AS source,entity_rows.rowid AS id,entity_rows.time,entity_rows.").append(ConfigHandler.databaseType.getUserColumn()).append(",spawn_rows.current_wid AS wid,spawn_rows.x,spawn_rows.y,spawn_rows.z,entity_rows.action,entity_rows.type,entity_rows.data,entity_rows.amount,entity_rows.metadata,entity_rows.rolled_back FROM ")
                            .append(filter.entityContainerTable(connection, "entity_rows", entityWhere, 1)).append(" JOIN ").append(ConfigHandler.prefix).append("entity_spawn spawn_rows ON spawn_rows.rowid=entity_rows.entity_spawn_rowid WHERE ").append(entityWhere);
                    query.append(") AS container_lookup");

                    filter.read(connection, query.toString(), " ORDER BY time DESC,source DESC,id DESC", statement -> {
                        int parameterIndex = filter.bind(statement);
                        filter.bindEntityContainer(statement, parameterIndex);
                    }, results -> result.add(parseContainerResult(connection, results)));
                }
                finally {
                    filter.endDuckDBSnapshot(connection, snapshot);
                }
            }
            finally {
                filter.close();
            }
        }
        catch (Exception e) {
            ErrorReporter.report(e);
        }

        return result;
    }

    private static ContainerResult parseContainerResult(Connection connection, ResultSet results) throws Exception {
        int resultUserId = results.getInt("user");
        String resultUser = UserStatement.getName(connection, resultUserId);

        return new ContainerResult(
                results.getLong("time"), resultUser, WorldUtils.getWorldName(results.getInt("wid")), (int) Math.floor(results.getDouble("x")), (int) Math.floor(results.getDouble("y")), (int) Math.floor(results.getDouble("z")),
                results.getInt("type"), results.getInt("data"), results.getInt("amount"), DatabaseUtils.getBlobBytes(results, "metadata"),
                results.getInt("action"), results.getInt("rolled_back")
        );
    }

    private static BlockResult parseBlockResult(Connection connection, ResultSet results, String worldName) throws Exception {
        int resultUserId = results.getInt("user");
        String resultUser = UserStatement.getName(connection, resultUserId);

        int resultType = results.getInt("type");
        String blockData = BlockUtils.byteDataToString(DatabaseUtils.getBytes(results, "blockdata"), resultType);

        return new BlockResult(
                results.getLong("time"), resultUser, worldName, results.getInt("x"), results.getInt("y"), results.getInt("z"),
                resultType, results.getInt("data"), blockData, results.getInt("action"), results.getInt("rolled_back")
        );
    }

}
