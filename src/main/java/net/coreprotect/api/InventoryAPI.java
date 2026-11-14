package net.coreprotect.api;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import net.coreprotect.api.result.InventoryResult;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.Database;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.model.item.InventorySources;
import net.coreprotect.utility.WorldUtils;
import net.coreprotect.utility.DatabaseUtils;
import net.coreprotect.utility.ErrorReporter;

/**
 * Provides API methods for normalized player inventory transaction lookups. Tracked entity-container rows match their original or persisted
 * current/final location and expose the persisted current/final location as container-source results.
 */
public class InventoryAPI {

    private InventoryAPI() {
        throw new IllegalStateException("API class");
    }

    public static List<InventoryResult> performLookup(LookupOptions options) {
        List<InventoryResult> result = new ArrayList<>();

        if (!Config.getGlobal().API_ENABLED) {
            return result;
        }

        if (options == null) {
            options = LookupOptions.builder().build();
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
                    List<InventoryAction> actions = options.getInventoryActions();
                    StringBuilder whereBuilder = new StringBuilder(filter.where(""));
                    StringBuilder blockWhereBuilder = new StringBuilder(whereBuilder);
                    filter.appendMaterialWhere(blockWhereBuilder, "", true);
                    blockWhereBuilder.append(" AND action = 1");
                    appendActionWhere(blockWhereBuilder, "", actions, InventorySources.BLOCK);
                    filter.appendMaterialWhere(whereBuilder);
                    StringBuilder containerWhereBuilder = new StringBuilder(whereBuilder);
                    appendActionWhere(containerWhereBuilder, "", actions, InventorySources.CONTAINER);
                    StringBuilder itemWhereBuilder = new StringBuilder(whereBuilder);
                    appendActionWhere(itemWhereBuilder, "", actions, InventorySources.ITEM);
                    StringBuilder entityWhereBuilder = new StringBuilder(filter.entityContainerWhere("entity_rows", "spawn_rows"));
                    filter.appendMaterialWhere(entityWhereBuilder, "entity_rows");
                    appendActionWhere(entityWhereBuilder, "entity_rows", actions, InventorySources.CONTAINER);

                    String blockWhere = blockWhereBuilder.toString();
                    String containerWhere = containerWhereBuilder.toString();
                    String entityWhere = entityWhereBuilder.toString();
                    String itemWhere = itemWhereBuilder.toString();
                    String query = buildQuery(
                            blockWhere,
                            containerWhere,
                            entityWhere,
                            itemWhere,
                            filter.table(connection, "block", "", blockWhere, InventorySources.BLOCK),
                            filter.table(connection, "container", "", containerWhere, InventorySources.CONTAINER),
                            filter.entityContainerTable(connection, "entity_rows", entityWhere, InventorySources.ENTITY_CONTAINER),
                            filter.table(connection, "item", "", itemWhere, InventorySources.ITEM)
                    );

                    filter.read(connection, query, " ORDER BY time DESC, source DESC, id DESC", statement -> {
                        int parameterIndex = filter.bind(statement);
                        parameterIndex = filter.bind(statement, parameterIndex);
                        parameterIndex = filter.bindEntityContainer(statement, parameterIndex);
                        return filter.bind(statement, parameterIndex);
                    }, results -> result.add(parseInventoryResult(connection, results)));
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

    /**
     * @return the lookup without its order or limit, which {@link LookupFilter#read} adds
     */
    private static String buildQuery(String blockWhere, String containerWhere, String entityWhere, String itemWhere, String blockTable, String containerTable, String entityContainerTable, String itemTable) {
        StringBuilder query = new StringBuilder("SELECT * FROM (");
        query.append("SELECT 0 AS source,rowid AS id,time,").append(ConfigHandler.databaseType.getUserColumn()).append(",wid,x,y,z,type,data,1 AS amount,meta AS metadata,action,rolled_back FROM ")
                .append(blockTable).append(" WHERE ").append(blockWhere);
        query.append(" UNION ALL ");
        query.append("SELECT 1 AS source,rowid AS id,time,").append(ConfigHandler.databaseType.getUserColumn()).append(",wid,x,y,z,type,data,amount,metadata,action,rolled_back FROM ")
                .append(containerTable).append(" WHERE ").append(containerWhere);
        query.append(" UNION ALL ");
        query.append("SELECT ").append(InventorySources.ENTITY_CONTAINER).append(" AS source,entity_rows.rowid AS id,entity_rows.time,entity_rows.").append(ConfigHandler.databaseType.getUserColumn()).append(",spawn_rows.current_wid AS wid,spawn_rows.x,spawn_rows.y,spawn_rows.z,entity_rows.type,entity_rows.data,entity_rows.amount,entity_rows.metadata,entity_rows.action,entity_rows.rolled_back FROM ")
                .append(entityContainerTable).append(" JOIN ").append(ConfigHandler.prefix).append("entity_spawn spawn_rows ON spawn_rows.rowid=entity_rows.entity_spawn_rowid WHERE ").append(entityWhere);
        query.append(" UNION ALL ");
        query.append("SELECT 2 AS source,rowid AS id,time,").append(ConfigHandler.databaseType.getUserColumn()).append(",wid,x,y,z,type,0 AS data,amount,data AS metadata,action,rolled_back FROM ")
                .append(itemTable).append(" WHERE ").append(itemWhere);
        query.append(") AS inventory_lookup");
        return query.toString();
    }

    /**
     * Restricts one source of the lookup to the actions asked of it. The restriction is part of each
     * source's own clause, rather than applied to the merged rows, so that on SQLite the reader of
     * packed rows knows exactly which rows each source keeps.
     *
     * @param source
     *            the source the clause is for; entity containers count as containers
     */
    private static void appendActionWhere(StringBuilder query, String alias, List<InventoryAction> actions, int source) {
        if (actions.isEmpty()) {
            return;
        }
        StringJoiner ids = new StringJoiner(",");
        for (InventoryAction action : actions) {
            if (action.sourceId() == source) {
                ids.add(String.valueOf(action.id()));
            }
        }
        // No action has the id -1, so a source none of the actions belong to keeps no rows.
        query.append(" AND ").append(alias.isEmpty() ? "" : alias + ".").append("action IN (").append(ids.length() > 0 ? ids.toString() : "-1").append(")");
    }

    private static InventoryResult parseInventoryResult(Connection connection, ResultSet results) throws Exception {
        int userId = results.getInt("user");
        String username = UserStatement.getName(connection, userId);

        int source = results.getInt("source");
        if (source == InventorySources.ENTITY_CONTAINER) {
            source = InventorySources.CONTAINER;
        }

        return new InventoryResult(
                results.getLong("time"), username, WorldUtils.getWorldName(results.getInt("wid")),
                (int) Math.floor(results.getDouble("x")), (int) Math.floor(results.getDouble("y")), (int) Math.floor(results.getDouble("z")),
                results.getInt("type"), results.getInt("data"), results.getInt("amount"), DatabaseUtils.getBlobBytes(results, "metadata"),
                results.getInt("action"), results.getInt("rolled_back"), source
        );
    }
}
