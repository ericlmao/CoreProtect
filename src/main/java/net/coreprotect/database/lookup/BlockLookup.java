package net.coreprotect.database.lookup;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.Database;
import net.coreprotect.database.DuckDBLookupQuery;
import net.coreprotect.database.LocationQuery;
import net.coreprotect.database.statement.EntitySpawnStatement;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.language.Phrase;
import net.coreprotect.language.Selector;
import net.coreprotect.listener.channel.PluginChannelListener;
import net.coreprotect.model.action.LookupActions;
import net.coreprotect.utility.*;
import org.bukkit.Material;
import org.bukkit.block.BlockState;
import org.bukkit.command.CommandSender;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.UUID;

public class BlockLookup {

    public static String performLookup(String command, Statement statement, BlockState block, CommandSender commandSender, int offset, int page, int limit) {
        return performLookup(command, statement, block, commandSender, offset, page, limit, null);
    }

    public static String performEntityLookup(String command, Statement statement, BlockState block, CommandSender commandSender, int page, int limit, UUID entityUuid) throws SQLException {
        Integer entitySpawnRowId = EntitySpawnStatement.findRowIdByUuid(statement.getConnection(), entityUuid);
        if (entitySpawnRowId == null) {
            ConfigHandler.lookupType.remove(commandSender.getName());
            ConfigHandler.lookupCommand.remove(commandSender.getName());
            ConfigHandler.lookupPage.remove(commandSender.getName());
            return Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_DATA_LOCATION, Selector.FIRST);
        }
        return performLookup(command, statement, block, commandSender, 0, page, limit, entitySpawnRowId);
    }

    public static String performLookup(String command, Statement statement, BlockState block, CommandSender commandSender, int offset, int page, int limit, Integer entitySpawnRowId) {
        String resultText = "";
        InspectorSource source = null;

        try {
            if (block == null) {
                return resultText;
            }

            if (command == null) {
                if (commandSender.hasPermission("coreprotect.co")) {
                    command = "co";
                }
                else if (commandSender.hasPermission("coreprotect.core")) {
                    command = "core";
                }
                else if (commandSender.hasPermission("coreprotect.coreprotect")) {
                    command = "coreprotect";
                }
                else {
                    command = "co";
                }
            }

            boolean found = false;
            int x = block.getX();
            int y = block.getY();
            int z = block.getZ();
            long time = (System.currentTimeMillis() / 1000L);
            int worldId = WorldUtils.getWorldId(block.getWorld().getName());
            long checkTime = 0;
            int count = 0;
            int rowMax = page * limit;
            int page_start = rowMax - limit;
            if (offset > 0) {
                checkTime = time - offset;
            }

            String blockName = block.getType().name().toLowerCase(Locale.ROOT);
            String where;
            if (entitySpawnRowId == null) {
                String placedEntityTypes = placedEntityTypeIds();
                String placedEntityPredicate = ConfigHandler.databaseType.isDuckDB()
                        ? "list_contains([" + placedEntityTypes + "],type)"
                        : "type IN(" + placedEntityTypes + ")";
                String actionPredicate = "(action IN(0,1," + LookupActions.ENTITY_SPAWN + ") OR (action=" + LookupActions.ENTITY_KILL + " AND " + placedEntityPredicate + "))";
                where = LocationQuery.predicate("wid", " = " + worldId) + " AND " + LocationQuery.predicate("x", " = " + x) + " AND " + LocationQuery.predicate("z", " = " + z) + " AND y = " + y + " AND " + actionPredicate + " AND time >= " + checkTime;
            }
            else if (ConfigHandler.databaseType.isSQLite()) {
                where = null; // chosen below, once the entity's rows are known
            }
            else {
                where = "rowid IN(" + entityBlockRowIds(statement, entitySpawnRowId) + ") AND action IN(" + LookupActions.ENTITY_SPAWN + "," + LookupActions.ENTITY_KILL + ") AND time >= " + checkTime;
            }
            boolean combinedDuckDBPage = ConfigHandler.databaseType.isDuckDB();
            String query;
            ResultSet results;
            if (combinedDuckDBPage) {
                String sourceTable = entitySpawnRowId == null ? DuckDBLookupQuery.spatialTable(statement.getConnection(), "block", worldId, x, x, z, z, "spatial_rows") : ConfigHandler.prefix + "block";
                String columns = "data_rows.time,data_rows." + ConfigHandler.databaseType.getUserColumn() + ",data_rows.action,data_rows.type,data_rows.data,data_rows.rolled_back";
                if (entitySpawnRowId != null) {
                    columns += ",data_rows.wid,data_rows.x,data_rows.y,data_rows.z";
                }
                query = DuckDBLookupQuery.pageQuery(sourceTable, ConfigHandler.prefix + "block", where, columns, false, limit, page_start);
                results = statement.executeQuery(query);
            }
            else {
                if (entitySpawnRowId == null) {
                    // Compressed storage as well as the live rows, so a block's whole history is shown
                    // wherever each row of it currently sits. Only the page's own rows are read, and the
                    // page count is taken while the page is planned.
                    source = InspectorSource.open(statement.getConnection(), "block", worldId, x, x, z, z, checkTime);
                }
                else if (ConfigHandler.databaseType.isSQLite()) {
                    // The entity's rows are packed minutes after they are logged, so they are found by
                    // what they record rather than by row id in the live table, and only around the
                    // places the entity is known to have been.
                    EntityBlockRows rows = entityBlockRows(statement, entitySpawnRowId);
                    where = rows.packedPredicate() + " AND action IN(" + LookupActions.ENTITY_SPAWN + "," + LookupActions.ENTITY_KILL + ") AND time >= " + checkTime;
                    int[] area = rows.area(worldId, x, z);
                    source = InspectorSource.open(statement.getConnection(), "block", worldId, area[0], area[1], area[2], area[3], checkTime);
                }
                else {
                    source = InspectorSource.openForEntity(statement.getConnection(), "block", entitySpawnRowId);
                }
                String columns = "time," + ConfigHandler.databaseType.getUserColumn() + ",action,type,data,rolled_back" + (entitySpawnRowId == null ? "" : ",wid,x,y,z");
                results = source.page(columns, where, ConfigHandler.getDescendingEventOrder(), page_start, limit);
            }

            StringBuilder resultTextBuilder = new StringBuilder();

            while (results.next()) {
                if (combinedDuckDBPage) {
                    count = results.getInt("count");
                    if (results.getObject("result_id") == null) {
                        continue;
                    }
                }
                int resultUserId = results.getInt("user");
                int resultAction = results.getInt("action");
                int resultType = results.getInt("type");
                int resultData = results.getInt("data");
                long resultTime = results.getLong("time");
                int resultRolledBack = results.getInt("rolled_back");

                String resultUser = UserStatement.getName(statement.getConnection(), resultUserId);
                String timeAgo = ChatUtils.getTimeSince(resultTime, time, true);

                if (!found) {
                    resultTextBuilder = new StringBuilder(Color.WHITE + "----- " + Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "----- " + ChatUtils.getCoordinates(command, worldId, x, y, z, false, false) + "\n");
                }
                found = true;

                Phrase phrase = Phrase.LOOKUP_BLOCK;
                String selector = Selector.FIRST;
                String tag = Color.WHITE + "-";
                if (resultAction == LookupActions.ENTITY_SPAWN) {
                    phrase = EntitySpawnTracking.isPlacedEntityType(EntityUtils.getEntityType(resultType)) ? Phrase.LOOKUP_BLOCK : Phrase.LOOKUP_ENTITY_SPAWN;
                    selector = Selector.FIRST;
                    tag = Color.GREEN + "+";
                }
                else if (resultAction == LookupActions.ENTITY_KILL && EntitySpawnTracking.isPlacedEntityType(EntityUtils.getEntityType(resultType))) {
                    phrase = Phrase.LOOKUP_BLOCK;
                    selector = Selector.SECOND;
                    tag = Color.RED + "-";
                }
                else if (resultAction == 2 || resultAction == 3) {
                    phrase = Phrase.LOOKUP_INTERACTION; // {clicked|killed}
                    selector = (resultAction != 3 ? Selector.FIRST : Selector.SECOND);
                    tag = (resultAction != 3 ? Color.WHITE + "-" : Color.RED + "-");
                }
                else {
                    phrase = Phrase.LOOKUP_BLOCK; // {placed|broke}
                    selector = (resultAction != 0 ? Selector.FIRST : Selector.SECOND);
                    tag = (resultAction != 0 ? Color.GREEN + "+" : Color.RED + "-");
                }

                String rbFormat = "";
                if (resultRolledBack == 1 || resultRolledBack == 3) {
                    rbFormat = Color.STRIKETHROUGH;
                }

                String target;
                if (resultAction == 3 || resultAction == LookupActions.ENTITY_SPAWN) {
                    target = EntityUtils.getEntityType(resultType).name().toLowerCase(Locale.ROOT);
                }
                else {
                    target = MaterialUtils.getBlockDisplayName(resultType, resultData);
                    if (target.length() > 0 && !target.contains(":")) {
                        target = "minecraft:" + target.toLowerCase(Locale.ROOT);
                    }
                }
                if (target.length() > 0) {
                    target = "" + target + "";
                }

                // Hide "minecraft:" for now.
                if (target.startsWith("minecraft:")) {
                    target = target.split(":")[1];
                }

                String coordinateInfo = "";
                if (entitySpawnRowId != null) {
                    int originWorldId = results.getInt("wid");
                    int originX = results.getInt("x");
                    int originY = results.getInt("y");
                    int originZ = results.getInt("z");
                    if (originWorldId != worldId || originX != x || originY != y || originZ != z) {
                        coordinateInfo = ChatUtils.getCoordinateTooltip(originWorldId, originX, originY, originZ, Phrase.build(Phrase.LOOKUP_ENTITY_ORIGIN), true);
                    }
                }
                resultTextBuilder.append(timeAgo + " " + tag + " ").append(Phrase.build(phrase, Color.DARK_AQUA + rbFormat + resultUser + Color.WHITE + rbFormat, Color.DARK_AQUA + rbFormat + target + Color.WHITE + coordinateInfo, selector)).append("\n");
                PluginChannelListener.getInstance().sendData(commandSender, resultTime, phrase, selector, resultUser, target, -1, x, y, z, worldId, rbFormat, false, tag.contains("+"));
            }

            resultText = resultTextBuilder.toString();
            results.close();
            if (source != null) {
                count = (int) source.count(where);
            }
            int totalPages = (int) Math.ceil(count / (limit + 0.0));

            if (found) {
                if (count > limit) {
                    String pageInfo = Color.WHITE + "-----\n";
                    pageInfo = pageInfo + ChatUtils.getPageNavigation(command, page, totalPages) + "\n";
                    resultText = resultText + pageInfo;
                }
            }
            else {
                if (rowMax > count && count > 0) {
                    resultText = Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_RESULTS_PAGE, Selector.SECOND);
                }
                else {
                    // resultText = Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Color.WHITE + "No block data found at " + Color.ITALIC + "x" + x + "/y" + y + "/z" + z + ".";
                    resultText = Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_DATA_LOCATION, Selector.FIRST);
                    if (entitySpawnRowId == null && !blockName.equals("air") && !blockName.equals("cave_air")) {
                        resultText = Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- " + Phrase.build(Phrase.NO_DATA, Color.ITALIC + block.getType().name().toLowerCase(Locale.ROOT) + Color.WHITE) + "\n";
                    }
                }
            }

            ConfigHandler.lookupPage.put(commandSender.getName(), page);
            ConfigHandler.lookupType.put(commandSender.getName(), 2);
            ConfigHandler.lookupCommand.put(commandSender.getName(), x + "." + y + "." + z + "." + worldId + ".0." + limit + (entitySpawnRowId == null ? "" : "." + entitySpawnRowId));
        }
        catch (Exception e) {
            Database.reportDatabaseFailure(e);
        }
        finally {
            if (source != null) {
                source.close();
            }
        }
        return resultText;
    }

    /** The block rows of one entity's spawn and kill, as the entity spawn row records them. */
    private static final class EntityBlockRows {
        private final long blockRowId;
        private final long killRowId;
        private final Integer originWorldId;
        private final double originX;
        private final double originZ;
        private final Integer currentWorldId;
        private final double currentX;
        private final double currentZ;

        private EntityBlockRows(long blockRowId, long killRowId, Integer originWorldId, double originX, double originZ, Integer currentWorldId, double currentX, double currentZ) {
            this.blockRowId = blockRowId;
            this.killRowId = killRowId;
            this.originWorldId = originWorldId;
            this.originX = originX;
            this.originZ = originZ;
            this.currentWorldId = currentWorldId;
            this.currentX = currentX;
            this.currentZ = currentZ;
        }

        /**
         * @return a clause keeping the spawn and kill rows wherever they are stored, live or packed
         */
        private String packedPredicate() {
            StringJoiner alternatives = new StringJoiner(" OR ", "(", ")");
            alternatives.add("rowid = " + (blockRowId > 0L ? blockRowId : 0L));
            if (killRowId > 0L) {
                alternatives.add("(action = " + LookupActions.ENTITY_KILL + " AND data = " + killRowId + ")");
            }
            return alternatives.toString();
        }

        /**
         * @return the x and z bounds, as {minX, maxX, minZ, maxZ}, covering the inspected block and
         *         every place in its world the entity was spawned at or last seen at
         */
        private int[] area(int worldId, int x, int z) {
            int[] area = { x, x, z, z };
            include(area, worldId, originWorldId, originX, originZ);
            include(area, worldId, currentWorldId, currentX, currentZ);
            return area;
        }

        private static void include(int[] area, int worldId, Integer placeWorldId, double placeX, double placeZ) {
            if (placeWorldId == null || placeWorldId != worldId) {
                return;
            }
            int blockX = (int) Math.floor(placeX);
            int blockZ = (int) Math.floor(placeZ);
            area[0] = Math.min(area[0], blockX);
            area[1] = Math.max(area[1], blockX);
            area[2] = Math.min(area[2], blockZ);
            area[3] = Math.max(area[3], blockZ);
        }
    }

    private static EntityBlockRows entityBlockRows(Statement statement, int entitySpawnRowId) throws SQLException {
        try (ResultSet results = statement.executeQuery("SELECT block_rowid,kill_rowid,wid,origin_x,origin_z,current_wid,x,z FROM " + ConfigHandler.prefix + "entity_spawn WHERE rowid=" + entitySpawnRowId)) {
            if (!results.next()) {
                return new EntityBlockRows(0L, 0L, null, 0, 0, null, 0, 0);
            }
            long blockRowId = results.getLong("block_rowid");
            long killRowId = results.getLong("kill_rowid");
            int originWorldId = results.getInt("wid");
            Integer origin = results.wasNull() ? null : originWorldId;
            double originX = results.getDouble("origin_x");
            double originZ = results.getDouble("origin_z");
            int currentWorldId = results.getInt("current_wid");
            Integer current = results.wasNull() ? null : currentWorldId;
            return new EntityBlockRows(blockRowId, killRowId, origin, originX, originZ, current, results.getDouble("x"), results.getDouble("z"));
        }
    }

    private static String entityBlockRowIds(Statement statement, int entitySpawnRowId) throws SQLException {
        StringJoiner ids = new StringJoiner(",");
        long killRowId = 0L;
        try (ResultSet results = statement.executeQuery("SELECT block_rowid,kill_rowid FROM " + ConfigHandler.prefix + "entity_spawn WHERE rowid=" + entitySpawnRowId)) {
            if (results.next()) {
                long blockRowId = results.getLong("block_rowid");
                killRowId = results.getLong("kill_rowid");
                if (blockRowId > 0L) {
                    ids.add(Long.toString(blockRowId));
                }
            }
        }
        if (killRowId > 0L) {
            try (ResultSet results = statement.executeQuery("SELECT rowid AS id FROM " + ConfigHandler.prefix + "block WHERE action=" + LookupActions.ENTITY_KILL + " AND data=" + killRowId)) {
                while (results.next()) {
                    ids.add(Long.toString(results.getLong("id")));
                }
            }
        }
        return ids.length() == 0 ? "0" : ids.toString();
    }

    private static String placedEntityTypeIds() {
        StringJoiner ids = new StringJoiner(",");
        for (Integer id : EntitySpawnTracking.getPlacedEntityTypeIds()) {
            ids.add(Integer.toString(id));
        }
        return ids.length() == 0 ? "0" : ids.toString();
    }

}
