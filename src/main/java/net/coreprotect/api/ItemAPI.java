package net.coreprotect.api;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import net.coreprotect.api.result.ItemResult;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.Database;
import net.coreprotect.database.logger.ItemLogger;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.utility.WorldUtils;
import net.coreprotect.utility.DatabaseUtils;
import net.coreprotect.utility.ErrorReporter;

/**
 * Provides API methods for world item transaction lookups.
 */
public class ItemAPI {

    private ItemAPI() {
        throw new IllegalStateException("API class");
    }

    public static List<ItemResult> performLookup(LookupOptions options) {
        List<ItemResult> result = new ArrayList<>();

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
                String where = filter.where("") + " AND action NOT IN (" + ItemLogger.ITEM_BREAK + "," + ItemLogger.ITEM_DESTROY + "," + ItemLogger.ITEM_CREATE + "," + ItemLogger.ITEM_SELL + "," + ItemLogger.ITEM_BUY + ")";
                StringBuilder query = new StringBuilder("SELECT time," + ConfigHandler.databaseType.getUserColumn() + ",wid,x,y,z,type,data,amount,action,rolled_back FROM ");
                query.append(filter.table(connection, "item", "", where, 0)).append(' ');
                query.append(filter.index("item"));
                query.append("WHERE ").append(where);

                filter.read(connection, query.toString(), " ORDER BY " + ConfigHandler.getDescendingEventOrder(), filter::bind, results -> result.add(parseItemResult(connection, results)));
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

    private static ItemResult parseItemResult(Connection connection, ResultSet results) throws Exception {
        int userId = results.getInt("user");
        String username = UserStatement.getName(connection, userId);

        return new ItemResult(
                results.getLong("time"), username, WorldUtils.getWorldName(results.getInt("wid")),
                results.getInt("x"), results.getInt("y"), results.getInt("z"),
                results.getInt("type"), results.getInt("amount"), DatabaseUtils.getBlobBytes(results, "data"),
                results.getInt("action"), results.getInt("rolled_back")
        );
    }
}
