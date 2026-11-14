package net.coreprotect.api;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import net.coreprotect.api.result.MessageResult;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.Database;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.WorldUtils;

/**
 * Provides API methods for player chat and command lookups.
 */
public class MessageAPI {
    private MessageAPI() {
        throw new IllegalStateException("API class");
    }

    public static List<MessageResult> performChatLookup(String user, int offset) {
        return performChatLookup(LookupOptions.builder().user(user).time(offset).build());
    }

    public static List<MessageResult> performChatLookup(LookupOptions options) {
        return performLookup("chat", CoreProtectAction.CHAT, options);
    }

    public static List<MessageResult> performCommandLookup(String user, int offset) {
        return performCommandLookup(LookupOptions.builder().user(user).time(offset).build());
    }

    public static List<MessageResult> performCommandLookup(LookupOptions options) {
        return performLookup("command", CoreProtectAction.COMMAND, options);
    }

    private static List<MessageResult> performLookup(String table, CoreProtectAction action, LookupOptions options) {
        List<MessageResult> result = new ArrayList<>();

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
                String where = filter.where("");
                StringBuilder query = new StringBuilder("SELECT time," + ConfigHandler.databaseType.getUserColumn() + ",wid,x,y,z,message FROM ");
                query.append(filter.table(connection, table, "", where, 0)).append(' ');
                query.append(filter.index(table));
                query.append("WHERE ").append(where);

                filter.read(connection, query.toString(), " ORDER BY " + ConfigHandler.getDescendingEventOrder(), filter::bind, results -> result.add(parseMessageResult(connection, results, action.id())));
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

    static Integer getUserId(Connection connection, String user) throws Exception {
        if (user == null || user.isEmpty() || user.equalsIgnoreCase("#global")) {
            return null;
        }

        return UserStatement.findId(connection, user);
    }

    private static MessageResult parseMessageResult(Connection connection, ResultSet results, int actionId) throws Exception {
        int userId = results.getInt("user");
        String username = UserStatement.getName(connection, userId);

        return new MessageResult(
                results.getLong("time"), username, WorldUtils.getWorldName(results.getInt("wid")),
                results.getInt("x"), results.getInt("y"), results.getInt("z"),
                results.getString("message"), actionId
        );
    }

    static int clampToInt(long value) {
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        else if (value < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }

        return (int) value;
    }
}
