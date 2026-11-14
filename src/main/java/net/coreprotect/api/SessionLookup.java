package net.coreprotect.api;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import net.coreprotect.api.result.SessionResult;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.Database;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.utility.WorldUtils;
import net.coreprotect.utility.ErrorReporter;

/**
 * Provides API methods for looking up player session data in the CoreProtect database.
 * Session data includes login/logout events and their associated timestamps and locations.
 */
public class SessionLookup {

    /**
     * The session type ID used for identifying session-related events.
     */
    public static final int ID = 0;

    /**
     * Private constructor to prevent instantiation.
     * This is a utility class with static methods only.
     */
    private SessionLookup() {
        throw new IllegalStateException("API class");
    }

    /**
     * Performs a lookup of session-related actions for the specified user.
     * This returns login and logout events within the specified time constraint.
     * 
     * @param user
     *            The username to look up session data for
     * @param offset
     *            Time constraint in seconds (0 means no time constraint)
     * @return List of results in a String array format, empty list if API is disabled or no results found
     */
    public static List<String[]> performLookup(String user, int offset) {
        List<String[]> result = new ArrayList<>();

        if (!Config.getGlobal().API_ENABLED) {
            return result;
        }

        if (user == null) {
            return result;
        }

        try (Connection connection = Database.getConnection(false, 1000)) {
            if (connection == null) {
                return result;
            }

            String type = String.valueOf(ID);
            Integer userId = MessageAPI.getUserId(connection, user);
            if (userId == null || userId < 0) {
                return result;
            }

            // The same lookup as the typed one, which on SQLite reads the packed sessions together
            // with the live rows a bounded window of the history at a time.
            LookupFilter filter = LookupFilter.fromOptions(connection, LookupOptions.builder().user(user).time(Math.max(0, offset)).build());
            try {
                String where = filter.where("");
                String query = "SELECT time," + ConfigHandler.databaseType.getUserColumn() + ",wid,x,y,z,action FROM " + filter.table(connection, "session", "", where, 0) + " WHERE " + where;
                filter.read(connection, query, " ORDER BY " + ConfigHandler.getDescendingEventOrder(), filter::bind, results -> result.add(extractSessionData(connection, results, type)));
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
     * Performs a typed lookup of session-related actions.
     *
     * @param options
     *            Lookup options
     * @return List of typed session results, empty list if API is disabled or no results found
     */
    public static List<SessionResult> performLookup(LookupOptions options) {
        List<SessionResult> result = new ArrayList<>();

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
                StringBuilder query = new StringBuilder("SELECT time," + ConfigHandler.databaseType.getUserColumn() + ",wid,x,y,z,action FROM ");
                query.append(filter.table(connection, "session", "", where, 0)).append(' ');
                query.append(filter.index("session"));
                query.append("WHERE ").append(where);

                filter.read(connection, query.toString(), " ORDER BY " + ConfigHandler.getDescendingEventOrder(), filter::bind, results -> result.add(parseSessionResult(connection, results)));
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
     * Extracts session data from a result set row.
     * 
     * @param connection
     *            The database connection
     * @param results
     *            The result set to extract data from
     * @param type
     *            The session type ID
     * @return An array of session data values
     * @throws Exception
     *             if an error occurs while extracting data
     */
    private static String[] extractSessionData(Connection connection, ResultSet results, String type) throws Exception {
        String resultTime = results.getString("time");
        int resultUserId = results.getInt("user");
        String resultWorldId = results.getString("wid");
        String resultX = results.getString("x");
        String resultY = results.getString("y");
        String resultZ = results.getString("z");
        String resultAction = results.getString("action");

        String resultUser = UserStatement.getName(connection, resultUserId);

        // Create and return the session data array
        return new String[] { resultTime, resultUser, resultX, resultY, resultZ, resultWorldId, type, resultAction };
    }

    private static SessionResult parseSessionResult(Connection connection, ResultSet results) throws Exception {
        int userId = results.getInt("user");
        String username = UserStatement.getName(connection, userId);

        return new SessionResult(
                results.getLong("time"), username, WorldUtils.getWorldName(results.getInt("wid")),
                results.getInt("x"), results.getInt("y"), results.getInt("z"), results.getInt("action")
        );
    }
}
