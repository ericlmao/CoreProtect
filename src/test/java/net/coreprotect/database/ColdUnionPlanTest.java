package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * The gate for planning a page of a lookup that reads several tables at once.
 *
 * <p>
 * Those results are merged by time rather than by row id, so a page cannot be planned by skipping
 * rows in one table alone. Instead the whole query is limited to rows at or below a timestamp, and
 * its offset is reduced by exactly how many matching rows lie above that timestamp. Because time is
 * the first thing the merge sorts by, the rows removed are exactly the ones the page was going to
 * skip past, which is what makes the arithmetic sound.
 * </p>
 *
 * <p>
 * Every test here asks for the same page with and without that plan and requires identical rows.
 * </p>
 */
class ColdUnionPlanTest {

    private static final long DAY = 86400L;
    private static final int ROWS_PER_DAY = 800;
    private static final int DAYS = 5;
    private static final int USERS = 10;

    private Connection connection;
    private DatabaseType previousType;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_container (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, amount INTEGER, metadata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_item (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data BLOB, amount INTEGER, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
        }
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void plannedUnionPagesMatchAtEveryBoundary() throws Exception {
        seal();
        for (int offset : offsets()) {
            assertSamePage(1, offset, 4, 0, 0);
        }
    }

    @Test
    void plannedUnionPagesMatchWhenTimeBounded() throws Exception {
        seal();
        long base = base();
        long startTime = base + DAY;
        long endTime = base + (4 * DAY);
        for (int offset : offsets()) {
            assertSamePage(1, offset, 4, startTime, endTime);
        }
    }

    @Test
    void plannedUnionPagesMatchWhenRowsShareTimestamps() throws Exception {
        // Every table writes at the same timestamps, so the merge has to break ties consistently
        // whether or not the page was planned.
        sealWithSharedTimestamps();
        for (int offset : offsets()) {
            assertSamePage(1, offset, 4, 0, 0);
        }
    }

    @Test
    void plannedUnionPagesMatchPastTheEndOfTheResult() throws Exception {
        seal();
        int owned = (ROWS_PER_DAY * DAYS * 3) / USERS;
        for (int offset : new int[] { owned - 8, owned - 1, owned, owned + 1, owned + 500 }) {
            assertSamePage(1, offset, 4, 0, 0);
        }
    }

    @Test
    void aReadStopsAtTheLimitEvenWhenItWasNotExpectedTo() throws Exception {
        seal();
        int limit = net.coreprotect.config.Config.getGlobal().COLD_MAX_ROWS;
        net.coreprotect.config.Config.getGlobal().COLD_MAX_ROWS = 10000;
        try {
            // A budget under the limit that nonetheless walks a lot of rows must still stop, so a
            // read can never grow beyond what the limit allows whatever the plan expected.
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                SQLiteColdIndex.setLookupFilters("", "");
                SQLiteColdIndex.setRowBudget(9999);
                SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
                assertTrue(count("SELECT COUNT(*) FROM sqlite_temp_master WHERE name = 'cp_cold_block'") == 0
                        || count("SELECT COUNT(*) FROM temp.cp_cold_block") <= 10001, "a read never exceeds the limit");
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
        }
        finally {
            net.coreprotect.config.Config.getGlobal().COLD_MAX_ROWS = limit;
        }
    }

    @Test
    void aPlainUserLookupIsRecognisedAsReadingSeveralTables() throws Exception {
        // This is the shape of "/co lookup user:X time:1d": a lookup with no actions given, which
        // reads blocks together with container and item history and merges them by time.
        assertEquals(4, LookupRawAccess.unionTables(new ArrayList<>(), true, "block").length,
                "a plain user lookup merges block, container, entity container and item");
        assertEquals(1, LookupRawAccess.unionTables(new ArrayList<>(List.of(net.coreprotect.model.action.LookupActions.BLOCK_BREAK)), false, "block").length,
                "a lookup restricted to one action reads one table");
    }

    @Test
    void planningActuallyCutsTheWorkDown() throws Exception {
        seal();
        int deep = ((ROWS_PER_DAY * DAYS * 3) / USERS) - 20;

        long unplanned = rowsRead(1, deep, 4, false);
        long planned = rowsRead(1, deep, 4, true);
        assertTrue(planned < unplanned, "planning reads fewer rows: " + planned + " vs " + unplanned);
    }

    private void assertSamePage(long user, int offset, int pageSize, long startTime, long endTime) throws Exception {
        List<String> unplanned = unionPage(user, offset, pageSize, startTime, endTime, false);
        List<String> planned = unionPage(user, offset, pageSize, startTime, endTime, true);
        assertEquals(unplanned, planned, "planned union page differs at offset " + offset + (startTime > 0 ? " (time bounded)" : ""));
    }

    private long rowsRead(long user, int offset, int pageSize, boolean planned) throws Exception {
        unionPage(user, offset, pageSize, 0, 0, planned);
        return lastRowsRead;
    }

    private long lastRowsRead;

    /**
     * Builds the same union a lookup across block, container and item builds, optionally planned.
     */
    private List<String> unionPage(long user, int offset, int pageSize, long startTime, long endTime, boolean planned) throws Exception {
        String[] tables = { "block", "container", "item" };
        List<String> page = new ArrayList<>();
        lastRowsRead = 0;

        ColdUnionPlan plan = planned
                ? ColdUnionPlan.forPage(connection, tables, Long.toString(user), "", null, null, null, null, startTime, endTime, offset)
                : ColdUnionPlan.none();

        long effectiveEnd = plan.getCutTime() > 0 ? plan.getCutTime() : endTime;
        long effectiveOffset = offset - plan.getSkippedRows();

        SQLiteColdIndex.beginLookup(startTime, effectiveEnd);
        try {
            SQLiteColdIndex.setLookupFilters(Long.toString(user), "");
            SQLiteColdIndex.setRowBudget(effectiveOffset + pageSize);

            StringBuilder union = new StringBuilder();
            for (int index = 0; index < tables.length; index++) {
                String source = SQLiteColdIndex.sourceExpression(connection, tables[index], 1, null);
                if (source.startsWith("(")) {
                    lastRowsRead = lastRowsRead + count("SELECT COUNT(*) FROM temp.cp_cold_" + tables[index]);
                }
                if (index > 0) {
                    union.append(" UNION ALL ");
                }
                union.append("SELECT * FROM (SELECT '").append(index).append("' tbl, rowid id, time FROM ").append(source)
                        .append(" WHERE user = ").append(user)
                        .append(startTime > 0 ? " AND time > " + startTime : "")
                        .append(effectiveEnd > 0 ? " AND time <= " + effectiveEnd : "")
                        .append(" ORDER BY time DESC, id DESC LIMIT ").append(effectiveOffset + pageSize).append(')');
            }

            String query = "SELECT tbl, id FROM (" + union + ") ORDER BY time DESC, tbl DESC, id DESC LIMIT " + pageSize
                    + " OFFSET " + effectiveOffset;
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
                while (results.next()) {
                    page.add(results.getString(1) + ":" + results.getLong(2));
                }
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        return page;
    }

    private int[] offsets() {
        int perDay = (ROWS_PER_DAY * 3) / USERS;
        return new int[] { 0, 1, perDay - 1, perDay, perDay + 1, (2 * perDay) - 1, 2 * perDay, (2 * perDay) + 1,
                (3 * perDay) + 7, (4 * perDay) - 3 };
    }

    private long base() {
        long now = System.currentTimeMillis() / 1000L;
        return ((now - (40 * DAY)) / DAY) * DAY;
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private void seal() throws Exception {
        write(false);
    }

    private void sealWithSharedTimestamps() throws Exception {
        write(true);
    }

    /**
     * Writes several days of activity across all three tables and seals it.
     *
     * @param sharedTimestamps
     *            true to give rows in different tables the same timestamps, which forces the merge
     *            to break ties
     */
    private void write(boolean sharedTimestamps) throws Exception {
        long base = base();
        connection.setAutoCommit(false);
        try (PreparedStatement block = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,1,0,NULL,NULL,0,0)");
                PreparedStatement container = connection.prepareStatement("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?,1,?,64,?,1,0,1,NULL,0,0)");
                PreparedStatement item = connection.prepareStatement("INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,1,?,64,?,1,NULL,1,0,0)")) {
            for (int day = 0; day < DAYS; day++) {
                for (int index = 0; index < ROWS_PER_DAY; index++) {
                    long time = base + (day * DAY) + index;
                    int user = 1 + (index % USERS);
                    bind(block, time, user, index);
                    bind(container, sharedTimestamps ? time : time + 1, user, index);
                    bind(item, sharedTimestamps ? time : time + 2, user, index);
                }
            }
            block.executeBatch();
            container.executeBatch();
            item.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);

        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });
        assertTrue(sealed > 0, "the test data has to reach compressed storage");
        SQLiteColdIndex.reload(connection);
    }

    private void bind(PreparedStatement statement, long time, int user, int index) throws SQLException {
        statement.setLong(1, time);
        statement.setInt(2, user);
        statement.setInt(3, index % 100);
        statement.setInt(4, index % 100);
        statement.addBatch();
    }
}
