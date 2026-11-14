package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.listener.channel.PluginChannelHandshakeListener;
import net.coreprotect.listener.channel.PluginChannelListener;
import net.coreprotect.model.action.EntityActionFilter;
import net.coreprotect.model.lookup.EntityLookupContext;
import net.coreprotect.model.lookup.LookupRollbackState;

/**
 * A page read from packed rows keeps only the rows that can appear on it, and is still exactly the
 * page plain SQL over the same rows unpacked would show.
 *
 * <p>
 * When the reader can test every condition of a lookup, it ranks the matching packed rows in the
 * page's own order and hands the query only as many as the page could show, rather than copying
 * every match into a temporary table for the database to sort. Each test here runs pages through
 * the real lookup path, or through the reader the way that path drives it, and compares them with
 * the same query over an unpacked copy of the table. Times within a day are deliberately out of
 * row id order, so a page merged by time ranks rows differently from one paged by row id, and some
 * packed rows are marked rolled back after packing, which only the recorded overlay knows about.
 * </p>
 */
class ColdTopRowsTest {

    private static final long DAY = 86400L;
    private static final int DAYS = 12;
    private static final int ROWS_PER_DAY = 6000;
    private static final int PAGE = 7;
    private static final int GRANITE = 21;

    @TempDir
    static Path directory;

    private static Connection connection;
    private static DatabaseType previousType;
    private static CommandSender console;
    private static World world;
    private static long startTime;

    @BeforeAll
    static void buildHistory() throws Exception {
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = Mockito.mock(org.bukkit.Server.class);
            ConsoleCommandSender serverConsole = Mockito.mock(ConsoleCommandSender.class);
            Mockito.when(server.getConsoleSender()).thenReturn(serverConsole);
            Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
        world = Mockito.mock(World.class);
        Mockito.when(world.getName()).thenReturn("world");
        console = Mockito.mock(CommandSender.class);
        Mockito.when(console.getName()).thenReturn("console");
        Mockito.when(console.hasPermission(Mockito.anyString())).thenReturn(true);
        new PluginChannelListener();
        new PluginChannelHandshakeListener();

        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.worlds.put("world", 1);
        ConfigHandler.worldsReversed.put(1, "world");
        ConfigHandler.materials.put("minecraft:granite", GRANITE);
        for (int player = 1; player <= 12; player++) {
            ConfigHandler.playerIdCache.put("player" + player, player);
            ConfigHandler.playerIdCacheReversed.put(player, "player" + player);
        }
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        Path database = directory.resolve("database.db");
        Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + database), DatabaseType.SQLITE, false);
        connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            SQLiteSchema.createTables("co_", statement);
        }

        long now = System.currentTimeMillis() / 1000L;
        long firstDay = ((now - (DAYS + 20) * DAY) / DAY) * DAY;
        // A window that starts part way through the history, so its edge falls inside a segment.
        startTime = firstDay + (4 * DAY) + (DAY / 3);
        Random random = new Random(11);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,0,NULL,NULL,?,?)")) {
            for (int day = 0; day < DAYS; day++) {
                long dayStart = firstDay + (day * DAY);
                for (int index = 0; index < ROWS_PER_DAY; index++) {
                    // Logged roughly in time order but not exactly, the way a busy consumer does.
                    long time = dayStart + 600 + (index * 12L) + random.nextInt(900);
                    addRow(statement, time, 1 + random.nextInt(12), random.nextInt(8) == 0 ? 2 : 1, random.nextInt(201) - 100, random.nextInt(129),
                            random.nextInt(201) - 100, 20 + random.nextInt(10), random.nextInt(2), random.nextInt(40) == 0 ? 1 : 0);
                }
                statement.executeBatch();
            }
        }
        connection.commit();
        connection.setAutoCommit(true);

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block_all AS SELECT rowid AS id, * FROM co_block");
        }
        long packed = ColdRollupTask.rollUp(connection, () -> {
        });
        assertTrue(packed > 0, "the history has to reach compressed storage");
        SQLiteColdIndex.reload(connection);
        long highWater = SQLiteColdIndex.coldHighWaterMark("block");

        // Rollbacks after packing are recorded beside the segments rather than in them.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_cold_flag (table_id,rowid_ref,rolled_back) SELECT 0,id,1 FROM co_block_all WHERE id <= " + highWater + " AND id % 13 = 0");
            statement.executeUpdate("UPDATE co_block_all SET rolled_back = 1 WHERE id <= " + highWater + " AND id % 13 = 0");
        }

        // Recent rows, which stay live and are merged in.
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,0,NULL,NULL,?,?)")) {
            for (int index = 0; index < 300; index++) {
                addRow(statement, now - 3600 + (index * 5) + random.nextInt(60), 1 + random.nextInt(12), 1, random.nextInt(201) - 100, random.nextInt(129),
                        random.nextInt(201) - 100, 20 + random.nextInt(10), random.nextInt(2), index % 9 == 0 ? 1 : 0);
            }
            statement.executeBatch();
        }
        try (Statement statement = connection.createStatement()) {
            // The newest row of the history is left live by packing, and is already in the copy.
            statement.executeUpdate("INSERT INTO co_block_all SELECT rowid AS id, * FROM co_block WHERE rowid > (SELECT MAX(id) FROM co_block_all)");
        }
    }

    private static void addRow(PreparedStatement statement, long time, int user, int wid, int x, int y, int z, int type, int action, int rolledBack) throws SQLException {
        statement.setLong(1, time);
        statement.setInt(2, user);
        statement.setInt(3, wid);
        statement.setInt(4, x);
        statement.setInt(5, y);
        statement.setInt(6, z);
        statement.setInt(7, type);
        statement.setInt(8, action);
        statement.setInt(9, rolledBack);
        statement.addBatch();
    }

    @AfterAll
    static void tearDown() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void userTypeActionAndTimePagesMatchPlainSql() throws Exception {
        List<Long> expected = ids("SELECT id FROM co_block_all WHERE user = 3 AND type = " + GRANITE + " AND action IN(0,1) AND time > " + startTime + " ORDER BY id DESC");
        verifyPages(expected, Collections.singletonList("player3"), Collections.singletonList("granite"), Arrays.asList(0, 1), null, null, LookupRollbackState.ANY, allPages(expected));
    }

    @Test
    void pagesThatSkipWholeSegmentsStillCountTheLiveRowsAboveThem() throws Exception {
        // One restriction and a window the recorded counts can answer, so whole segments are
        // skipped by arithmetic. Every live row matches and comes before them on the page.
        List<Long> expected = ids("SELECT id FROM co_block_all WHERE action IN(1) AND time > " + startTime + " ORDER BY id DESC");
        // The newest segment holds about three thousand matches, so these pages start among the
        // live rows, just past them, and across the offsets where that segment could be skipped.
        List<Integer> chosen = new ArrayList<>(Arrays.asList(1, 21, 22, 23));
        for (int page = 425; page <= 500; page++) {
            chosen.add(page);
        }
        chosen.add((int) Math.ceil(expected.size() / (double) PAGE));
        verifyPages(expected, Collections.emptyList(), Collections.emptyList(), Collections.singletonList(1), null, null, LookupRollbackState.ANY, chosen);
    }

    @Test
    void mergedPagesRankedByTimeMatchPlainSql() throws Exception {
        // A lookup with no action merges every table by time, and each table is cut to its newest
        // rows by time rather than by row id.
        List<Long> expected = ids("SELECT id FROM co_block_all WHERE user = 5 AND time > " + startTime + " ORDER BY time DESC, id DESC");
        int pages = (int) Math.ceil(expected.size() / (double) PAGE);
        List<Integer> chosen = Arrays.asList(1, 2, 3, 10, 57, pages / 2, pages - 1, pages);
        verifyPages(expected, Collections.singletonList("player5"), Collections.emptyList(), Collections.emptyList(), null, null, LookupRollbackState.ANY, chosen);
    }

    @Test
    void radiusPagesWithHeightBoundsMatchPlainSql() throws Exception {
        Location location = new Location(world, 10, 64, -5);
        Integer[] radius = new Integer[] { 40, -30, 50, 40, 80, -45, 35 };
        List<Long> expected = ids("SELECT id FROM co_block_all WHERE wid = 1 AND x >= -30 AND x <= 50 AND z >= -45 AND z <= 35 AND y >= 40 AND y <= 80 AND action IN(0,1) ORDER BY id DESC");
        int pages = (int) Math.ceil(expected.size() / (double) PAGE);
        List<Integer> chosen = Arrays.asList(1, 2, 5, 40, pages / 2, pages);
        verifyPages(expected, Collections.emptyList(), Collections.emptyList(), Arrays.asList(0, 1), location, radius, LookupRollbackState.ANY, chosen);
    }

    @Test
    void rolledBackPagesMatchPlainSqlIncludingRollbacksRecordedAfterPacking() throws Exception {
        List<Long> expected = ids("SELECT id FROM co_block_all WHERE user = 7 AND action IN(0,1) AND rolled_back IN(1,3) AND time > " + startTime + " ORDER BY id DESC");
        List<Long> overlaid = ids("SELECT id FROM co_block_all WHERE user = 7 AND action IN(0,1) AND rolled_back IN(1,3) AND id % 13 = 0 AND id <= " + SQLiteColdIndex.coldHighWaterMark("block"));
        assertFalse(overlaid.isEmpty(), "some matches are rolled back only in the overlay");
        verifyPages(expected, Collections.singletonList("player7"), Collections.emptyList(), Arrays.asList(0, 1), null, null, LookupRollbackState.ROLLED_BACK, allPages(expected));

        List<Long> notRolledBack = ids("SELECT id FROM co_block_all WHERE user = 7 AND action IN(0,1) AND rolled_back IN(0,2) AND time > " + startTime + " ORDER BY id DESC");
        int pages = (int) Math.ceil(notRolledBack.size() / (double) PAGE);
        verifyPages(notRolledBack, Collections.singletonList("player7"), Collections.emptyList(), Arrays.asList(0, 1), null, null, LookupRollbackState.NOT_ROLLED_BACK,
                Arrays.asList(1, 2, 30, pages));
    }

    @Test
    void aBudgetedReadWritesNoMoreRowsThanItsBudget() throws Exception {
        String[] clauses = { " user IN(3) AND action IN(0,1)", " 1", " wid=1 AND x >= -20 AND x <= 20 AND z >= -20 AND z <= 20 AND y >= 10 AND y <= 30",
                " user IN(9) AND rolled_back IN(0,2) AND time > " + startTime,
                // Alternatives made of comparisons the reader can test are tested exactly too.
                " user IN(3) AND (type = 21 OR type = 22)", " (action IN(0) OR (action = 1 AND type IN(21,22,23)))" };
        for (String clause : clauses) {
            Integer[] bounds = clause.contains("x >=") ? new Integer[] { 20, -20, 20, 10, 30, -20, 20 } : null;
            for (String order : new String[] { "rowid DESC", "time DESC, id DESC" }) {
                String sqlOrder = order.equals("rowid DESC") ? "id DESC" : "time DESC, id DESC";
                List<Long> expected = ids("SELECT id FROM co_block_all WHERE" + clause + " ORDER BY " + sqlOrder);
                for (long budget : new long[] { PAGE, 70, 700, 5000 }) {
                    long offset = budget - PAGE;
                    SQLiteColdIndex.beginLookup(0, 0);
                    try {
                        SQLiteColdIndex.setRowBudget(budget);
                        SQLiteColdIndex.setPageOrder(order);
                        SQLiteColdIndex.setPredicateFilters(clause, "user");
                        String source = SQLiteColdIndex.sourceExpression(connection, "block", bounds == null ? 0 : 1, bounds);
                        long written = source.startsWith("(") ? count("SELECT COUNT(*) FROM temp.cp_cold_block") : 0;
                        assertTrue(written <= budget, "a budget of " + budget + " wrote " + written + " rows for" + clause + " by " + order);
                        assertFalse(SQLiteColdIndex.wasTruncated(), "every row that can reach the page was read");

                        List<Long> page = ids("SELECT rowid FROM " + source + " WHERE" + clause + " ORDER BY " + order.replace(", id DESC", ", rowid DESC")
                                + " LIMIT " + PAGE + " OFFSET " + offset);
                        assertEquals(slice(expected, (int) offset, PAGE), page, "budget " + budget + " for" + clause + " by " + order);
                    }
                    finally {
                        SQLiteColdIndex.endLookup(connection);
                    }
                }
            }
        }
    }

    @Test
    void clausesTheReaderCannotTestFallBackAndStayExact() throws Exception {
        String[] clauses = { " user IN(3) AND (type = 21 OR data % 2 = 0)", " user IN(3) AND meta IS NULL", " user IN(3) AND x BETWEEN -50 AND 50" };
        for (String clause : clauses) {
            List<Long> expected = ids("SELECT id FROM co_block_all WHERE" + clause + " ORDER BY id DESC");
            long budget = 70;
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                SQLiteColdIndex.setRowBudget(budget);
                SQLiteColdIndex.setPageOrder("rowid DESC");
                SQLiteColdIndex.setPredicateFilters(clause, "user");
                String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
                long written = count("SELECT COUNT(*) FROM temp.cp_cold_block");
                assertTrue(written > budget, "a clause the reader cannot test hands every candidate to the query, wrote " + written + " for" + clause);

                List<Long> page = ids("SELECT rowid FROM " + source + " WHERE" + clause + " ORDER BY rowid DESC LIMIT " + PAGE + " OFFSET " + (budget - PAGE));
                assertEquals(slice(expected, (int) (budget - PAGE), PAGE), page, "page for" + clause);
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
        }
    }

    @Test
    void onlyClausesMadeEntirelyOfSimpleComparisonsAreRead() {
        ColdPagePredicate read = ColdPagePredicate.parse(" user IN(1,2) AND (time > '5' AND wid=1) AND action NOT IN(-1) AND 1 AND y <= 70", "user");
        assertNotNull(read);
        assertEquals(5, read.conditions().size());
        // Alternatives are read when every alternative is, and so is a test that a column holds
        // text; an alternative the reader cannot test leaves the whole clause unread.
        ColdPagePredicate alternatives = ColdPagePredicate.parse(" user IN(1) AND (type = 2 OR (type = 3 AND action IN(0,1)))", "user");
        assertNotNull(alternatives);
        assertEquals(2, alternatives.conditions().size());
        assertEquals(" user IN(1) AND (1)", " " + ColdPagePredicate.requiredPart("user IN(1) AND (type = 2 OR (type = 3 AND action IN(0,1)))"));
        assertNotNull(ColdPagePredicate.parse(" user IN(1) AND (LENGTH(line_1) > 0 OR LENGTH(line_2) > 0)", "user"));
        assertNull(ColdPagePredicate.parse(" user IN(1) AND (type = 2 OR type % 3 = 0)", "user"));
        assertNull(ColdPagePredicate.parse(" user IN(1) AND LENGTH(line_1) > 2", "user"));
        assertNull(ColdPagePredicate.parse(" rowid IN(SELECT block_rowid FROM co_entity_spawn WHERE wid=1)", "user"));
        assertNull(ColdPagePredicate.parse(" x BETWEEN 1 AND 2", "user"));
        assertNull(ColdPagePredicate.parse(" message LIKE ? ESCAPE '~'", "user"));
        assertNull(ColdPagePredicate.parse(" time > 'soon'", "user"));
        assertNull(ColdPagePredicate.parse(null, "user"));
    }

    /**
     * Shows the chosen pages through the lookup path and requires each to be exactly the matching
     * slice of the expected rows, and to have written no more packed rows than it could show.
     */
    private void verifyPages(List<Long> expected, List<String> users, List<Object> restrict, List<Integer> actions, Location location, Integer[] radius, LookupRollbackState rollbackState,
            List<Integer> pages) throws Exception {
        assertTrue(expected.size() > 3 * PAGE, "the sample must span several pages, had " + expected.size());
        Long[] rowData = new Long[] { 0L, 0L, 0L, 0L, 0L };
        long lookupStart = radius == null ? startTime : 0;
        try (Statement statement = connection.createStatement()) {
            for (int page : pages) {
                if (page < 1) {
                    continue;
                }
                int offset = (page - 1) * PAGE;
                long writtenBefore = ColdSegmentReadCounter.scratchRows();
                List<Object[]> rows = LookupRaw.performLookupRaw(statement, console, Collections.emptyList(), users, new ArrayList<>(restrict), new HashMap<>(), Collections.emptyList(),
                        new ArrayList<>(actions), EntityActionFilter.NONE, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), location, radius,
                        rowData, lookupStart, 0, offset, PAGE, radius != null, true, null, rollbackState);
                long written = ColdSegmentReadCounter.scratchRows() - writtenBefore;

                List<Long> actual = new ArrayList<>();
                for (Object[] row : rows) {
                    actual.add(((Number) row[0]).longValue());
                }
                assertEquals(slice(expected, offset, PAGE), actual, "page " + page);
                assertTrue(written <= offset + PAGE, "page " + page + " wrote " + written + " packed rows for a page ending at row " + (offset + PAGE));
            }
        }
    }

    private static List<Integer> allPages(List<Long> expected) {
        List<Integer> pages = new ArrayList<>();
        int count = (int) Math.ceil(expected.size() / (double) PAGE) + 1;
        for (int page = 1; page <= count; page++) {
            pages.add(page);
        }
        return pages;
    }

    private static List<Long> slice(List<Long> rows, int offset, int size) {
        if (offset >= rows.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(rows.subList(offset, Math.min(rows.size(), offset + size)));
    }

    private static long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private static List<Long> ids(String query) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                ids.add(results.getLong(1));
            }
        }
        return ids;
    }
}
