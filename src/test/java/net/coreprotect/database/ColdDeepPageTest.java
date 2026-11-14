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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.listener.channel.PluginChannelHandshakeListener;
import net.coreprotect.listener.channel.PluginChannelListener;
import net.coreprotect.model.action.EntityActionFilter;
import net.coreprotect.model.action.LookupActions;
import net.coreprotect.model.item.ItemTransactionActions;
import net.coreprotect.model.lookup.EntityLookupContext;
import net.coreprotect.model.lookup.LookupRollbackState;
import net.coreprotect.model.lookup.LookupSummaryPage;
import net.coreprotect.model.lookup.LookupSummaryRow;

/**
 * Pages deep into a long, compacted history, through the lookup path a command takes, compared row
 * for row with plain SQL over the same rows never packed.
 *
 * <p>
 * One player owns hundreds of thousands of rows across placed blocks, containers and items, packed
 * and then compacted so each day is a few segments spanning the day's whole row range. Every page
 * checked, the first, one in the middle, the last, a last page only partly full, one past the end,
 * and pages starting among thousands of rows logged in the same second, has to be exactly the rows
 * plain SQL gives, and has to write no more rows to temporary storage than the page shows.
 * </p>
 */
class ColdDeepPageTest {

    private static final long DAY = 86400L;
    private static final int USER = 7;
    private static final int DAYS = 6;
    private static final int BLOCKS_PER_DAY = 150_000;
    private static final int CONTAINERS_PER_DAY = 20_000;
    private static final int ITEMS_PER_DAY = 20_000;
    private static final int BURST_BLOCKS = 3000;
    private static final int BURST_CONTAINERS = 400;
    private static final int BURST_ITEMS = 400;
    private static final int PAGE = 5;

    @TempDir
    static Path directory;

    private static Connection connection;
    private static CommandSender console;
    private static DatabaseType previousType;
    private static long startTime;
    private static long burstTime;

    @BeforeAll
    static void buildHistory() throws Exception {
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = Mockito.mock(org.bukkit.Server.class);
            ConsoleCommandSender serverConsole = Mockito.mock(ConsoleCommandSender.class);
            Mockito.when(server.getConsoleSender()).thenReturn(serverConsole);
            Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
        console = Mockito.mock(CommandSender.class);
        Mockito.when(console.getName()).thenReturn("console");
        new PluginChannelListener();
        new PluginChannelHandshakeListener();

        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.playerIdCache.put("deeptester", USER);
        ConfigHandler.playerIdCacheReversed.put(USER, "deeptester");
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ColdRollupTask.setSegmentRowsForTesting(20_000);
        SegmentCompactor.setTargetRowsForTesting(60_000);

        Path database = directory.resolve("database.db");
        Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + database), DatabaseType.SQLITE, false);
        connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            SQLiteSchema.createTables("co_", statement);
        }

        long now = System.currentTimeMillis() / 1000L;
        long firstDay = ((now / DAY) - (DAYS + 2)) * DAY;
        startTime = firstDay - 1;
        burstTime = firstDay + (2 * DAY) + 40_000;
        writeHistory(firstDay);

        try (Statement statement = connection.createStatement()) {
            for (String table : new String[] { "block", "container", "item" }) {
                statement.executeUpdate("CREATE TABLE co_" + table + "_all AS SELECT rowid AS id, * FROM co_" + table);
                statement.executeUpdate("CREATE INDEX co_" + table + "_all_user ON co_" + table + "_all(user,time)");
            }
        }
        assertTrue(ColdRollupTask.rollUp(connection, () -> {
        }) > 0, "the history is packed");
        assertTrue(SegmentCompactor.compactAll(connection, () -> {
        }) > 0, "the packed days are compacted");
        SQLiteColdIndex.reload(connection);

        // Recent activity that has not been packed yet, which every page ranks alongside.
        writeLive(now - 3600);
        try (Statement statement = connection.createStatement()) {
            for (String table : new String[] { "block", "container", "item" }) {
                statement.executeUpdate("INSERT INTO co_" + table + "_all SELECT rowid AS id, * FROM co_" + table + " WHERE rowid > (SELECT MAX(id) FROM co_" + table + "_all)");
            }
        }
    }

    @AfterAll
    static void closeDatabase() throws SQLException {
        ColdRollupTask.setSegmentRowsForTesting(0);
        SegmentCompactor.setTargetRowsForTesting(0);
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @AfterEach
    void restoreLimit() {
        SQLiteColdIndex.setMaterializeLimitForTesting(0);
    }

    @Test
    void mergedPagesAtEveryDepthMatchPlainSql() throws Exception {
        String excluded = ItemTransactionActions.BREAK + "," + ItemTransactionActions.DESTROY + "," + ItemTransactionActions.CREATE + "," + ItemTransactionActions.SELL + ","
                + ItemTransactionActions.BUY;
        List<String> expected = strings("SELECT tbl || ':' || id FROM ("
                + "SELECT 0 AS tbl, id, time FROM co_block_all WHERE user = " + USER + " AND time > " + startTime
                + " UNION ALL SELECT 1, id, time FROM co_container_all WHERE user = " + USER + " AND time > " + startTime
                + " UNION ALL SELECT 2, id, time FROM co_item_all WHERE user = " + USER + " AND time > " + startTime + " AND action NOT IN(" + excluded + ")"
                + ") ORDER BY time DESC, tbl DESC, id DESC");
        assertTrue(expected.size() > 400_000, "the player owns a long history, " + expected.size() + " rows");

        int total = expected.size();
        int burst = firstIndexAt(expected, burstTime);
        List<Integer> offsets = new ArrayList<>(Arrays.asList(0, 1, 199, 200, 201, total / 10, total / 2, total / 2 + 3, (total * 99) / 100, total - PAGE, total - 2, total, total + 10));
        // Pages starting among the thousands of rows logged in one second: among the items, which
        // rank first there, across into the containers, across into the blocks, and among them.
        offsets.addAll(Arrays.asList(burst - 2, burst + 250, burst + BURST_ITEMS - 2, burst + BURST_ITEMS + 1, burst + BURST_ITEMS + BURST_CONTAINERS - 3, burst + 2000));

        List<Integer> actions = new ArrayList<>();
        for (int offset : offsets) {
            long writtenBefore = ColdSegmentReadCounter.scratchRows();
            ColdSegmentReadCounter.resetPeakHeldRows();
            List<Object[]> rows = page(actions, offset);
            long written = ColdSegmentReadCounter.scratchRows() - writtenBefore;

            List<String> actual = new ArrayList<>();
            for (Object[] row : rows) {
                actual.add(row[14] + ":" + row[0]);
            }
            assertEquals(slice(expected, offset), actual, "merged page at offset " + offset + " of " + total);
            assertTrue(written <= PAGE, "the page at offset " + offset + " wrote " + written + " rows to temporary storage");
            assertTrue(ColdSegmentReadCounter.peakHeldRows() <= PAGE, "the page at offset " + offset + " held " + ColdSegmentReadCounter.peakHeldRows() + " rows at once");
        }
    }

    @Test
    void pagesOfOneTableAtEveryDepthMatchPlainSql() throws Exception {
        List<String> expected = strings("SELECT id FROM co_block_all WHERE user = " + USER + " AND action IN(0,1) AND time > " + startTime + " ORDER BY id DESC");
        int total = expected.size();
        assertTrue(total > 300_000, "the player placed and broke many blocks, " + total);

        List<Integer> actions = new ArrayList<>(Arrays.asList(LookupActions.BLOCK_BREAK, LookupActions.BLOCK_PLACE));
        for (int offset : new int[] { 0, 1, 199, 200, 201, total / 10, total / 2, (total * 99) / 100, total - PAGE, total - 2, total, total + 3 }) {
            long writtenBefore = ColdSegmentReadCounter.scratchRows();
            List<Object[]> rows = page(actions, offset);
            long written = ColdSegmentReadCounter.scratchRows() - writtenBefore;
            List<String> actual = new ArrayList<>();
            for (Object[] row : rows) {
                actual.add(String.valueOf(row[0]));
            }
            assertEquals(slice(expected, offset), actual, "page of one table at offset " + offset + " of " + total);
            assertTrue(written <= PAGE, "the page at offset " + offset + " wrote " + written + " rows to temporary storage");
        }
    }

    @Test
    void theCountMatchesPlainSqlWithoutBuildingARow() throws Exception {
        long expected = count("SELECT COUNT(*) FROM co_block_all WHERE user = " + USER + " AND action IN(0,1) AND time > " + startTime);
        long writtenBefore = ColdSegmentReadCounter.scratchRows();
        try (Statement statement = connection.createStatement()) {
            long counted = Lookup.countLookupRows(statement, console, Collections.emptyList(), Collections.singletonList("deeptester"), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                    new ArrayList<>(Arrays.asList(LookupActions.BLOCK_BREAK, LookupActions.BLOCK_PLACE)), EntityActionFilter.NONE, Collections.emptyList(),
                    EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, new Long[] { 0L, 0L, 0L, 0L, 0L }, startTime, 0, false, true, null,
                    LookupRollbackState.ANY);
            assertEquals(expected, counted);
        }
        assertEquals(0, ColdSegmentReadCounter.scratchRows() - writtenBefore, "a count builds no rows");
    }

    @Test
    void aClauseTheReaderCannotTestWalksTheHistoryAWindowAtATime() throws Exception {
        // Arithmetic is something only the query can evaluate, so the page cannot be planned. It
        // has to stay exact, and no window may ever hold more rows than one lookup may.
        long limit = 40_000;
        SQLiteColdIndex.setMaterializeLimitForTesting(limit);
        String clause = " user = " + USER + " AND (action = 0 OR type % 3 = 0) AND time > " + startTime;
        List<String> expected = strings("SELECT id FROM co_block_all WHERE" + clause + " ORDER BY id DESC");
        int total = expected.size();
        for (int offset : new int[] { 0, total / 2, total - 2 }) {
            ColdSegmentReadCounter.resetPeakHeldRows();
            List<String> actual = new ArrayList<>();
            SQLiteColdIndex.beginLookup(startTime, 0);
            try (Statement statement = connection.createStatement()) {
                ColdLookupQuery query = ColdLookupQuery.forLookup(connection, "user");
                String source = query.source("block", clause, 0, 0, null);
                try (ResultSet results = query.page(statement, "SELECT 0 as tbl,rowid as id FROM " + source + " WHERE" + clause, " ORDER BY rowid DESC", offset, PAGE, Collections.emptyList())) {
                    while (results.next()) {
                        actual.add(String.valueOf(results.getLong("id")));
                    }
                }
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
            assertEquals(slice(expected, offset), actual, "walked page at offset " + offset + " of " + total);
            assertTrue(ColdSegmentReadCounter.peakHeldRows() <= limit, "a window held " + ColdSegmentReadCounter.peakHeldRows() + " rows, over the limit of " + limit);
            assertTrue(ColdSegmentReadCounter.peakHeldRows() > 0, "the walk read packed rows");
        }

        // A count of the same clause walks it too, and so does a read of every matching row.
        ColdSegmentReadCounter.resetPeakHeldRows();
        SQLiteColdIndex.beginLookup(startTime, 0);
        try (Statement statement = connection.createStatement()) {
            ColdLookupQuery query = ColdLookupQuery.forLookup(connection, "user");
            String source = query.source("block", clause, 0, 0, null);
            try (ResultSet results = query.count(statement, "SELECT 0 as tbl," + query.countColumn("block") + " FROM " + source + " WHERE" + clause, "", Collections.emptyList())) {
                assertTrue(results.next());
                assertEquals(total, results.getLong("count"), "the walked count");
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        assertTrue(ColdSegmentReadCounter.peakHeldRows() <= limit, "a counted window held " + ColdSegmentReadCounter.peakHeldRows() + " rows");

        ColdSegmentReadCounter.resetPeakHeldRows();
        List<String> everything = new ArrayList<>();
        SQLiteColdIndex.beginLookup(startTime, 0);
        try (Statement statement = connection.createStatement()) {
            ColdLookupQuery query = ColdLookupQuery.forLookup(connection, "user");
            String source = query.source("block", clause, 0, 0, null);
            try (ResultSet results = query.all(statement, "SELECT 0 as tbl,rowid as id FROM " + source + " WHERE" + clause, " ORDER BY rowid DESC", Collections.emptyList())) {
                while (results.next()) {
                    everything.add(String.valueOf(results.getLong("id")));
                }
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        assertEquals(expected, everything, "every matching row, newest first");
        assertTrue(ColdSegmentReadCounter.peakHeldRows() <= limit, "a window of the full read held " + ColdSegmentReadCounter.peakHeldRows() + " rows");
    }

    @Test
    void aReadLargerThanOneLookupMayHoldIsRefusedRatherThanWritten() throws Exception {
        SQLiteColdIndex.setMaterializeLimitForTesting(1000);
        ColdSegmentReadCounter.resetPeakHeldRows();
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters(Integer.toString(USER), "");
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            assertEquals("co_block", source, "the read falls back to live rows alone");
            assertTrue(SQLiteColdIndex.readFailed(), "and says the packed rows could not be read");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        assertTrue(ColdSegmentReadCounter.peakHeldRows() <= 1000, "nothing over the limit was ever held");
    }

    @Test
    void aSummaryOfTheWholeHistoryIsTakenAWindowAtATimeAndMatchesTheUnpackedRows() throws Exception {
        // The same rows, never packed, in a database of their own.
        Path plain = directory.resolve("plain.db");
        Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + plain), DatabaseType.SQLITE, false);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("ATTACH DATABASE '" + plain + "' AS plain");
            for (String table : new String[] { "block", "container", "item" }) {
                String columns = String.join(",", strings("SELECT name FROM pragma_table_info('co_" + table + "')"));
                statement.executeUpdate("INSERT INTO plain.co_" + table + " (rowid," + columns + ") SELECT id," + columns + " FROM co_" + table + "_all");
            }
            statement.executeUpdate("DETACH DATABASE plain");
        }
        LookupSummaryPage expected;
        long expectedGroups;
        SQLiteColdIndex.invalidate();
        try (Connection plainConnection = DriverManager.getConnection("jdbc:sqlite:" + plain)) {
            try (Statement statement = plainConnection.createStatement()) {
                SQLiteSchema.createTables("co_", statement);
            }
            SQLiteColdIndex.reload(plainConnection);
            expected = summary(plainConnection);
            expectedGroups = summaryGroups(plainConnection);
        }
        finally {
            SQLiteColdIndex.invalidate();
            SQLiteColdIndex.reload(connection);
        }
        assertTrue(expected.getRecordRows() > 400_000, "the summary covers the whole history, " + expected.getRecordRows() + " rows");

        long limit = 50_000;
        SQLiteColdIndex.setMaterializeLimitForTesting(limit);
        ColdSegmentReadCounter.resetPeakHeldRows();
        LookupSummaryPage actual = summary(connection);
        assertEquals(describe(expected), describe(actual), "the summary of the packed history");
        assertEquals(expectedGroups, summaryGroups(connection), "the number of summarised groups");
        assertTrue(ColdSegmentReadCounter.peakHeldRows() <= limit, "a window of the summary held " + ColdSegmentReadCounter.peakHeldRows() + " rows");
        assertTrue(ColdSegmentReadCounter.peakHeldRows() > 0, "the summary read packed rows");
    }

    private static LookupSummaryPage summary(Connection target) throws Exception {
        try (Statement statement = target.createStatement()) {
            return Lookup.performSummaryLookupPage(statement, console, Collections.emptyList(), Collections.singletonList("deeptester"), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                    new ArrayList<>(), EntityActionFilter.NONE, EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, startTime, 0, 0, 50, false, null,
                    LookupRollbackState.ANY);
        }
    }

    private static long summaryGroups(Connection target) throws Exception {
        try (Statement statement = target.createStatement()) {
            return Lookup.countSummaryRows(statement, console, Collections.emptyList(), Collections.singletonList("deeptester"), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                    new ArrayList<>(), EntityActionFilter.NONE, EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, startTime, 0, false, null,
                    LookupRollbackState.ANY);
        }
    }

    private static List<String> describe(LookupSummaryPage page) {
        List<String> described = new ArrayList<>();
        described.add("groups=" + page.getTotalRows() + " records=" + page.getRecordRows());
        for (LookupSummaryRow row : page.getRows()) {
            described.add(row.getUserId() + ":" + row.getMaterialId() + " removed " + row.getRemovedAmount() + " placed " + row.getPlacedAmount() + " net " + row.getAmount());
        }
        return described;
    }

    private List<Object[]> page(List<Integer> actions, int offset) throws Exception {
        try (Statement statement = connection.createStatement()) {
            return LookupRaw.performLookupRaw(statement, console, Collections.emptyList(), Collections.singletonList("deeptester"), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                    new ArrayList<>(actions), EntityActionFilter.NONE, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null,
                    new Long[] { 0L, 0L, 0L, 0L, 0L }, startTime, 0, offset, PAGE, false, true, null, LookupRollbackState.ANY);
        }
    }

    private static int firstIndexAt(List<String> expected, long time) throws SQLException {
        long above = count("SELECT (SELECT COUNT(*) FROM co_block_all WHERE user = " + USER + " AND time > " + time + ") + (SELECT COUNT(*) FROM co_container_all WHERE user = " + USER
                + " AND time > " + time + ") + (SELECT COUNT(*) FROM co_item_all WHERE user = " + USER + " AND time > " + time + ")");
        assertTrue(above > 0 && above < expected.size());
        return (int) above;
    }

    /**
     * Writes several days of activity for several players, the one looked up owning two rows in
     * every five, with a burst of thousands of rows in one second in every table.
     */
    private static void writeHistory(long firstDay) throws SQLException {
        connection.setAutoCommit(false);
        try (PreparedStatement block = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,NULL,?,?,0)");
                PreparedStatement container = connection.prepareStatement("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,?,?,?,0)");
                PreparedStatement item = connection.prepareStatement("INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,1,?,64,?,?,?,?,?,0)")) {
            for (int day = 0; day < DAYS; day++) {
                long dayStart = firstDay + (day * DAY);
                writeTable(block, dayStart, BLOCKS_PER_DAY, BURST_BLOCKS, 0);
                writeTable(container, dayStart, CONTAINERS_PER_DAY, BURST_CONTAINERS, 1);
                writeTable(item, dayStart, ITEMS_PER_DAY, BURST_ITEMS, 2);
            }
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private static void writeTable(PreparedStatement statement, long dayStart, int rows, int burst, int kind) throws SQLException {
        boolean burstWritten = false;
        for (int index = 0; index < rows; index++) {
            long time = dayStart + 60 + ((index * 86_000L) / rows);
            if (!burstWritten && time >= burstTime && burstTime >= dayStart && burstTime < dayStart + DAY) {
                for (int extra = 0; extra < burst; extra++) {
                    bind(statement, burstTime, USER, index + extra, kind);
                }
                burstWritten = true;
            }
            int user = (index % 5) < 2 ? USER : 1 + (index % 6);
            bind(statement, time, user, index, kind);
            if (index % 5000 == 4999) {
                statement.executeBatch();
            }
        }
        statement.executeBatch();
    }

    private static void bind(PreparedStatement statement, long time, int user, int index, int kind) throws SQLException {
        statement.setLong(1, time);
        statement.setInt(2, user);
        statement.setInt(3, ((index * 37) % 2000) - 1000);
        statement.setInt(4, ((index * 91) % 2000) - 1000);
        statement.setInt(5, 1 + (index % 50));
        if (kind == 0) {
            statement.setBytes(6, index % 3 == 0 ? new byte[] { (byte) index, 1, 2 } : null);
            statement.setInt(7, index % 2);
        }
        else if (kind == 1) {
            statement.setInt(6, 1 + (index % 64));
            statement.setBytes(7, index % 4 == 0 ? new byte[] { (byte) index, 3 } : null);
            statement.setInt(8, index % 2);
        }
        else {
            statement.setBytes(6, new byte[] { (byte) index, 4, 5 });
            statement.setInt(7, 1 + (index % 64));
            statement.setInt(8, ItemTransactionActions.DROP + (index % 2));
        }
        statement.addBatch();
    }

    private static void writeLive(long time) throws SQLException {
        connection.setAutoCommit(false);
        try (PreparedStatement block = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,NULL,?,?,0)");
                PreparedStatement container = connection.prepareStatement("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,?,?,?,0)");
                PreparedStatement item = connection.prepareStatement("INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,1,?,64,?,?,?,?,?,0)")) {
            for (int index = 0; index < 200; index++) {
                bind(block, time + (index / 3), index % 2 == 0 ? USER : 2, index, 0);
                bind(container, time + (index / 3), index % 2 == 0 ? USER : 2, index, 1);
                bind(item, time + (index / 3), index % 2 == 0 ? USER : 2, index, 2);
            }
            block.executeBatch();
            container.executeBatch();
            item.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private static List<String> slice(List<String> rows, int offset) {
        if (offset >= rows.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(rows.subList(offset, Math.min(rows.size(), offset + PAGE)));
    }

    private static long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private static List<String> strings(String query) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                values.add(results.getString(1));
            }
        }
        return values;
    }
}
