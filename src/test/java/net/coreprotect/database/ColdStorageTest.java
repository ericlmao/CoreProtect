package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * End to end coverage of the compressed cold storage: rows are rolled out of the live table into a
 * segment, the live table shrinks, and the rows read back through the ordinary lookup path with
 * their original row ids and values.
 */
class ColdStorageTest {

    private static final long DAY = 86400L;

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
    void rollsOldRowsIntoASegmentAndReadsThemBack() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;

        List<Object[]> written = insertBlocks(oldTime, 500, 1);
        insertBlocks(now - 60, 5, 1); // recent rows, which must stay in the live table

        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });

        assertEquals(500, sealed);
        assertEquals(5, count("SELECT COUNT(*) FROM co_block"));
        assertEquals(1, count("SELECT COUNT(*) FROM co_segment"));
        assertEquals(500, count("SELECT row_count FROM co_segment"));

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertTrue(source.startsWith("("), "a lookup covering cold data reads from a table expression");

            List<Object[]> read = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT rowid AS id,time,user,wid,x,y,z,type,meta,rolled_back FROM " + source + " WHERE wid = 1 ORDER BY rowid")) {
                while (results.next()) {
                    read.add(new Object[] { results.getLong("id"), results.getLong("time"), results.getLong("user"), results.getLong("x"), results.getLong("y"), results.getLong("z"), results.getBytes("meta") });
                }
            }

            assertEquals(505, read.size(), "cold rows and live rows are read together");
            for (int index = 0; index < written.size(); index++) {
                Object[] expected = written.get(index);
                Object[] actual = read.get(index);
                assertEquals(expected[0], actual[0], "row id is preserved");
                assertEquals(expected[1], actual[1], "time is preserved");
                assertEquals(expected[2], actual[2], "user is preserved");
                assertEquals(expected[3], actual[3], "x is preserved");
                assertEquals(expected[5], actual[5], "z is preserved");
                assertTrue(Arrays.equals((byte[]) expected[6], (byte[]) actual[6]), "payload is preserved");
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void readsOnlyWhatAPageNeedsAndStillReturnsEveryRowWhenAsked() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        // Three days of activity, which seals as three segments, so a page can be satisfied from
        // the newest of them alone.
        insertBlocks(oldTime, 200, 1);
        insertBlocks(oldTime + DAY, 200, 1);
        insertBlocks(oldTime + (2 * DAY), 200, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        SQLiteColdIndex.beginLookup(0, 0);
        long budgeted;
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            SQLiteColdIndex.setRowBudget(5);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            budgeted = count("SELECT COUNT(*) FROM temp.cp_cold_block");
            assertTrue(source.startsWith("("), "the page still reads compressed rows");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        SQLiteColdIndex.beginLookup(0, 0);
        long complete;
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            SQLiteColdIndex.setRowBudget(0);
            SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            complete = count("SELECT COUNT(*) FROM temp.cp_cold_block");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        assertEquals(24, complete, "without a budget every row for the user is read");
        assertTrue(budgeted >= 5, "a budgeted read still covers the page");
        assertTrue(budgeted < complete, "a budgeted read stops early");
    }

    @Test
    void ignoresABudgetSetBeforeTheLookupBegins() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 200, 1);
        insertBlocks(oldTime + DAY, 200, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        // Setting a budget with no lookup in progress must not appear to succeed: the value would
        // be discarded by the next lookup and the read would quietly become an unbudgeted one.
        SQLiteColdIndex.setRowBudget(5);
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertEquals(16, count("SELECT COUNT(*) FROM temp.cp_cold_block"),
                    "a budget set before the lookup started is not in force");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            SQLiteColdIndex.setRowBudget(5);
            SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertTrue(count("SELECT COUNT(*) FROM temp.cp_cold_block") < 16, "a budget set during the lookup limits the read");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void countsWithoutBuildingRowsAndRespectsTheActionFilter() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 200, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        // The generator alternates the action of every row, so half of user 1's rows are action 0.
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "", "0");
            SQLiteColdIndex.setCounting(true);
            assertEquals(ConfigHandler.prefix + "block", SQLiteColdIndex.sourceExpression(connection, "block", 1, null),
                    "counting reads no rows into the temporary table");
            assertEquals(4, SQLiteColdIndex.countedRows("block"));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        // Without the action filter every row of that user counts.
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "", null);
            SQLiteColdIndex.setCounting(true);
            SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertEquals(8, SQLiteColdIndex.countedRows("block"));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void countsRowsPerUserWithoutOpeningSegments() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 500, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        byte[] stored;
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT user_stats FROM co_segment LIMIT 1")) {
            results.next();
            stored = results.getBytes(1);
        }

        SegmentStatistics statistics = SegmentStatistics.decode(stored);
        assertNotNull(statistics, "the roll-up records how many rows each user contributed");
        // The generator cycles through 25 users, so each owns a twenty fifth of the segment.
        assertEquals(20, statistics.count(1));
        assertEquals(0, statistics.count(9999));
    }

    @Test
    void readsSegmentsWrittenBeforeTheExactMembershipFormat() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 200, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });

        // Rewrite the membership the way earlier builds stored it: a bare filter with no tag byte.
        SegmentFilter legacy = new SegmentFilter(SegmentFilter.SMALL_BYTES);
        for (long user = 1; user <= 25; user++) {
            legacy.add(user);
        }
        try (PreparedStatement statement = connection.prepareStatement("UPDATE co_segment SET user_filter = ?")) {
            statement.setBytes(1, legacy.toBytes());
            statement.executeUpdate();
        }
        SQLiteColdIndex.invalidate();
        SQLiteColdIndex.reload(connection);

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertTrue(source.startsWith("("), "a segment stored in the older format is still read");
            assertEquals(8, count("SELECT COUNT(*) FROM " + source + " WHERE user = 1"));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void skipsSegmentsThatHoldNoneOfTheRequestedUsers() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 200, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        // The rows were written for user 1; a lookup for a different player must not open them.
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("9999", "");
            assertEquals(ConfigHandler.prefix + "block", SQLiteColdIndex.sourceExpression(connection, "block", 1, null),
                    "a lookup for an absent user reads the live table only");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertTrue(source.startsWith("("), "a lookup for a user that is present still reads the segment");
            // The generator cycles through 25 users, so user 1 owns every twenty-fifth row.
            assertEquals(8, count("SELECT COUNT(*) FROM " + source + " WHERE user = 1"));
            assertEquals(8, count("SELECT COUNT(*) FROM temp.cp_cold_block"), "only the requested user's rows are decoded");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void reportsTheSplitBetweenLiveAndCompressedStorage() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 2000, 1);
        insertBlocks(now - 60, 50, 1);

        ColdStorageStats before = ColdStorageStats.read(connection);
        assertNotNull(before);
        assertEquals(0, before.getColdRows());
        assertTrue(before.getHotBytes() > 0, "live rows occupy space before anything is compressed");

        ColdRollupTask.rollUp(connection, () -> {
        });

        ColdStorageStats after = ColdStorageStats.read(connection);
        assertNotNull(after);
        assertEquals(2000, after.getColdRows());
        assertTrue(after.getColdBytes() > 0, "compressed segments occupy space");
        assertTrue(after.getColdBytes() < before.getHotBytes(), "compressed storage is smaller than the rows it replaced");
        assertTrue(ColdStorageStats.format(5L * 1024 * 1024).endsWith(" MB"));
        assertTrue(ColdStorageStats.format(3L * 1024 * 1024 * 1024).endsWith(" GB"));
    }

    @Test
    void packsDataFromTodayWhenGivenAnExplicitCutoff() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        insertBlocks(now - 400, 300, 1); // written moments ago, all of it before the cutoff

        // The scheduled run leaves recent data alone, which is what the hot window is for.
        assertEquals(0, ColdRollupTask.rollUp(connection, () -> {
        }));
        assertEquals(300, count("SELECT COUNT(*) FROM co_block"));

        // A manual compact packs everything written so far, however recent it is.
        long sealed = ColdRollupTask.rollUp(connection, () -> {
        }, now);

        assertEquals(300, sealed);
        assertEquals(0, count("SELECT COUNT(*) FROM co_block"));
        assertEquals(300, count("SELECT SUM(row_count) FROM co_segment"));
    }

    @Test
    void skipsSegmentsThatCannotHoldTheRequestedChunk() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 200, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });

        SQLiteColdIndex.reload(connection);
        // The rows were written around x/z 0-199; a lookup far away cannot match this segment.
        List<SQLiteColdIndex.ColdSegment> far = SQLiteColdIndex.selectSegments(connection, "block", 1,
                new Integer[] { 0, 900000, 900100, 0, 0, 900000, 900100 }, 0, 0, null);
        assertEquals(0, far.size());

        List<SQLiteColdIndex.ColdSegment> near = SQLiteColdIndex.selectSegments(connection, "block", 1,
                new Integer[] { 0, 0, 32, 0, 0, 0, 32 }, 0, 0, null);
        assertEquals(1, near.size());
    }

    @Test
    void keepsSegmentsOutOfLookupsThatPredateThem() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 100, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        List<SQLiteColdIndex.ColdSegment> recentOnly = SQLiteColdIndex.selectSegments(connection, "block", 0, null, now - DAY, 0, null);
        assertEquals(0, recentOnly.size(), "a lookup for the last day never reads month old segments");
    }

    @Test
    void appliesRollbackStateRecordedForColdRows() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 50, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        try (PreparedStatement statement = connection.prepareStatement("INSERT OR REPLACE INTO co_cold_flag (table_id,rowid_ref,rolled_back) VALUES (?,?,?)")) {
            statement.setInt(1, SQLiteColdIndex.tableId("block"));
            statement.setLong(2, 3);
            statement.setInt(3, 1);
            statement.executeUpdate();
        }

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT rolled_back FROM " + source + " WHERE rowid = 3")) {
                assertTrue(results.next());
                assertEquals(1, results.getInt(1), "the recorded rollback state wins over the stored one");
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void dropsExpiredSegmentsWholesale() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (400 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 120, 1);
        ColdRollupTask.rollUp(connection, () -> {
        });
        assertEquals(1, count("SELECT COUNT(*) FROM co_segment"));

        long removed = PurgeExecutor.purgeColdSegments(connection, now - (180 * DAY), Connection::prepareStatement, () -> {
        });

        assertEquals(120, removed);
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment"));
    }

    @Test
    void storesSegmentsFarSmallerThanTheRowsTheyReplace() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 20000, 1);

        long before = count("SELECT SUM(LENGTH(meta) + 40) FROM co_block");
        ColdRollupTask.rollUp(connection, () -> {
        });
        long after = count("SELECT SUM(LENGTH(scalars) + LENGTH(COALESCE(payload,''))) FROM co_segment");

        assertTrue(after * 3 < before, "a segment should be several times smaller than the rows it replaces, was " + after + " against " + before);
    }

    private List<Object[]> insertBlocks(long baseTime, int rows, int worldId) throws SQLException {
        List<Object[]> written = new ArrayList<>();
        String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        long startingRowId = count("SELECT COALESCE(MAX(rowid),0) FROM co_block");

        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int index = 0; index < rows; index++) {
                long time = baseTime + index;
                int user = 1 + (index % 25);
                int x = index % 200;
                int y = 64;
                int z = index % 200;
                byte[] meta = payload(index);

                statement.setLong(1, time);
                statement.setInt(2, user);
                statement.setInt(3, worldId);
                statement.setInt(4, x);
                statement.setInt(5, y);
                statement.setInt(6, z);
                statement.setInt(7, 10 + (index % 40));
                statement.setInt(8, 0);
                statement.setBytes(9, meta);
                statement.setNull(10, java.sql.Types.BLOB);
                statement.setInt(11, index % 2);
                statement.setInt(12, 0);
                statement.addBatch();

                written.add(new Object[] { startingRowId + index + 1, time, (long) user, (long) x, (long) y, (long) z, meta });
            }
            statement.executeBatch();
        }

        return written;
    }

    /** A payload shaped like the item metadata CoreProtect stores: repetitive, a few hundred bytes. */
    private static byte[] payload(int index) {
        StringBuilder builder = new StringBuilder();
        builder.append("{meta-type=UNSPECIFIC, display-name=Item ").append(index % 50);
        builder.append(", enchants={minecraft:sharpness=").append(index % 5);
        builder.append(", minecraft:unbreaking=3}, lore=[Forged in the nether, A relic of the old world]");
        builder.append(", Damage=").append(index % 250).append(", repair-cost=").append(index % 40).append('}');
        return builder.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            assertNotNull(results);
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
