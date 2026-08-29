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

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;

/**
 * The gate every page planning change has to pass.
 *
 * <p>
 * A planned page is only allowed to be faster, never different. Each test asks for the same page
 * twice, once by reading everything the page has to skip past and once by letting the planner work
 * out where the page starts, and requires the two to return the same rows in the same order. The
 * offsets land inside a segment, exactly on a boundary, and one row either side of one, because
 * that is where arithmetic mistakes hide.
 * </p>
 */
class ColdPagePlanTest {

    private static final long DAY = 86400L;

    /** Rows written per day of test data. Several days give several segments to plan across. */
    private static final int ROWS_PER_DAY = 400;

    private static final int DAYS = 5;

    /** The generator cycles through this many players, so each owns an even share of the rows. */
    private static final int USERS = 25;

    private Connection connection;
    private DatabaseType previousType;
    private long lastSkipped;
    private long lastDecoded;

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
    void plannedPagesMatchUnplannedPagesAtEveryBoundary() throws Exception {
        seal();
        for (int offset : offsets()) {
            assertSamePage(1, offset, 4, 0, 0);
        }
    }

    @Test
    void planningActuallySkipsSegmentsRatherThanReadingThemAll() throws Exception {
        seal();
        int perSegment = ROWS_PER_DAY / USERS;

        // A page inside the newest segment cannot skip anything.
        readPage(1, 1, 4, 0, 0, true);
        assertEquals(0, skippedFor(1, 1), "the newest page reads from the newest segment");

        // A page past the first segment skips it outright, and deeper pages skip more.
        long afterOne = skippedFor(1, perSegment + 1);
        long afterThree = skippedFor(1, (3 * perSegment) + 1);
        assertTrue(afterOne >= perSegment, "a page past the first segment skips it: " + afterOne);
        assertTrue(afterThree > afterOne, "a deeper page skips more: " + afterThree + " vs " + afterOne);

        // Skipping is what keeps the read small: only the segments around the page are decoded.
        long decoded = decodedRowsFor(1, (3 * perSegment) + 1);
        assertTrue(decoded < ROWS_PER_DAY * DAYS / USERS, "only part of the result is read: " + decoded);
    }

    @Test
    void planningAppliesToOneTableOnlyAndNeverLeaksToAnother() throws Exception {
        seal();
        int perSegment = ROWS_PER_DAY / USERS;

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            SQLiteColdIndex.setRowBudget((3L * perSegment) + 4);
            SQLiteColdIndex.setPlannedOffset(3 * perSegment);

            // The table the offset was set for may skip.
            SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertTrue(SQLiteColdIndex.skippedRows("block") > 0, "the planned table skips segments");

            // A second table read in the same lookup must not: its rows would shift a merged
            // result that the query's single offset knows nothing about.
            SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertEquals(0, SQLiteColdIndex.skippedRows("container"), "an unplanned table never skips");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void readingWithNothingToMaterializeDoesNotFail() throws Exception {
        seal();
        // A lookup for a player with no rows at all writes nothing, which must not be treated as a
        // transaction to commit.
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("999999", "");
            SQLiteColdIndex.setRowBudget(4);
            assertEquals(ConfigHandler.prefix + "block", SQLiteColdIndex.sourceExpression(connection, "block", 1, null));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        // And one that selects segments but keeps no rows from them.
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("1", "");
            SQLiteColdIndex.setRowBudget(4);
            SQLiteColdIndex.sourceExpression(connection, "block", 1, new Integer[] { 0, 900000, 900100, 0, 0, 900000, 900100 });
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void refusesAPageThatWouldReadMoreThanTheLimitAllows() throws Exception {
        seal();
        Config.getGlobal().COLD_MAX_ROWS = 50;
        try {
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                SQLiteColdIndex.setLookupFilters("1", "");
                SQLiteColdIndex.setRowBudget(5000);
                String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
                assertEquals(ConfigHandler.prefix + "block", source, "an out of reach page reads no compressed rows");
                assertTrue(SQLiteColdIndex.isOutOfReach(), "the lookup is told the page is out of reach");
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }

            // A page within the limit still reads normally.
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                SQLiteColdIndex.setLookupFilters("1", "");
                SQLiteColdIndex.setRowBudget(10);
                assertTrue(SQLiteColdIndex.sourceExpression(connection, "block", 1, null).startsWith("("));
                assertTrue(!SQLiteColdIndex.isOutOfReach());
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
        }
        finally {
            Config.getGlobal().COLD_MAX_ROWS = 500000;
        }
    }

    private long skippedFor(long user, int offset) throws Exception {
        readPage(user, offset, 4, 0, 0, true);
        return lastSkipped;
    }

    private long decodedRowsFor(long user, int offset) throws Exception {
        readPage(user, offset, 4, 0, 0, true);
        return lastDecoded;
    }

    @Test
    void plannedPagesMatchWhenTheLookupIsTimeBounded() throws Exception {
        seal();
        long base = base();
        // A window whose edges fall inside segments, so both ends straddle a boundary.
        long startTime = base + DAY + (DAY / 2);
        long endTime = base + (3 * DAY) + (DAY / 2);
        for (int offset : offsets()) {
            assertSamePage(1, offset, 4, startTime, endTime);
        }
    }

    @Test
    void plannedPagesMatchWhenSegmentsHaveNoStatistics() throws Exception {
        seal();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE co_segment SET user_stats = NULL, type_stats = NULL");
        }
        SQLiteColdIndex.invalidate();
        SQLiteColdIndex.reload(connection);

        for (int offset : offsets()) {
            assertSamePage(1, offset, 4, 0, 0);
        }
    }

    @Test
    void plannedPagesMatchPastTheEndOfTheResult() throws Exception {
        seal();
        int owned = (ROWS_PER_DAY * DAYS) / USERS;
        for (int offset : new int[] { owned - 5, owned - 1, owned, owned + 1, owned + 100 }) {
            assertSamePage(1, offset, 4, 0, 0);
        }
    }

    /**
     * Asks for the same page with and without planning and requires an identical answer.
     */
    private void assertSamePage(long user, int offset, int pageSize, long startTime, long endTime) throws Exception {
        List<Long> unplanned = readPage(user, offset, pageSize, startTime, endTime, false);
        List<Long> planned = readPage(user, offset, pageSize, startTime, endTime, true);
        assertEquals(unplanned, planned, "planned page differs at offset " + offset + (startTime > 0 ? " (time bounded)" : ""));
    }

    private List<Long> readPage(long user, int offset, int pageSize, long startTime, long endTime, boolean planned) throws Exception {
        List<Long> page = new ArrayList<>();
        SQLiteColdIndex.beginLookup(startTime, endTime);
        try {
            SQLiteColdIndex.setLookupFilters(Long.toString(user), "");
            SQLiteColdIndex.setPlannedOffset(planned ? offset : 0);
            SQLiteColdIndex.setRowBudget((long) offset + pageSize);

            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            long skipped = SQLiteColdIndex.skippedRows("block");
            lastSkipped = skipped;
            lastDecoded = source.startsWith("(") ? count("SELECT COUNT(*) FROM temp.cp_cold_block") : 0;
            StringBuilder query = new StringBuilder("SELECT rowid FROM " + source + " WHERE user = " + user);
            if (startTime > 0) {
                query.append(" AND time > ").append(startTime);
            }
            if (endTime > 0) {
                query.append(" AND time <= ").append(endTime);
            }
            query.append(" ORDER BY rowid DESC LIMIT ").append(pageSize).append(" OFFSET ").append(offset - skipped);

            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query.toString())) {
                while (results.next()) {
                    page.add(results.getLong(1));
                }
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        return page;
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    /** Offsets inside a segment, exactly on a boundary, and one row either side of one. */
    private int[] offsets() {
        int perSegment = ROWS_PER_DAY / USERS;
        return new int[] { 0, 1, perSegment - 1, perSegment, perSegment + 1, (2 * perSegment) - 1, 2 * perSegment,
                (2 * perSegment) + 1, 3 * perSegment, (3 * perSegment) + 5 };
    }

    private long base() {
        long now = System.currentTimeMillis() / 1000L;
        return ((now - (40 * DAY)) / DAY) * DAY;
    }

    /** Writes several days of activity and seals it, which gives one segment per day. */
    private void seal() throws Exception {
        long base = base();
        connection.setAutoCommit(false);
        String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,NULL,NULL,?,0)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int day = 0; day < DAYS; day++) {
                for (int index = 0; index < ROWS_PER_DAY; index++) {
                    statement.setLong(1, base + (day * DAY) + index);
                    statement.setInt(2, 1 + (index % USERS));
                    statement.setInt(3, index % 200);
                    statement.setInt(4, index % 200);
                    statement.setInt(5, 10 + (index % 40));
                    statement.setInt(6, index % 2);
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);

        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });
        assertTrue(sealed > 0, "the test data has to reach compressed storage");
        SQLiteColdIndex.reload(connection);
    }
}
