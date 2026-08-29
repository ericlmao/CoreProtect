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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * The gate for counting rows from segment metadata instead of by reading them.
 *
 * <p>
 * Each segment records how many rows it holds for each player and block type, so a count can often
 * be answered by adding those numbers up. That is only allowed where it gives the same answer as
 * reading every row would, so every test here counts both ways and requires them to agree. The
 * filters are chosen to cover the cases where metadata can answer, the cases where it cannot
 * because the query cuts through a segment, and the boundary between them.
 * </p>
 */
class ColdCountTest {

    private static final long DAY = 86400L;
    private static final int ROWS_PER_DAY = 500;
    private static final int DAYS = 4;
    private static final int USERS = 10;
    private static final int TYPES = 5;

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
    void countsFromMetadataMatchCountsFromReadingForAUser() throws Exception {
        seal();
        for (long user = 1; user <= USERS; user++) {
            assertSameCount(Long.toString(user), "", 0, 0);
        }
        assertSameCount("999999", "", 0, 0);
    }

    @Test
    void countsFromMetadataMatchWhenTheWindowCoversWholeSegments() throws Exception {
        seal();
        long base = base();
        // A window aligned to day boundaries covers each segment completely.
        assertSameCount("1", "", base, base + (DAYS * DAY));
        assertSameCount("1", "", base + DAY, base + (3 * DAY));
    }

    @Test
    void countsFromMetadataMatchWhenTheWindowCutsThroughSegments() throws Exception {
        seal();
        long base = base();
        // Edges inside segments, where the recorded counts describe more rows than the query wants.
        assertSameCount("1", "", base + (DAY / 2), base + (2 * DAY) + (DAY / 3));
        assertSameCount("", "", base + 10, base + (DAYS * DAY) - 10);
    }

    @Test
    void countsFromMetadataMatchForBlockTypesAndCombinedFilters() throws Exception {
        seal();
        assertSameCount("", "10", 0, 0);
        assertSameCount("", "10,11", 0, 0);
        // A user and a type together cannot be answered by adding two separate sets of counts, so
        // this has to fall back to reading, and must still agree.
        assertSameCount("1", "10", 0, 0);
    }

    @Test
    void countsFromMetadataMatchWithoutStatistics() throws Exception {
        seal();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE co_segment SET user_stats = NULL, type_stats = NULL");
        }
        SQLiteColdIndex.invalidate();
        SQLiteColdIndex.reload(connection);

        assertSameCount("1", "", 0, 0);
        assertSameCount("", "10", 0, 0);
    }

    @Test
    void countingAUserNoLongerReadsTheSegments() throws Exception {
        seal();
        long readBefore = ColdSegmentReadCounter.reads();
        countFromIndex("1", "", 0, 0);
        long readDuring = ColdSegmentReadCounter.reads() - readBefore;
        assertEquals(0, readDuring, "counting a player is answered from the recorded counts alone");

        // A window starting part way through a segment has to read that one segment. Each day's
        // rows span the first ROWS_PER_DAY seconds of the day, so this lands inside the first one.
        long boundaryBefore = ColdSegmentReadCounter.reads();
        countFromIndex("1", "", base() + (ROWS_PER_DAY / 2), 0);
        assertEquals(1, ColdSegmentReadCounter.reads() - boundaryBefore, "only the partly covered segment is read");
    }

    /**
     * Counts a filter through the index and by reading every row, and requires the same answer.
     */
    private void assertSameCount(String users, String types, long startTime, long endTime) throws Exception {
        long fromIndex = countFromIndex(users, types, startTime, endTime);
        long fromRows = countByReading(users, types, startTime, endTime);
        assertEquals(fromRows, fromIndex, "counting users=[" + users + "] types=[" + types + "] window=" + startTime + ".." + endTime);
    }

    private long countFromIndex(String users, String types, long startTime, long endTime) throws Exception {
        SQLiteColdIndex.beginLookup(startTime, endTime);
        try {
            SQLiteColdIndex.setLookupFilters(users, types);
            SQLiteColdIndex.setCounting(true);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            // What a lookup actually reports: the rows the segments counted, plus the live rows
            // counted by the query. Sealing leaves the newest row behind, so leaving the live side out
            // would compare the segments alone against a reading of both.
            return countRows(source, users, types, startTime, endTime) + SQLiteColdIndex.countedRows("block");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    /** Counts the rows of a source that match the same restrictions. */
    private long countRows(String source, String users, String types, long startTime, long endTime) throws Exception {
        StringBuilder query = new StringBuilder("SELECT COUNT(*) FROM " + source + " WHERE 1");
        if (!users.isEmpty()) {
            query.append(" AND user IN(").append(users).append(')');
        }
        if (!types.isEmpty()) {
            query.append(" AND type IN(").append(types).append(')');
        }
        if (startTime > 0) {
            query.append(" AND time > ").append(startTime);
        }
        if (endTime > 0) {
            query.append(" AND time <= ").append(endTime);
        }
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query.toString())) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    /** Counts the same rows the slow way, by reading every segment. */
    private long countByReading(String users, String types, long startTime, long endTime) throws Exception {
        long counted = 0;
        SQLiteColdIndex.beginLookup(startTime, endTime);
        try {
            SQLiteColdIndex.setLookupFilters(users, types);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            StringBuilder query = new StringBuilder("SELECT COUNT(*) FROM " + source + " WHERE 1");
            if (!users.isEmpty()) {
                query.append(" AND user IN(").append(users).append(')');
            }
            if (!types.isEmpty()) {
                query.append(" AND type IN(").append(types).append(')');
            }
            if (startTime > 0) {
                query.append(" AND time > ").append(startTime);
            }
            if (endTime > 0) {
                query.append(" AND time <= ").append(endTime);
            }
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query.toString())) {
                counted = results.next() ? results.getLong(1) : 0;
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        return counted;
    }

    private long base() {
        long now = System.currentTimeMillis() / 1000L;
        return ((now - (40 * DAY)) / DAY) * DAY;
    }

    private void seal() throws Exception {
        long base = base();
        connection.setAutoCommit(false);
        String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,NULL,NULL,?,0)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int day = 0; day < DAYS; day++) {
                for (int index = 0; index < ROWS_PER_DAY; index++) {
                    statement.setLong(1, base + (day * DAY) + index);
                    statement.setInt(2, 1 + (index % USERS));
                    statement.setInt(3, index % 100);
                    statement.setInt(4, index % 100);
                    statement.setInt(5, 10 + (index % TYPES));
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
