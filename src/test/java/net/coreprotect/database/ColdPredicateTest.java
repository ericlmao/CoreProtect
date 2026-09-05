package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
 * The gate for the compressed reader applying the same restrictions the query does.
 *
 * <p>
 * A count is answered without handing any compressed rows to the query, so the reader has to apply
 * the query's restrictions itself. It takes them from the assembled clause rather than from the
 * request, because the same request restricts different tables differently. These tests take a
 * clause, count it through the reader, and require the same answer the clause gives when it is run
 * as ordinary SQL over the same rows.
 * </p>
 */
class ColdPredicateTest {

    private static final long DAY = 86400L;
    private static final int ROWS_PER_DAY = 400;
    private static final int DAYS = 3;
    private static final int USERS = 8;
    private static final int TYPES = 5;
    private static final int ACTIONS = 4;

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
    void readsRestrictionsOutOfAClause() {
        assertEquals("1,2,3", SQLiteColdIndex.listedValues("user IN(1,2,3) AND time > 5", "user", false));
        assertEquals("4,5", SQLiteColdIndex.listedValues("time > 5 AND action NOT IN(4,5)", "action", true));
        assertNull(SQLiteColdIndex.listedValues("action NOT IN(4,5)", "action", false), "a NOT IN list is not an IN list");
        assertNull(SQLiteColdIndex.listedValues("user IN(1) AND time > 5", "action", false), "a column that is not restricted");
        assertNull(SQLiteColdIndex.listedValues("rolled_back IN(1)", "back", false), "part of a longer column name");
        assertNull(SQLiteColdIndex.listedValues("type IN(1) AND type IN(2)", "type", false), "one column restricted twice");
        assertEquals("3", SQLiteColdIndex.listedValues("(action=3 AND type NOT IN(1,2)) AND time > 5", "action", false), "a single action by equality");
        assertNull(SQLiteColdIndex.listedValues("action!=13 AND time > 5", "action", false), "an inequality restricts nothing");
        assertNull(SQLiteColdIndex.listedValues("action=3 AND action IN(1,2)", "action", false), "equality and a list together");
    }

    @Test
    void countsMatchTheClauseForEveryRestriction() throws Exception {
        seal();
        assertCountMatchesClause(" user IN(1,2)");
        assertCountMatchesClause(" user NOT IN(1,2)");
        assertCountMatchesClause(" type IN(10,11)");
        assertCountMatchesClause(" type NOT IN(10,11)");
        assertCountMatchesClause(" action IN(1)");
        assertCountMatchesClause(" action NOT IN(1,2)");
        assertCountMatchesClause(" action NOT IN(-1)");
        assertCountMatchesClause(" user IN(1,2) AND user NOT IN(2)");
        assertCountMatchesClause(" user IN(1,2) AND action NOT IN(1)");
        assertCountMatchesClause(" 1");
    }

    @Test
    void aClauseWithAlternativesIsNotTreatedAsRequired() throws Exception {
        seal();
        // Nothing in an OR is guaranteed to hold, so the reader must not use any of it to skip
        // rows, and must hand the rows to the query rather than counting them itself.
        assertCountMatchesClause(" (user IN(1) OR type IN(10))");
    }

    @Test
    void countsMatchWhenTheLookupIsRestrictedToAnArea() throws Exception {
        seal();
        // Height is not summarised anywhere, so a count over an area cannot be taken from the
        // recorded counts and has to come from the rows themselves.
        Integer[] bounds = { 1, 0, 40, 0, 60, 0, 40 };
        assertEquals(countWithBounds(" y >= 0 AND y <= 60 AND x >= 0 AND x <= 40 AND z >= 0 AND z <= 40", bounds, false),
                countWithBounds(" y >= 0 AND y <= 60 AND x >= 0 AND x <= 40 AND z >= 0 AND z <= 40", bounds, true),
                "counting an area");
        assertEquals(countWithBounds(" y >= 0 AND y <= 10 AND x >= 0 AND x <= 40 AND z >= 0 AND z <= 40", bounds, false),
                countWithBounds(" y >= 0 AND y <= 10 AND x >= 0 AND x <= 40 AND z >= 0 AND z <= 40", bounds, true),
                "counting an area with a height limit");
    }

    private long countWithBounds(String clause, Integer[] bounds, boolean counting) throws Exception {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setPredicateFilters(clause, "user");
            if (counting) {
                SQLiteColdIndex.setCounting(true);
            }
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, bounds);
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + source + " WHERE" + clause)) {
                long inQuery = results.next() ? results.getLong(1) : 0;
                return inQuery + SQLiteColdIndex.countedRows("block");
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void countingNeverBuildsATemporaryTable() throws Exception {
        seal();
        // The crash this guards against was a count copying every compressed row into scratch
        // space until the disk filled up.
        countThroughReader(" user NOT IN(1)");
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM temp.sqlite_master WHERE name = 'cp_cold_block'")) {
            assertTrue(results.next());
            assertEquals(0, results.getInt(1), "counting must not copy compressed rows anywhere");
        }
    }

    @Test
    void aFailedReadFallsBackToLiveRowsInsteadOfFailing() throws Exception {
        seal();
        // Nothing can be written, so copying the rows into scratch space cannot succeed.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA query_only = true");
        }

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setPredicateFilters(" 1", "user");
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            assertFalse(source.contains("cp_cold_block"), "a read that cannot be done falls back to live rows");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA query_only = false");
            }
        }
    }

    /**
     * Counts a clause through the reader and by running it as SQL over every row, and requires the
     * same answer.
     */
    private void assertCountMatchesClause(String clause) throws Exception {
        long throughReader = countThroughReader(clause);
        long throughSql = countThroughSql(clause);
        assertEquals(throughSql, throughReader, "counting rows matching[" + clause + "]");
    }

    private long countThroughReader(String clause) throws Exception {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setPredicateFilters(clause, "user");
            SQLiteColdIndex.setCounting(true);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            // This is what a lookup does: whatever the reader counted for itself is added to what
            // the query counts over the rows it was given.
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + source + " WHERE" + clause)) {
                long inQuery = results.next() ? results.getLong(1) : 0;
                return inQuery + SQLiteColdIndex.countedRows("block");
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    /** Counts the same clause the slow way, over rows the reader has built. */
    private long countThroughSql(String clause) throws Exception {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setPredicateFilters(clause, "user");
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + source + " WHERE" + clause)) {
                return results.next() ? results.getLong(1) : 0;
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private void seal() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long base = ((now - (40 * DAY)) / DAY) * DAY;
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
                    statement.setInt(6, index % ACTIONS);
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
