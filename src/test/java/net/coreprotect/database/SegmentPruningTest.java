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
 * The gate for a segment never denying that it holds something it does.
 *
 * <p>
 * Segments record which players, block types, entities and chunks they hold rows for, and lookups
 * use those records to skip segments they need not read. A record that overstates costs a wasted
 * read. A record that understates loses rows from the answer with nothing to show that it happened,
 * which is the failure these tests exist to catch.
 * </p>
 *
 * <p>
 * The way to get that wrong is to stop collecting values while reading the rows, at the point where
 * there are too many to list exactly. What is stored is decided later, and a filter built from only
 * the values seen before the cut off denies every value after it.
 * </p>
 */
class SegmentPruningTest {

    private static final long DAY = 86400L;

    /**
     * More distinct players than a segment keeps counts for, which matters: while the counts are
     * there they answer on their own and the record of which players a segment holds is never
     * consulted. Only past that point does a wrong record start losing rows.
     */
    private static final int USERS = SegmentStatistics.MAXIMUM_VALUES + 500;

    private static final int ROWS = USERS + 1000;

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
    void everyPlayerInASegmentIsStillFoundWhenThereAreTooManyToList() throws Exception {
        seal();

        // The players recorded last are the ones a truncated record forgets, so they are what is
        // asked for. Every one of them has rows, and every one has to come back.
        for (int user = USERS - 5; user <= USERS; user++) {
            assertEquals(rowsFor(user), coldRowsFor(user), "player " + user + " must not be pruned away");
        }
    }

    @Test
    void everyChunkInASegmentIsStillFound() throws Exception {
        seal();

        // Each row of the test data sits in a chunk of its own, so the segment covers far more chunks
        // than are listed exactly and the record has to become a filter.
        for (int row = ROWS - 5; row < ROWS; row++) {
            int x = row * 16;
            int z = row * 16;
            assertTrue(coldRowsInChunk(x, z) > 0, "the chunk at " + x + "," + z + " must not be pruned away");
        }
    }

    /** How many rows the test data holds for a player. */
    private long rowsFor(int user) {
        long rows = 0;
        for (int row = 0; row < ROWS; row++) {
            if ((row % USERS) + 1 == user) {
                rows++;
            }
        }
        return rows;
    }

    /** How many the segments give back for that player. */
    private long coldRowsFor(int user) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setPredicateFilters("user IN(" + user + ")", "user");
            String table = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + table + " WHERE user = " + user)) {
                assertTrue(results.next());
                return results.getLong(1);
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private long coldRowsInChunk(int x, int z) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String table = SQLiteColdIndex.sourceExpression(connection, "block", 1, new Integer[] { 0, x, x, 0, 0, z, z });
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + table + " WHERE wid = 1 AND x = " + x + " AND z = " + z)) {
                assertTrue(results.next());
                return results.getLong(1);
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private void seal() throws Exception {
        long base = (((System.currentTimeMillis() / 1000L) - (30 * DAY)) / DAY) * DAY;
        connection.setAutoCommit(false);
        String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,10,0,NULL,NULL,0,0)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int row = 0; row < ROWS; row++) {
                statement.setLong(1, base + row);
                statement.setInt(2, (row % USERS) + 1);
                // A chunk of its own for every row, so the segment covers thousands of them.
                statement.setInt(3, row * 16);
                statement.setInt(4, row * 16);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);

        assertTrue(ColdRollupTask.rollUp(connection, () -> {
        }) > 0, "the rows have to reach compressed storage");
        SQLiteColdIndex.reload(connection);
    }
}
