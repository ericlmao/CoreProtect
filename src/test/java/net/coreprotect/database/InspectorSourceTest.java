package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import net.coreprotect.database.lookup.InspectorSource;

/**
 * The gate for the inspector seeing a block's whole history.
 *
 * <p>
 * The inspector used to read the live tables alone. Once activity was packed into compressed storage
 * it disappeared from the inspector, so clicking a block that had been broken and replaced months ago
 * reported that nothing had ever happened to it. That is a worse failure than being slow, because it
 * looks like an answer.
 * </p>
 *
 * <p>
 * What makes it affordable is that the coordinates narrow it down: only the segments whose chunks
 * could hold the block are opened. These tests check both halves, that the packed rows are found and
 * that finding them does not mean reading segments belonging to somewhere else.
 * </p>
 */
class InspectorSourceTest {

    private static final long DAY = 86400L;
    private static final int WORLD = 1;
    private static final int X = 120;
    private static final int Y = 64;
    private static final int Z = -340;

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
            // The inspector hints this index when it reads the live table on its own.
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS block_index ON co_block(wid,x,z,time);");
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
    void theInspectorFindsRowsThatOnlyExistInCompressedStorage() throws Exception {
        write(600, X, Z);
        seal();

        // Everything has been packed away, which is exactly when the inspector used to go blind.
        assertEquals(0, countLive(), "the live table no longer holds the rows");

        try (InspectorSource source = InspectorSource.open(connection, "block", WORLD, X, X, Z, Z, 0)) {
            assertTrue(source.table().startsWith("("), "the source reads from the live rows and the segments together");
            assertTrue(source.index().isEmpty(), "an index on the live table cannot be hinted for an expression");
            assertEquals(600, countThrough(source), "the packed rows are found");
        }
    }

    @Test
    void whatItFindsIsTheSameAsWhatWasWritten() throws Exception {
        write(120, X, Z);
        seal();

        try (InspectorSource source = InspectorSource.open(connection, "block", WORLD, X, X, Z, Z, 0)) {
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT time,user,type FROM " + source.table()
                            + " WHERE wid = " + WORLD + " AND x = " + X + " AND z = " + Z + " AND y = " + Y + " ORDER BY rowid")) {
                int seen = 0;
                while (results.next()) {
                    seen++;
                    assertEquals(base() + seen, results.getLong("time"), "row " + seen + " keeps its time");
                    assertEquals(1 + (seen % 7), results.getInt("user"), "row " + seen + " keeps its player");
                }
                assertEquals(120, seen, "every row came back");
            }
        }
    }

    @Test
    void aBlockSomewhereElseIsNotDraggedIn() throws Exception {
        write(200, X, Z);
        write(200, X + 5000, Z + 5000);
        seal();

        try (InspectorSource source = InspectorSource.open(connection, "block", WORLD, X, X, Z, Z, 0)) {
            assertEquals(200, countThrough(source), "only the rows at these coordinates are read");
        }
    }

    @Test
    void closingItLeavesNothingBehind() throws Exception {
        write(300, X, Z);
        seal();

        InspectorSource source = InspectorSource.open(connection, "block", WORLD, X, X, Z, Z, 0);
        assertTrue(countThrough(source) > 0);
        source.close();

        // A source left open would hold its scratch table for as long as the connection lived, and
        // the inspector opens one on every click.
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT name FROM temp.sqlite_master WHERE type='table' AND name LIKE 'cp_cold_%'")) {
            assertFalse(results.next(), "the scratch table is gone");
        }
    }

    @Test
    void aDatabaseWithNothingPackedAwayStillReadsTheLiveTable() throws Exception {
        write(50, X, Z);

        try (InspectorSource source = InspectorSource.open(connection, "block", WORLD, X, X, Z, Z, 0)) {
            assertEquals(ConfigHandler.prefix + "block", source.table(), "with no segments there is nothing to union with");
            assertFalse(source.index().isEmpty(), "and the index on the live table is still worth hinting");
            assertEquals(50, countThrough(source));
        }
    }

    private long countThrough(InspectorSource source) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + source.table() + " " + source.index()
                + "WHERE wid = " + WORLD + " AND x = " + X + " AND z = " + Z + " AND y = " + Y;
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    private long countLive() throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_block")) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    private void write(int rows, int x, int z) throws SQLException {
        connection.setAutoCommit(false);
        String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?," + WORLD + ",?," + Y + ",?,?,0,NULL,NULL,?,0)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int row = 1; row <= rows; row++) {
                statement.setLong(1, base() + row);
                statement.setInt(2, 1 + (row % 7));
                statement.setInt(3, x);
                statement.setInt(4, z);
                statement.setInt(5, 10 + (row % 3));
                statement.setInt(6, row % 2);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void seal() throws Exception {
        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });
        assertTrue(sealed > 0, "the rows have to reach compressed storage for this to mean anything");
        SQLiteColdIndex.reload(connection);
    }

    private static long base() {
        long now = System.currentTimeMillis() / 1000L;
        return ((now - (30 * DAY)) / DAY) * DAY;
    }
}
