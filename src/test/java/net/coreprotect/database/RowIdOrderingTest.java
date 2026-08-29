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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * The gate for a segment's row ids always sitting below the live table's.
 *
 * <p>
 * Everything about reading compressed storage rests on this. Rows are ordered newest first by row id
 * across the live table and the segments together, and a page of results is resumed by row id, so two
 * rows sharing a number is not a slow answer but a wrong one.
 * </p>
 *
 * <p>
 * The way to lose it is to seal every row a table has. SQLite numbers a new row from the highest one
 * still in the table, and knows nothing of the rows that were taken away, so an emptied table starts
 * again at one, on top of numbers the segments already hold. Those rows can then never be sealed
 * either, because sealing only looks above the highest row id already sealed.
 * </p>
 */
class RowIdOrderingTest {

    private static final long DAY = 86400L;
    private static final int ROWS = 500;

    private Connection connection;
    private DatabaseType previousType;

    @BeforeAll
    static void prepareServer() {
        // The repair says what it moved, and saying anything needs a server to say it to.
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = org.mockito.Mockito.mock(org.bukkit.Server.class);
            org.mockito.Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
    }

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
    void sealingLeavesTheTableAbleToKeepNumbering() throws Exception {
        write(ROWS);
        sealEverything();

        // One row is left behind on purpose. Without it the next row written starts again at one.
        long live = count();
        assertEquals(1, live, "the newest row stays so the numbering has something to count from");

        write(1);
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT MAX(rowid) FROM co_block")) {
            assertTrue(results.next());
            assertEquals(ROWS + 1, results.getLong(1), "the row written afterwards carries on from where the table left off");
        }
    }

    @Test
    void aRowWrittenAfterSealingIsNeverGivenARowIdASegmentAlreadyHolds() throws Exception {
        write(ROWS);
        sealEverything();
        write(20);

        long coldHigh = SQLiteColdIndex.coldHighWaterMark("block");
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT MIN(rowid) FROM co_block WHERE rowid <= " + coldHigh)) {
            assertTrue(results.next());
            results.getLong(1);
            assertTrue(results.wasNull(), "no live row shares a number with a sealed one");
        }
    }

    @Test
    void rowsLeftUnderneathTheSegmentsAreMovedBackAboveThem() throws Exception {
        write(ROWS);
        sealEverything();

        // What an earlier build left behind: the table emptied, then written to again from one.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_block");
        }
        write(3);
        long coldHigh = SQLiteColdIndex.coldHighWaterMark("block");
        assertTrue(lowestRowId() <= coldHigh, "the rows really are underneath the segments");

        assertEquals(3, ColdRollupTask.repairRowIds(connection, () -> {
        }), "every row underneath is moved");

        assertTrue(lowestRowId() > coldHigh, "and now sits above them");
        assertEquals(3, count(), "with none of them lost");
    }

    @Test
    void theRepairFindsTheSegmentsWithoutBeingToldAboutThemFirst() throws Exception {
        write(ROWS);
        sealEverything();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_block");
        }
        write(4);

        // What the repair knows about the segments comes from an index that is empty until it has
        // been read from the database. On a server it runs before anything else has cause to read it,
        // so forgetting that leaves it seeing no segments and doing nothing at all.
        SQLiteColdIndex.invalidate();

        assertEquals(4, ColdRollupTask.repairRowIds(connection, () -> {
        }), "the repair reads the segments for itself");
    }

    @Test
    void movedRowsCanThenBeSealedLikeAnyOther() throws Exception {
        write(ROWS);
        sealEverything();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_block");
        }
        write(200);

        // Before the repair these rows are invisible to sealing, which only looks above the highest
        // row id already sealed. That is the second half of the damage and the reason it accumulates.
        ColdRollupTask.repairRowIds(connection, () -> {
        });
        long sealed = ColdRollupTask.rollUp(connection, () -> {
        }, (System.currentTimeMillis() / 1000L) + 1);

        assertTrue(sealed > 0, "the moved rows reach compressed storage");
        assertEquals(1, count(), "leaving only the newest behind");
    }

    private long lowestRowId() throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(MIN(rowid),0) FROM co_block")) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    private long count() throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_block")) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    private void write(int rows) throws SQLException {
        long base = (((System.currentTimeMillis() / 1000L) - (30 * DAY)) / DAY) * DAY;
        connection.setAutoCommit(false);
        String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,1,1,?,64,?,10,0,NULL,NULL,0,0)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int row = 0; row < rows; row++) {
                statement.setLong(1, base + row);
                statement.setInt(2, row % 40);
                statement.setInt(3, row % 40);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void sealEverything() throws Exception {
        assertTrue(ColdRollupTask.rollUp(connection, () -> {
        }, (System.currentTimeMillis() / 1000L) + 1) > 0, "the rows reach compressed storage");
        SQLiteColdIndex.reload(connection);
    }
}
