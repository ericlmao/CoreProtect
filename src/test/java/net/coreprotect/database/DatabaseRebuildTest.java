package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;

/**
 * The gate for writing the file out afresh instead of handing pages back one at a time.
 *
 * <p>
 * Compacting leaves a file that is almost entirely free space, and returning that space page by page
 * means moving every page that still holds data out of the end of the file first. On a real server's
 * seventy five gigabyte file that came to days of work. Writing the data out to a new file costs one
 * pass over what is actually there.
 * </p>
 *
 * <p>
 * What makes it safe is that row ids survive it. Compressed storage is addressed by row id from end
 * to end: sealed rows sit below live ones, pages of results resume by row id, and rows in one table
 * point at rows in another by number. The tests here hold that to account, including the one that
 * says why plain <code>VACUUM</code> cannot be used for this.
 * </p>
 */
class DatabaseRebuildTest {

    private static final int ROWS = 4000;

    /** A file this small would not be worth rebuilding on a server, so the test says what to accept. */
    private static final long SMALL_ENOUGH = 64 * 1024;

    private Path database;
    private DatabaseType previousType;
    private String previousPath;
    private String previousFile;

    @BeforeAll
    static void prepareServer() {
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = org.mockito.Mockito.mock(org.bukkit.Server.class);
            org.mockito.Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
    }

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        previousPath = ConfigHandler.path;
        previousFile = ConfigHandler.sqlite;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.path = directory.toString() + java.io.File.separator;
        ConfigHandler.sqlite = "database.db";
        Config.getGlobal().COMPACT_REBUILD = true;
        SQLiteColdIndex.invalidate();

        database = directory.resolve("database.db");
        try (Connection connection = open()) {
            try (Statement statement = connection.createStatement()) {
                SQLiteSchema.applyFileSettings(statement);
                statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
                statement.executeUpdate("CREATE TABLE co_entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
                statement.executeUpdate("CREATE TABLE co_entity_spawn (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB, block_rowid INTEGER);");
                SQLiteSchema.createTables("co_", statement);
            }
            fill(connection);
        }
    }

    @AfterEach
    void restoreSettings() {
        ConfigHandler.databaseType = previousType;
        ConfigHandler.path = previousPath;
        ConfigHandler.sqlite = previousFile;
        SQLiteColdIndex.invalidate();
    }

    @Test
    void everyRowKeepsTheRowIdItHad() throws Exception {
        List<Long> blockRowIds;
        List<Long> entityRowIds;
        try (Connection connection = open()) {
            blockRowIds = rowIds(connection, "co_block");
            entityRowIds = rowIds(connection, "co_entity");
        }

        long before = Files.size(database);
        long saved = DatabaseRebuild.rebuild(database, SMALL_ENOUGH);
        long after = Files.size(database);

        assertTrue(saved > 0, "the file shrank");
        assertTrue(after < before / 2, "and by most of what was free: " + before + " -> " + after);

        try (Connection connection = open()) {
            assertEquals(blockRowIds, rowIds(connection, "co_block"), "block row ids are untouched");
            assertEquals(entityRowIds, rowIds(connection, "co_entity"), "entity row ids are untouched");
            assertTrue(blockRowIds.size() > 0 && entityRowIds.size() > 0, "there were rows to keep");
            assertEquals(0, pragma(connection, "PRAGMA freelist_count"), "and nothing is left free in the new file");
        }
    }

    @Test
    void rowsThatPointAtOtherRowsByNumberStillFindThem() throws Exception {
        DatabaseRebuild.rebuild(database, SMALL_ENOUGH);

        try (Connection connection = open();
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(
                        "SELECT COUNT(*) FROM co_entity_spawn s JOIN co_block b ON b.rowid = s.block_rowid")) {
            assertTrue(results.next());
            assertEquals(ROWS / 4, results.getLong(1), "every spawn still resolves to the block row it names");
        }
    }

    @Test
    void plainVacuumIsNotUsedBecauseItRenumbersRows() throws Exception {
        // The reason the rebuild is a copy rather than a VACUUM. The activity tables have no explicit
        // integer primary key, and VACUUM renumbers such tables from one, which would drop live rows
        // on top of row ids the segments already hold and break every reference between tables.
        try (Connection connection = open()) {
            List<Long> before = rowIds(connection, "co_block");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("VACUUM");
            }
            List<Long> after = rowIds(connection, "co_block");
            assertNotEquals(before, after, "VACUUM renumbers, which is why the rebuild does not use it");
            assertEquals(1L, after.get(0), "it starts again from one");
        }
    }

    @Test
    void aFileThatIsMostlyDataIsLeftAlone() throws Exception {
        // Nothing has been deleted, so there is no free space worth the cost of a rebuild.
        try (Connection connection = open()) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("VACUUM");
            }
        }
        long before = Files.size(database);
        assertEquals(0, DatabaseRebuild.rebuild(database, SMALL_ENOUGH), "a full file is left as it is");
        assertEquals(before, Files.size(database), "and is not rewritten");
    }

    @Test
    void aCopyThatDoesNotMatchIsRefusedAndTheOriginalIsKept() throws Exception {
        // What the check is for. A copy that lost rows, or renumbered them, must never replace the
        // file it was made from.
        Path working = database.getParent().resolve("copy.db");
        try (Connection connection = open()) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("VACUUM INTO '" + working + "'");
            }
        }
        try (Connection copy = DriverManager.getConnection("jdbc:sqlite:" + working); Statement statement = copy.createStatement()) {
            statement.executeUpdate("DELETE FROM co_block WHERE rowid % 3 = 0");
        }

        try (Connection connection = open()) {
            SQLException refused = assertThrows(SQLException.class, () -> DatabaseRebuild.check(connection, working));
            assertTrue(refused.getMessage().contains("co_block"), refused.getMessage());
        }
        assertTrue(Files.isRegularFile(database), "the original is still there");
    }

    @Test
    void aRebuiltFileStillReadsItsCompressedStorage() throws Exception {
        // A smaller file that cannot be read back is worthless, so this seals rows into segments
        // first and reads them afterwards.
        try (Connection connection = open()) {
            ColdRollupTask.rollUp(connection, () -> {
            }, (System.currentTimeMillis() / 1000L) + 1);
            SQLiteColdIndex.reload(connection);
        }

        DatabaseRebuild.rebuild(database, SMALL_ENOUGH);

        try (Connection connection = open()) {
            SQLiteColdIndex.invalidate();
            SQLiteColdIndex.reload(connection);
            assertTrue(pragma(connection, "SELECT COUNT(*) FROM co_segment") > 0, "the segments came across");
            assertEquals(0, ColdRollupTask.repairRowIds(connection, () -> {
            }), "and no live row ended up underneath them");
        }
    }

    private void fill(Connection connection) throws SQLException {
        long base = (((System.currentTimeMillis() / 1000L) - (30 * 86400L)) / 86400L) * 86400L;
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,?,NULL,0,0)")) {
            byte[] filler = new byte[600];
            for (int row = 0; row < ROWS; row++) {
                statement.setLong(1, base + row);
                statement.setInt(2, row % 50);
                statement.setInt(3, row % 400);
                statement.setInt(4, row % 300);
                statement.setInt(5, row % 120);
                statement.setBytes(6, filler);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity (id,time,data) VALUES (?,?,?)")) {
            byte[] filler = new byte[900];
            for (int row = 1; row <= ROWS; row++) {
                statement.setInt(1, row);
                statement.setLong(2, base + row);
                statement.setBytes(3, filler);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity_spawn (id,time,data,block_rowid) VALUES (?,?,?,?)")) {
            for (int row = 1; row <= ROWS / 4; row++) {
                statement.setInt(1, row);
                statement.setLong(2, base + row);
                statement.setBytes(3, new byte[200]);
                // A block row that survives the deletion below, so the reference is one that resolves.
                statement.setLong(4, (ROWS - 39) + (row % 40));
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);

        // Most of the file becomes free space, as it does after a compact.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_block WHERE rowid <= " + (ROWS - 40));
            statement.executeUpdate("DELETE FROM co_entity WHERE rowid <= " + (ROWS - 40));
            statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
        }
    }

    private List<Long> rowIds(Connection connection, String table) throws SQLException {
        List<Long> rowIds = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT rowid FROM " + table + " ORDER BY rowid")) {
            while (results.next()) {
                rowIds.add(results.getLong(1));
            }
        }
        return rowIds;
    }

    private static long pragma(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private Connection open() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=15000");
            statement.executeUpdate("PRAGMA journal_mode=WAL");
        }
        return connection;
    }
}
