package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import org.bukkit.Bukkit;
import org.bukkit.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.config.ConfigHandler;

/**
 * The gate for importing a database written by upstream CoreProtect.
 *
 * <p>
 * Every test here builds a version 1 database on disk, imports it into a fresh version 2 database,
 * and checks the result against the source. The properties that matter are that no row is lost, that
 * every row keeps the row id the rest of the data refers to it by, that a source written by an older
 * version with fewer columns still imports, and that an interrupted import resumes to the same
 * result as one that ran straight through.
 * </p>
 */
class LegacyImportTest {

    private static final long DAY = 86400L;
    private static final int BLOCK_ROWS = 4000;
    private static final int ITEM_ROWS = 60;
    private static final int SIGN_ROWS = 5;

    private Path directory;
    private Path source;
    private Connection connection;
    private DatabaseType previousType;
    private String previousPath;
    private String previousFile;
    private boolean previousRunning;

    @BeforeAll
    static void prepareServer() {
        if (Bukkit.getServer() == null) {
            Server server = Mockito.mock(Server.class);
            Mockito.when(server.getLogger()).thenReturn(Logger.getLogger("CoreProtectTest"));
            Bukkit.setServer(server);
        }
    }

    @BeforeEach
    void openDatabase(@TempDir Path temporary) throws Exception {
        directory = temporary;
        source = directory.resolve("old.db");

        previousType = ConfigHandler.databaseType;
        previousPath = ConfigHandler.path;
        previousFile = ConfigHandler.sqlite;
        previousRunning = ConfigHandler.serverRunning;

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.path = directory.toString() + File.separator;
        ConfigHandler.sqlite = "database.db";
        ConfigHandler.serverRunning = true;
        ConfigHandler.migrationRunning = false;
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        writeLegacyDatabase();
        createTargetDatabase();
        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        ConfigHandler.path = previousPath;
        ConfigHandler.sqlite = previousFile;
        ConfigHandler.serverRunning = previousRunning;
        ConfigHandler.migrationRunning = false;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void importsEveryRowAndKeepsItsRowId() throws Exception {
        runImport(false);

        assertEquals(BLOCK_ROWS, count("co_block"), "every block row is imported");
        assertEquals(ITEM_ROWS, count("co_item"), "every item row is imported");
        assertEquals(SIGN_ROWS, count("co_sign"), "every sign row is imported");
        assertEquals(3, count("co_user"), "every player is imported");
        assertEquals(2, count("co_material_map"), "every material is imported");

        // The time of a block row is derived from its row id in the test data, so a row that kept
        // its row id is a row that is still where everything else expects to find it.
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT rowid,time FROM co_block ORDER BY rowid")) {
            long expected = 1;
            while (results.next()) {
                assertEquals(expected, results.getLong(1), "row ids are copied unchanged");
                assertEquals(blockTime(expected), results.getLong(2), "row " + expected + " holds its own data");
                expected++;
            }
            assertEquals(BLOCK_ROWS + 1, expected, "the whole table was read back");
        }
    }

    @Test
    void keepsReferencesBetweenTablesIntact() throws Exception {
        runImport(false);

        // An entity spawn names the block row that produced it. If either side's row ids moved, this
        // no longer resolves to the row it was written against.
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(
                        "SELECT s.id, b.time FROM co_entity_spawn s JOIN co_block b ON b.rowid = s.block_rowid ORDER BY s.id")) {
            assertTrue(results.next(), "the first spawn still names a block row");
            assertEquals(1, results.getLong(1));
            assertEquals(blockTime(7), results.getLong(2), "the spawn still points at block row 7");
            assertTrue(results.next(), "the second spawn still names a block row");
            assertEquals(2, results.getLong(1));
            assertEquals(blockTime(9), results.getLong(2), "the spawn still points at block row 9");
            assertFalse(results.next());
        }
    }

    @Test
    void importsASourceWithFewerColumns() throws Exception {
        runImport(false);

        // The legacy sign table predates waxed, face and the secondary colour.
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT line_1,waxed,face FROM co_sign ORDER BY rowid LIMIT 1")) {
            assertTrue(results.next());
            assertEquals("line 1", results.getString(1), "the columns both sides have are copied");
            results.getObject(2);
            assertTrue(results.wasNull(), "a column the source lacks is left at its default");
            results.getObject(3);
            assertTrue(results.wasNull(), "a column the source lacks is left at its default");
        }
    }

    @Test
    void skipsTablesTheSourceDoesNotHave() throws Exception {
        runImport(false);

        // The legacy database has no entity_container table at all.
        assertEquals(0, count("co_entity_container"), "a table the source lacks stays empty");
        assertEquals(0, count("co_chat"), "a table the source lacks stays empty");
    }

    @Test
    void packsImportedRowsIntoSegments() throws Exception {
        runImport(true);

        long cold;
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(row_count),0) FROM co_segment")) {
            assertTrue(results.next());
            cold = results.getLong(1);
        }
        long live = count("co_block") + count("co_item") + count("co_sign");

        assertTrue(cold > 0, "imported rows reach compressed storage");
        assertTrue(live < BLOCK_ROWS + ITEM_ROWS + SIGN_ROWS, "the live tables no longer hold everything");
        assertEquals(BLOCK_ROWS + ITEM_ROWS + SIGN_ROWS, live + cold, "no row is lost between the live tables and the segments");
    }

    @Test
    void leavesRowIdsFreeForRowsWrittenDuringTheImport() throws Exception {
        assertTrue(LegacyImport.importReferenceTables(), "the import starts");

        try (Connection working = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"))) {
            try (PreparedStatement statement = working.prepareStatement("ATTACH DATABASE ? AS legacy")) {
                statement.setString(1, source.toString());
                statement.execute();
            }
            long fence = LegacyImport.placeFence(working, "block");
            assertEquals(BLOCK_ROWS, fence, "the fence sits at the top of the legacy range");
        }

        // A row logged while the import is still running must not be given a row id that a row
        // waiting to be copied already owns.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (1,1,1,0,0,0,10,0,NULL,NULL,0,0)");
        }
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT MAX(rowid) FROM co_block")) {
            assertTrue(results.next());
            assertEquals(BLOCK_ROWS + 1, results.getLong(1), "a live row is numbered above the whole import");
        }

        LegacyImport.runActivityImport(false);
        assertEquals(BLOCK_ROWS + 1, count("co_block"), "the live row and every imported row are both there");
    }

    @Test
    void resumesAnUnfinishedImport() throws Exception {
        assertTrue(LegacyImport.importReferenceTables(), "the import starts");

        // Shutting down stops the import between batches and leaves the rest for the next start.
        ConfigHandler.serverRunning = false;
        LegacyImport.runActivityImport(false);
        assertTrue(count("co_block") < BLOCK_ROWS, "the interrupted import did not finish");
        assertTrue(Files.exists(source), "the source is still there to resume from");

        ConfigHandler.serverRunning = true;
        assertTrue(LegacyImport.importReferenceTables(), "the unfinished import resumes");
        LegacyImport.runActivityImport(false);

        assertEquals(BLOCK_ROWS, count("co_block"), "the resumed import holds the same rows as a clean one");
        assertEquals(ITEM_ROWS, count("co_item"));
        assertFalse(Files.exists(source), "the imported source is set aside");
    }

    @Test
    void refusesASourceThatIsNotTheOneTheImportStarted() throws Exception {
        assertTrue(LegacyImport.importReferenceTables(), "the import starts");
        ConfigHandler.serverRunning = false;
        LegacyImport.runActivityImport(false);
        ConfigHandler.serverRunning = true;

        try (Connection legacy = DriverManager.getConnection("jdbc:sqlite:" + source); Statement statement = legacy.createStatement()) {
            statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (1,1,1,0,0,0,10,0,NULL,NULL,0,0)");
        }

        assertFalse(LegacyImport.importReferenceTables(), "a different source cannot resume the import");
    }

    @Test
    void doesNotCopyTheSameRowsTwice() throws Exception {
        // A start-up that gets as far as the identifier tables and then fails is retried on the next
        // one. What is already in place has to be recognised rather than copied again.
        assertTrue(LegacyImport.importReferenceTables(), "the import starts");
        assertTrue(LegacyImport.importReferenceTables(), "the unfinished import is picked up again");

        assertEquals(3, count("co_user"), "players are not imported twice");
        assertEquals(2, count("co_material_map"), "materials are not imported twice");

        LegacyImport.runActivityImport(false);
        assertEquals(BLOCK_ROWS, count("co_block"), "the activity tables still import in full");
        assertEquals(2, count("co_entity_spawn"), "entity spawns are imported with the activity tables");
    }

    @Test
    void importsWhatItCanFromADamagedSource() throws Exception {
        damageSourcePages();

        assertTrue(LegacyImport.importReferenceTables(), "a damaged source still starts an import");
        LegacyImport.runActivityImport(false);

        long imported = count("co_block");
        assertTrue(LegacyImport.getSkippedRows() > 0, "the unreadable rows are given up rather than read");
        assertTrue(imported > 0, "the readable rows are still imported");
        assertEquals(BLOCK_ROWS, imported + LegacyImport.getSkippedRows(), "every row is either imported or accounted for");

        // Whatever came across has to be the row it says it is; salvaging must not shift anything.
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT rowid,time FROM co_block")) {
            while (results.next()) {
                assertEquals(blockTime(results.getLong(1)), results.getLong(2), "an imported row holds its own data");
            }
        }
    }

    @Test
    void refusesWhenTheTargetAlreadyHoldsData() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (1,1,1,0,0,0,10,0,NULL,NULL,0,0)");
        }

        assertFalse(LegacyImport.importReferenceTables(), "an import into a database with data is refused");
        assertEquals(1, count("co_block"), "nothing was written");
        assertTrue(Files.exists(source), "the source is left where it was");
    }

    @Test
    void refusesASourceInThisLayout() throws Exception {
        Files.delete(source);
        try (Connection legacy = DriverManager.getConnection("jdbc:sqlite:" + source); Statement statement = legacy.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
        }

        assertFalse(LegacyImport.importReferenceTables(), "a database already in this layout is refused");
    }

    @Test
    void setsTheSourceAsideWhenItIsDone() throws Exception {
        runImport(false);

        assertFalse(Files.exists(source), "the imported source is renamed rather than left in place");
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            assertTrue(files.anyMatch(path -> path.getFileName().toString().startsWith("old.db.imported-")),
                    "the imported source is kept, not deleted");
        }
        assertFalse(ConfigHandler.migrationRunning, "the import releases the commands it was blocking");
    }

    /**
     * Overwrites pages in the middle of the source with rubbish, the way a bad disk or a half copied
     * file leaves it: the header and most of the data are intact, and a run of pages is not.
     */
    private void damageSourcePages() throws Exception {
        long size = Files.size(source);
        byte[] rubbish = new byte[4096];
        java.util.Arrays.fill(rubbish, (byte) 0xA5);
        try (java.io.RandomAccessFile file = new java.io.RandomAccessFile(source.toFile(), "rw")) {
            for (long page = size / 2; page < (size / 2) + (4 * 4096) && page + 4096 <= size; page = page + 4096) {
                file.seek(page - (page % 4096));
                file.write(rubbish);
            }
        }
    }

    private void runImport(boolean seal) {
        assertTrue(LegacyImport.importReferenceTables(), "the import starts");
        LegacyImport.runActivityImport(seal);
    }

    private long count(String table) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    /** The time of a block row, derived from its row id so that the two can be checked against each other. */
    private static long blockTime(long rowId) {
        return base() + (rowId * 60);
    }

    private static long base() {
        long now = System.currentTimeMillis() / 1000L;
        return ((now - (30 * DAY)) / DAY) * DAY;
    }

    /**
     * Writes a database in the layout upstream CoreProtect uses, deliberately older than the current
     * one: the sign table has no waxed, face or secondary colour columns, and there is no
     * entity_container table at all.
     */
    private void writeLegacyDatabase() throws SQLException {
        try (Connection legacy = DriverManager.getConnection("jdbc:sqlite:" + source); Statement statement = legacy.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_item (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data BLOB, amount INTEGER, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_sign (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, action INTEGER, color INTEGER, data INTEGER, line_1 TEXT, line_2 TEXT, line_3 TEXT, line_4 TEXT);");
            statement.executeUpdate("CREATE TABLE co_user (id INTEGER PRIMARY KEY ASC, time INTEGER, user TEXT, uuid TEXT);");
            statement.executeUpdate("CREATE TABLE co_material_map (id INTEGER, material TEXT);");
            statement.executeUpdate("CREATE TABLE co_world (id INTEGER, world TEXT);");
            statement.executeUpdate("CREATE TABLE co_entity_spawn (id INTEGER PRIMARY KEY ASC, time INTEGER, block_rowid INTEGER, kill_rowid INTEGER, uuid TEXT UNIQUE, wid INTEGER, current_wid INTEGER, origin_x REAL, origin_y REAL, origin_z REAL, x REAL, y REAL, z REAL, yaw REAL, pitch REAL, data BLOB, removed INTEGER);");
            statement.executeUpdate("CREATE TABLE co_version (time INTEGER, version TEXT);");

            statement.executeUpdate("INSERT INTO co_world (id, world) VALUES (1, 'world');");
            statement.executeUpdate("INSERT INTO co_material_map (id, material) VALUES (10, 'minecraft:stone'), (11, 'minecraft:dirt');");
            statement.executeUpdate("INSERT INTO co_user (id, time, user, uuid) VALUES (1, 0, 'Alice', 'a'), (2, 0, 'Bob', 'b'), (3, 0, 'Carol', 'c');");
            statement.executeUpdate("INSERT INTO co_version (time, version) VALUES (0, '22.4');");
            statement.executeUpdate("INSERT INTO co_entity_spawn (id, time, block_rowid, kill_rowid, uuid, wid, current_wid, origin_x, origin_y, origin_z, x, y, z, yaw, pitch, data, removed) "
                    + "VALUES (1, 0, 7, NULL, 'spawn-one', 1, 1, 0, 64, 0, 0, 64, 0, 0, 0, NULL, 0), (2, 0, 9, NULL, 'spawn-two', 1, 1, 0, 64, 0, 0, 64, 0, 0, 0, NULL, 0);");
        }

        try (Connection legacy = DriverManager.getConnection("jdbc:sqlite:" + source)) {
            legacy.setAutoCommit(false);
            try (PreparedStatement statement = legacy.prepareStatement(
                    "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,NULL,NULL,?,0)")) {
                for (int row = 1; row <= BLOCK_ROWS; row++) {
                    statement.setLong(1, blockTime(row));
                    statement.setInt(2, 1 + (row % 3));
                    statement.setInt(3, row % 50);
                    statement.setInt(4, row % 50);
                    statement.setInt(5, 10 + (row % 2));
                    statement.setInt(6, row % 2);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            try (PreparedStatement statement = legacy.prepareStatement(
                    "INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,1,?,64,?,?,NULL,1,?,0)")) {
                for (int row = 1; row <= ITEM_ROWS; row++) {
                    statement.setLong(1, blockTime(row));
                    statement.setInt(2, 1 + (row % 3));
                    statement.setInt(3, row % 50);
                    statement.setInt(4, row % 50);
                    statement.setInt(5, 10 + (row % 2));
                    statement.setInt(6, row % 2);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            try (PreparedStatement statement = legacy.prepareStatement(
                    "INSERT INTO co_sign (time,user,wid,x,y,z,action,color,data,line_1,line_2,line_3,line_4) VALUES (?,1,1,0,64,0,0,0,0,'line 1','line 2','line 3','line 4')")) {
                for (int row = 1; row <= SIGN_ROWS; row++) {
                    statement.setLong(1, blockTime(row));
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            legacy.commit();
        }
    }

    /** Creates an empty database in this fork's layout, the way start-up would. */
    private void createTargetDatabase() throws SQLException {
        Connection target = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        try (Statement statement = target.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
        }
        Database.createDatabaseTables("co_", true, target, DatabaseType.SQLITE, false);

        try (Connection schema = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
                Statement statement = schema.createStatement()) {
            SQLiteSchema.createTables("co_", statement);
        }
    }
}
