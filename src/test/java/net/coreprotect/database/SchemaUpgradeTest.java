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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.model.action.EntityActionFilter;
import net.coreprotect.model.action.LookupActions;
import net.coreprotect.model.lookup.EntityLookupContext;
import net.coreprotect.model.lookup.LookupRollbackState;
import net.coreprotect.model.rollback.RollbackUpdateTargets;

/**
 * A database created before {@code sign.rolled_back} existed gains the column when it is opened,
 * and every sign row reads correctly afterwards: rows packed before the column existed read as not
 * rolled back, rows packed or logged since keep their real state, and rollbacks reach all of them.
 */
class SchemaUpgradeTest {

    private static final long DAY = 86400L;
    private static final String OLD_SIGN_TABLE = "CREATE TABLE co_sign (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, action INTEGER, "
            + "color INTEGER, color_secondary INTEGER, data INTEGER, waxed INTEGER, face INTEGER, line_1 TEXT, line_2 TEXT, line_3 TEXT, line_4 TEXT, "
            + "line_5 TEXT, line_6 TEXT, line_7 TEXT, line_8 TEXT)";

    private Path database;
    private Connection connection;
    private DatabaseType previousType;
    /** The state every row should read with, by row id. */
    private final Map<Long, Integer> expected = new TreeMap<>();

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        SegmentCompactionTest.prepareServer();
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.worlds.put("world", 1);
        ConfigHandler.worldsReversed.put(1, "world");
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ColdRollupTask.setSegmentRowsForTesting(150);
        SegmentCompactor.setTargetRowsForTesting(1000);
        database = directory.resolve("database.db");
        connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate(OLD_SIGN_TABLE);
            statement.executeUpdate("CREATE INDEX sign_index ON co_sign(wid,x,z,time)");
            SQLiteSchema.createTables("co_", statement);
        }
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        ColdRollupTask.setSegmentRowsForTesting(0);
        SegmentCompactor.setTargetRowsForTesting(0);
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void oldSignRowsReadAsNotRolledBackAndNewOnesKeepTheirState() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long firstDay = ((now / DAY) - 6) * DAY;

        // Packed by a build that had no rolled_back column: one day rewritten in the first segment
        // format, as an older dev database holds it, and one in the current one, as production does.
        writeSigns(1, 300, firstDay + 3600, null);
        writeSigns(301, 300, firstDay + DAY + 3600, null);
        assertTrue(ColdRollupTask.rollUp(connection, () -> {
        }, firstDay + 2 * DAY) > 0, "the old rows are packed");
        SQLiteColdIndex.reload(connection);
        SQLiteColdIndex.TableLayout oldLayout = SQLiteColdIndex.layout(connection, "sign");
        assertEquals(-1, oldLayout.rolledBackColumn, "the segments are packed without the column");
        for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, "sign", 0, null, 0, 0, null)) {
            if (segment.maxTime < firstDay + DAY) {
                SegmentUnitStorageTest.rewriteAsVersionOne(connection, oldLayout, segment);
            }
        }
        SegmentFrameCache.clear();
        SQLiteColdIndex.reload(connection);
        assertTrue(count("SELECT COUNT(*) FROM co_segment WHERE table_id = 5 AND codec_version = 1") > 0);
        assertTrue(count("SELECT COUNT(*) FROM co_segment WHERE table_id = 5 AND codec_version = " + ColdSegmentCodec.CODEC_VERSION) > 0);
        // Logged by the old build and still live.
        writeSigns(5001, 20, now - 600, null);

        // The server starts on this build.
        upgrade();
        assertEquals(0, count("SELECT COUNT(*) FROM co_sign WHERE rolled_back IS NOT 0"), "live rows from before the column read as not rolled back");

        // Rows logged since, some already rolled back, packed on the same day as old ones and the next.
        writeSigns(601, 300, firstDay + DAY + 7200, 3);
        writeSigns(901, 300, firstDay + 2 * DAY + 3600, 4);
        assertTrue(ColdRollupTask.rollUp(connection, () -> {
        }, firstDay + 3 * DAY) > 0, "the new rows are packed");
        writeSigns(5021, 40, now - 300, 2);
        SQLiteColdIndex.reload(connection);
        assertTrue(count("SELECT COUNT(*) FROM co_sign WHERE rowid < 5000") == 0, "everything but the recent rows is packed");

        assertEverySignReads("after the upgrade");

        // A rollback reaches old packed rows, new packed rows and live rows alike.
        List<Long> rolledBack = List.of(5L, 350L, 650L, 950L, 5005L, 5030L);
        try (Statement statement = connection.createStatement()) {
            Database.performRolledBackUpdateChecked(statement, 1, rolledBack, RollbackUpdateTargets.SIGN);
        }
        for (long rowId : rolledBack) {
            expected.put(rowId, 1);
        }
        try (Statement statement = connection.createStatement()) {
            Database.performRolledBackUpdateChecked(statement, 0, List.of(603L), RollbackUpdateTargets.SIGN);
        }
        expected.put(603L, 0);
        assertEverySignReads("after a rollback");

        // Old segments are brought to the current format with the column, then compacted together
        // with the segments packed since.
        assertTrue(ColdRollupTask.repackOldSegments(connection, () -> {
        }, 0) > 0);
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE codec_version < " + ColdSegmentCodec.CODEC_VERSION));
        assertEverySignReads("after old segments are rewritten");
        long segmentsBefore = count("SELECT COUNT(*) FROM co_segment WHERE table_id = 5");
        assertTrue(SegmentCompactor.compactAll(connection, () -> {
        }) > 0, "the days are compacted");
        assertTrue(count("SELECT COUNT(*) FROM co_segment WHERE table_id = 5") < segmentsBefore, "segments of old and new layouts are merged");
        SegmentUnitStorageTest.assertUnitsMatchSegments(connection);
        assertEverySignReads("after compaction");
    }

    @Test
    void upgradingTwiceChangesNothingTheSecondTime() throws Exception {
        writeSigns(1, 10, 1000, null);

        try (Statement statement = connection.createStatement()) {
            assertTrue(SchemaUpgrade.ensureColumns(statement, "co_", DatabaseType.SQLITE), "the first start adds the column");
        }
        String schema = tableSql();
        assertTrue(schema.contains("rolled_back INTEGER DEFAULT 0"), schema);
        try (Statement statement = connection.createStatement()) {
            assertFalse(SchemaUpgrade.ensureColumns(statement, "co_", DatabaseType.SQLITE), "a second start finds nothing to add");
        }
        assertEquals(schema, tableSql(), "and leaves the table as it was");
        assertEquals(10, count("SELECT COUNT(*) FROM co_sign WHERE rolled_back = 0"));
    }

    @Test
    void openingTheSchemaAddsTheColumnToAnExistingDatabase() throws Exception {
        writeSigns(1, 10, 1000, null);
        connection.close();
        connection = null;

        // The way the plugin opens its database on every start, twice.
        for (int start = 0; start < 2; start++) {
            Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + database), DatabaseType.SQLITE, false);
        }

        connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        assertTrue(tableSql().contains("rolled_back INTEGER DEFAULT 0"));
        assertEquals(1, count("SELECT COUNT(*) FROM pragma_table_info('co_sign') WHERE name = 'rolled_back'"));
        assertEquals(10, count("SELECT COUNT(*) FROM co_sign WHERE rolled_back = 0"), "existing rows are kept, and read as not rolled back");
    }

    @Test
    void duckDBTablesGainTheColumnOnce() throws Exception {
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:"); Statement statement = duck.createStatement()) {
            statement.executeUpdate(OLD_SIGN_TABLE.replace(" TEXT", " VARCHAR"));
            statement.executeUpdate("INSERT INTO co_sign (time, user, line_1) VALUES (1, 1, 'old')");
            assertTrue(SchemaUpgrade.ensureColumns(statement, "co_", DatabaseType.DUCKDB));
            assertFalse(SchemaUpgrade.ensureColumns(statement, "co_", DatabaseType.DUCKDB));
            try (ResultSet results = statement.executeQuery("SELECT rolled_back FROM co_sign")) {
                assertTrue(results.next());
                assertEquals(0, results.getInt(1));
                assertFalse(results.wasNull(), "an existing row reads as not rolled back");
            }
        }
    }

    @Test
    void aMissingTableIsLeftForTheSchemaToCreate() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE co_sign");
            assertFalse(SchemaUpgrade.ensureColumns(statement, "co_", DatabaseType.SQLITE));
        }
    }

    private void upgrade() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            assertTrue(SchemaUpgrade.ensureColumns(statement, "co_", DatabaseType.SQLITE));
        }
    }

    /**
     * Writes sign rows with consecutive ids, at most three blocks apart.
     *
     * @param rolledBackEvery
     *            every this many rows is marked rolled back, or null to write the table as it was
     *            before the column existed
     */
    private void writeSigns(long firstRowId, int rows, long time, Integer rolledBackEvery) throws SQLException {
        String columns = "rowid,time,user,wid,x,y,z,action,color,color_secondary,data,waxed,face,line_1,line_2,line_3,line_4,line_5,line_6,line_7,line_8";
        String values = "?,?,?,1,?,64,?,1,0,0,0,0,0,?,'',NULL,NULL,'','',NULL,NULL";
        if (rolledBackEvery != null) {
            columns += ",rolled_back";
            values += ",?";
        }
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_sign (" + columns + ") VALUES (" + values + ")")) {
            for (int index = 0; index < rows; index++) {
                long rowId = firstRowId + index;
                int state = rolledBackEvery != null && index % rolledBackEvery == 0 ? 1 : 0;
                statement.setLong(1, rowId);
                statement.setLong(2, time + index);
                statement.setInt(3, 1 + (index % 3));
                statement.setInt(4, index % 3);
                statement.setInt(5, index % 2);
                statement.setString(6, "sign " + rowId);
                if (rolledBackEvery != null) {
                    statement.setInt(7, state);
                }
                statement.addBatch();
                expected.put(rowId, state);
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    /** Checks every sign row through the union lookups read and through the rollback lookup. */
    private void assertEverySignReads(String when) throws Exception {
        Map<Long, Integer> union = new TreeMap<>();
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "sign", 0, null);
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT rowid,rolled_back,line_1 FROM " + source)) {
                while (results.next()) {
                    long rowId = results.getLong(1);
                    union.put(rowId, results.getInt(2));
                    assertFalse(results.wasNull(), "row " + rowId + " has a rolled back state " + when);
                    assertEquals("sign " + rowId, results.getString(3), "row " + rowId + " keeps its text " + when);
                }
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        assertEquals(expected, union, "every row reads with its state " + when);

        Map<Long, Integer> forRollback = rollbackLookup(null);
        assertEquals(expected, forRollback, "a rollback sees every row in its state " + when);
        Map<Long, Integer> notRolledBack = new TreeMap<>();
        for (Map.Entry<Long, Integer> entry : expected.entrySet()) {
            if (entry.getValue() == 0) {
                notRolledBack.put(entry.getKey(), 0);
            }
        }
        assertEquals(notRolledBack, rollbackLookup(LookupRollbackState.NOT_ROLLED_BACK), "a rollback finds every row still to roll back " + when);
    }

    private Map<Long, Integer> rollbackLookup(LookupRollbackState state) throws Exception {
        CommandSender console = Mockito.mock(ConsoleCommandSender.class);
        Mockito.when(console.getName()).thenReturn("console");
        Mockito.when(console.hasPermission(Mockito.anyString())).thenReturn(true);
        Map<Long, Integer> found = new TreeMap<>();
        try (Statement statement = connection.createStatement()) {
            List<Object[]> rows = LookupRaw.performLookupRaw(statement, console, Collections.emptyList(), new ArrayList<>(), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                    new ArrayList<>(List.of(LookupActions.SIGN)), EntityActionFilter.NONE, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()),
                    null, null, null, 0, 0, -1, -1, false, false, null, state);
            for (Object[] row : rows) {
                found.put(((Number) row[0]).longValue(), ((Number) row[9]).intValue());
            }
        }
        return found;
    }

    private String tableSql() throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT sql FROM sqlite_master WHERE name = 'co_sign'")) {
            assertTrue(results.next());
            return results.getString(1);
        }
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }
}
