package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * Runs a whole compact against a real database and checks that nothing is left uncompressed.
 *
 * <p>
 * The unit tests each hold one piece of this to account on data made up for the purpose. This runs
 * the same sequence the compact command runs, in the same order, over a copy of a real server's
 * database, and then asks the questions that matter afterwards: is every activity table packed away,
 * is every entity blob packed away, did the file actually end up the size the data needs, and does
 * everything still read back.
 * </p>
 *
 * <p>
 * It is skipped unless the database is there, because it takes as long as a real compact does and
 * needs tens of gigabytes of disk.
 * </p>
 */
class RealCompactRun {

    private static final Path DATABASE = Paths.get(System.getProperty("user.home"), "Downloads", "compact-test.db");

    @org.junit.jupiter.api.BeforeAll
    static void prepareServer() {
        // The repair says what it moved, and saying anything needs a server to say it to.
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = org.mockito.Mockito.mock(org.bukkit.Server.class);
            org.mockito.Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
    }

    static boolean hasDatabase() {
        return Files.isRegularFile(DATABASE);
    }

    @Test
    @EnabledIf("hasDatabase")
    void compactingLeavesNothingUncompressed() throws Exception {
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.path = DATABASE.getParent().toString() + java.io.File.separator;
        ConfigHandler.sqlite = DATABASE.getFileName().toString();
        ConfigHandler.serverRunning = true;
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        BlobDictionary.clear();
        ColdBlobStore.clearCache();

        long fileBefore = Files.size(DATABASE);
        System.out.printf("start: file %,d bytes%n", fileBefore);

        try (Connection connection = open()) {
            // What start-up does before anything else: bring the layout up to date. A database from
            // an older build has neither the group table nor the newer segment columns, and every
            // step below reads or writes one of them.
            try (Statement statement = connection.createStatement()) {
                SQLiteSchema.createTables(ConfigHandler.prefix, statement);
            }

            report(connection, "before");

            BlobRecompressTask.loadDictionaries(connection);

            long started = System.currentTimeMillis();
            long moved = ColdRollupTask.repairRowIds(connection, () -> {
            });
            System.out.printf("moved %,d rows back above the segments in %,d ms%n", moved, System.currentTimeMillis() - started);

            started = System.currentTimeMillis();
            // The same sequence, in the same order, as the compact command.
            long sealed = ColdRollupTask.rollUp(connection, () -> {
            }, (System.currentTimeMillis() / 1000L) + 1);
            System.out.printf("sealed %,d rows in %,d ms%n", sealed, System.currentTimeMillis() - started);

            started = System.currentTimeMillis();
            long backfilled = ColdRollupTask.backfillStatistics(connection, () -> {
            });
            System.out.printf("backfilled %,d segments in %,d ms%n", backfilled, System.currentTimeMillis() - started);

            started = System.currentTimeMillis();
            long packed = BlobRecompressTask.run(connection, () -> {
            });
            System.out.printf("packed, saving %,d bytes in %,d ms%n", packed, System.currentTimeMillis() - started);

            started = System.currentTimeMillis();
            Database.reclaimFreePages(connection);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }
            System.out.printf("reclaimed in %,d ms%n", System.currentTimeMillis() - started);

            report(connection, "after");

            long fileAfter = Files.size(DATABASE);
            System.out.printf("file %,d -> %,d bytes (%.1fx)%n", fileBefore, fileAfter, fileBefore / (double) fileAfter);

            // Every table that becomes segments keeps at most its newest row, which is left behind so
            // the table can carry on numbering from it. Everything else has to be packed away.
            for (String table : SQLiteColdIndex.getSegmentedTables()) {
                long live = count(connection, ConfigHandler.prefix + table);
                assertTrue(live <= 1, table + " is fully packed away apart from its newest row, but holds " + live);
            }

            // And nothing may be sitting on a row id a segment already uses, which is what ordering
            // and paging across the two depend on.
            for (String table : SQLiteColdIndex.getSegmentedTables()) {
                long high = SQLiteColdIndex.coldHighWaterMark(table);
                if (high > 0) {
                    assertEquals(0, count(connection, ConfigHandler.prefix + table + " WHERE rowid <= " + high),
                            table + " has live rows numbered underneath its segments");
                }
            }

            // Every entity blob has to be in a group, apart from the newest rows a group cannot be made
            // from yet. Packing stops a whole group short of the end and then rounds down to a group
            // boundary, so what is left over is under two groups rather than under one.
            long loose = count(connection, ConfigHandler.prefix + "entity WHERE data IS NOT NULL");
            assertTrue(loose < 2 * ColdBlobStore.GROUP_ROWS, "only the newest entity rows keep their own blobs: " + loose);

            // Nothing is left waiting to be handed back.
            assertEquals(0, pragma(connection, "PRAGMA freelist_count"), "every free page was returned");

            // And the file is the size the data needs rather than the size it used to be.
            ColdStorageStats stats = ColdStorageStats.read(connection);
            long accounted = stats.getHotBytes() + stats.getColdBytes();
            System.out.printf("hot %,d + cold %,d = %,d of %,d bytes%n", stats.getHotBytes(), stats.getColdBytes(), accounted, fileAfter);
            assertTrue(fileAfter < fileBefore / 10, "the file is a fraction of what it was");

            assertEveryBlobReads(connection);
        }
    }

    /** Reads a sample of packed blobs back, since a smaller file that cannot be read is worthless. */
    private void assertEveryBlobReads(Connection connection) throws SQLException {
        List<Long> rowIds = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT rowid FROM " + ConfigHandler.prefix
                        + "entity WHERE data IS NULL ORDER BY rowid LIMIT 20000")) {
            while (results.next()) {
                rowIds.add(results.getLong(1));
            }
        }
        assertTrue(rowIds.size() > 0, "there are packed rows to read");

        int read = 0;
        for (Long rowId : rowIds) {
            byte[] blob = ColdBlobStore.load(connection, "entity", rowId);
            assertTrue(blob != null && blob.length > 0, "row " + rowId + " reads back");
            read++;
        }
        System.out.printf("read back %,d packed blobs%n", read);
    }

    private void report(Connection connection, String when) throws SQLException {
        System.out.printf("%s: entity %,d rows (%,d still holding blobs), groups %,d, segments %,d, free pages %,d%n",
                when,
                count(connection, ConfigHandler.prefix + "entity"),
                count(connection, ConfigHandler.prefix + "entity WHERE data IS NOT NULL"),
                count(connection, ConfigHandler.prefix + "blob_group"),
                count(connection, ConfigHandler.prefix + "segment"),
                pragma(connection, "PRAGMA freelist_count"));
    }

    private static long count(Connection connection, String from) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + from)) {
            return results.next() ? results.getLong(1) : -1;
        }
    }

    private static long pragma(Connection connection, String pragma) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(pragma)) {
            return results.next() ? results.getLong(1) : -1;
        }
    }

    /** Opened with the settings the plugin's own pool uses. */
    private static Connection open() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + DATABASE);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=30000");
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("PRAGMA synchronous=NORMAL");
            statement.executeUpdate("PRAGMA temp_store=FILE");
        }
        return connection;
    }
}
