package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobCompression;
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * Measures blob compression against real entity data, when a sample of it is available.
 *
 * <p>
 * The unit tests use blobs shaped like the real thing, which is enough to catch the dictionary being
 * dropped but not to say what a server would actually save. This runs the same work over a copy of
 * real rows and prints the result. It is skipped unless the sample is there, so it never fails a
 * build on a machine that does not have one.
 * </p>
 *
 * <p>
 * A sample can be made from any CoreProtect database with:
 * </p>
 *
 * <pre>
 * sqlite3 sample.db "ATTACH 'database.db' AS live;
 *   CREATE TABLE co_entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);
 *   INSERT INTO co_entity SELECT id, time, data FROM live.co_entity LIMIT 50000;"
 * </pre>
 */
class BlobRecompressBenchmark {

    private static final Path SAMPLE = Paths.get(System.getProperty("user.home"), "Downloads", "entity-sample.db");

    static boolean hasSample() {
        return Files.isRegularFile(SAMPLE);
    }

    @Test
    @EnabledIf("hasSample")
    void measureAgainstRealEntityData(@TempDir Path directory) throws Exception {
        Path working = directory.resolve("sample.db");
        Files.copy(SAMPLE, working, StandardCopyOption.REPLACE_EXISTING);

        DatabaseType previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        BlobDictionary.clear();
        SegmentDictionary.clearCache();

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + working)) {
            try (Statement statement = connection.createStatement()) {
                SQLiteSchema.createTables("co_", statement);
            }

            Map<Long, byte[]> before = readAll(connection);
            long blobBytesBefore = storedBytes(connection);
            long fileBefore = Files.size(working);

            long start = System.currentTimeMillis();
            long saved = BlobRecompressTask.run(connection, () -> {
            });
            long elapsed = System.currentTimeMillis() - start;

            // Only the checkpoint, so that what is measured is what the task itself reclaims.
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }

            Map<Long, byte[]> after = readAll(connection);
            long blobBytesAfter = storedBytes(connection) + groupedBytes(connection);
            long fileAfter = Files.size(working);

            // Every blob has to come back exactly, or the number below means nothing.
            assertTrue(before.size() > 0, "the sample holds rows");
            for (Map.Entry<Long, byte[]> entry : before.entrySet()) {
                assertArrayEquals(entry.getValue(), after.get(entry.getKey()), "row " + entry.getKey() + " reads back unchanged");
            }

            System.out.printf("rows=%d%nblobs %,d -> %,d bytes (%.2fx, saved %,d)%nfile %,d -> %,d bytes%ntook %d ms (%,.0f rows/s)%n",
                    before.size(), blobBytesBefore, blobBytesAfter, blobBytesBefore / (double) blobBytesAfter, saved,
                    fileBefore, fileAfter, elapsed, before.size() / Math.max(elapsed / 1000.0, 0.001));

            // Compressing these blobs one at a time is worth about thirty times. Grouping them has to
            // be far ahead of that to be worth unpacking a group to read one row.
            assertTrue(blobBytesAfter * 50 < blobBytesBefore, "grouping beats compressing each blob singly");
            assertTrue(fileAfter * 25 < fileBefore, "and the file it is stored in shrinks with it");
        }
        finally {
            BlobDictionary.clear();
            SegmentDictionary.clearCache();
            ConfigHandler.databaseType = previousType;
        }
    }

    /** Reads every blob the way the plugin does: from the row when it is there, from its group when not. */
    private static Map<Long, byte[]> readAll(Connection connection) throws SQLException {
        Map<Long, byte[]> rows = new HashMap<>();
        java.util.List<Long> packedAway = new java.util.ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT rowid, data FROM co_entity ORDER BY rowid")) {
            while (results.next()) {
                byte[] stored = results.getBytes(2);
                if (stored == null || stored.length == 0) {
                    packedAway.add(results.getLong(1));
                }
                else {
                    rows.put(results.getLong(1), BlobCompression.decompress(stored));
                }
            }
        }
        rows.putAll(ColdBlobStore.load(connection, "entity", packedAway));
        return rows;
    }

    /** The bytes the packed groups occupy, which is where most of the blobs end up. */
    private static long groupedBytes(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(LENGTH(data)),0) FROM co_blob_group")) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    /** The bytes the blobs actually occupy in the table, which is what this is about. */
    private static long storedBytes(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(LENGTH(data)),0) FROM co_entity")) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
