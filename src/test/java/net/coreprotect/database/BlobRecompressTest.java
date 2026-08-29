package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobCompression;
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * The gate for compressing the blobs that never reach a compressed segment.
 *
 * <p>
 * Entity data is read one row at a time by row id, so it stays in the live table where a single row
 * can be fetched on its own. Compressed by itself a blob of a couple of kilobytes barely shrinks,
 * because what repeats in this data repeats between rows rather than within any one of them. A
 * dictionary trained on a sample supplies that repetition, and these tests hold it to it: the blobs
 * have to come back byte for byte, and they have to actually get much smaller, because a change that
 * quietly stopped using the dictionary would still read back perfectly while wasting the disk this
 * exists to save.
 * </p>
 */
class BlobRecompressTest {

    /** Rows written, enough to train a dictionary from and to span several batches. */
    private static final int ROWS = 6000;

    private Connection connection;
    private Path file;
    private DatabaseType previousType;
    private Map<Long, byte[]> written;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        BlobDictionary.clear();
        SegmentDictionary.clearCache();

        file = directory.resolve("database.db");
        connection = DriverManager.getConnection("jdbc:sqlite:" + file);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("CREATE TABLE co_entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            statement.executeUpdate("CREATE TABLE co_entity_spawn (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            SQLiteSchema.createTables("co_", statement);
        }

        written = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity (id, time, data) VALUES (?,?,?)")) {
            for (int row = 1; row <= ROWS; row++) {
                byte[] blob = entityBlob(row);
                written.put((long) row, blob);
                statement.setInt(1, row);
                statement.setInt(2, row);
                statement.setBytes(3, blob);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        BlobDictionary.clear();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void everyBlobStillReadsBackExactly() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });

        for (Map.Entry<Long, byte[]> entry : written.entrySet()) {
            assertArrayEquals(entry.getValue(), blobFor(entry.getKey()), "row " + entry.getKey() + " reads back as it was written");
        }
        assertEquals(ROWS, written.size(), "every row was checked");
    }

    @Test
    void theBlobsActuallyGetMuchSmaller() throws Exception {
        long before = storedBytes();
        long saved = BlobRecompressTask.run(connection, () -> {
        });
        long after = storedBytes();

        assertEquals(before - after, saved, "the saving reported is the saving made");
        assertTrue(BlobDictionary.hasDictionary(), "a dictionary was trained");

        // Compressing each of these blobs on its own is worth about three times. Anything close to
        // that means the dictionary is not being used, which is the failure this is here to catch.
        double ratio = before / (double) after;
        assertTrue(ratio > 8, "the dictionary is doing the work: only " + String.format("%.1fx", ratio));
    }

    @Test
    void theDatabaseFileItselfGetsSmaller() throws Exception {
        // The saving only counts if the file shrinks. Writing a shorter blob over a longer one leaves
        // the row on the page it was already on, and SQLite never repacks a page that still holds
        // rows, so a rewrite done that way compresses everything and frees nothing.
        long before = Files.size(file);
        BlobRecompressTask.run(connection, () -> {
        });
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
        }
        long after = Files.size(file);

        assertTrue(after * 2 < before, "the file shrank: " + before + " -> " + after + " bytes");
    }

    @Test
    void theSavingIsReportedSoTheShrinkIsNotMistakenForDataLoss() throws Exception {
        // Entity data stays in the live tables, so compressing it makes the hot size fall while the
        // cold size stays put. Without a figure for it that reads like rows going missing.
        long saved = BlobRecompressTask.run(connection, () -> {
        });

        ColdStorageStats stats = ColdStorageStats.read(connection);
        assertNotNull(stats);
        assertEquals(saved, stats.getBlobSavedBytes(), "the saving is reported for the status command");
        assertTrue(stats.getBlobSavedBytes() > 0, "and it is not nothing");
    }

    @Test
    void packedEntityDataCountsAsCompressedStorage() throws Exception {
        // Packing entity data does not seal a single segment, so if only segments counted the
        // compressed total would sit still while the live total fell, which reads as data going
        // missing rather than as compression working.
        ColdStorageStats before = ColdStorageStats.read(connection);
        assertNotNull(before);

        BlobRecompressTask.run(connection, () -> {
        });

        ColdStorageStats after = ColdStorageStats.read(connection);
        assertTrue(after.getBlobBytes() > 0, "the packed data is measured");
        assertTrue(after.getColdBytes() > before.getColdBytes(), "and counted as compressed storage");
        assertTrue(after.getHotBytes() < before.getHotBytes(), "while the live total falls");
    }

    @Test
    void theSavingAddsUpAcrossRuns() throws Exception {
        int[] batches = { 0 };
        assertThrows(InterruptedException.class, () -> BlobRecompressTask.run(connection, () -> {
            if (batches[0]++ == 2) {
                throw new InterruptedException("stopped");
            }
        }));
        long first = ColdStorageStats.read(connection).getBlobSavedBytes();
        assertTrue(first > 0, "the interrupted run recorded what it managed");

        BlobRecompressTask.run(connection, () -> {
        });

        assertTrue(ColdStorageStats.read(connection).getBlobSavedBytes() > first, "the rest is added to it, not replacing it");
    }

    @Test
    void runningItAgainDoesNothingMore() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });
        long after = storedBytes();

        long saved = BlobRecompressTask.run(connection, () -> {
        });

        assertEquals(0, saved, "there is nothing left to save");
        assertEquals(after, storedBytes(), "and nothing was rewritten");
    }

    @Test
    void anInterruptedRunCarriesOnFromWhereItStopped() throws Exception {
        // Stopping part way is the normal way a run ends when the server is shutting down.
        int[] batches = { 0 };
        assertThrows(InterruptedException.class, () -> BlobRecompressTask.run(connection, () -> {
            if (batches[0]++ == 2) {
                throw new InterruptedException("stopped");
            }
        }));

        long partway = storedBytes();
        assertTrue(partway < storedBytesUncompressed(), "some of it was rewritten");

        BlobRecompressTask.run(connection, () -> {
        });

        for (Map.Entry<Long, byte[]> entry : written.entrySet()) {
            assertArrayEquals(entry.getValue(), blobFor(entry.getKey()),
                    "row " + entry.getKey() + " survived being rewritten in two goes");
        }
        assertTrue(storedBytes() < partway, "the rest was rewritten on the second run");
    }

    @Test
    void aBlobWhoseDictionaryIsMissingIsReportedRatherThanGuessedAt() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });

        byte[] compressed;
        // One of the newest rows, still too close to the end of the table to have been grouped.
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT data FROM co_entity WHERE data IS NOT NULL ORDER BY rowid DESC LIMIT 1")) {
            assertTrue(results.next());
            compressed = results.getBytes(1);
        }
        assertNotNull(compressed);

        // A database restored without its dictionaries, which must not quietly hand back rubbish.
        BlobDictionary.clear();
        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> BlobCompression.decompress(compressed));
        assertTrue(failure.getMessage().contains("missing"), failure.getMessage());
    }

    @Test
    void blobsWrittenBeforeTheDictionaryStillRead() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });

        // A row written by an older build: no compression at all. Nothing about having a dictionary
        // may stop it being read.
        byte[] legacy = entityBlob(999999);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity (id, time, data) VALUES (?,?,?)")) {
            statement.setInt(1, ROWS + 1);
            statement.setInt(2, 1);
            statement.setBytes(3, legacy);
            statement.executeUpdate();
        }

        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT data FROM co_entity WHERE rowid = " + (ROWS + 1))) {
            assertTrue(results.next());
            assertArrayEquals(legacy, BlobCompression.decompress(results.getBytes(1)), "an uncompressed blob is returned as it is");
        }
    }

    /** Everything the blobs occupy, whether still on their rows or packed away into groups. */
    private long storedBytes() throws SQLException {
        long total = 0;
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(LENGTH(data)),0) FROM co_entity")) {
            assertTrue(results.next());
            total = results.getLong(1);
        }
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(LENGTH(data)),0) FROM co_blob_group")) {
            assertTrue(results.next());
            total = total + results.getLong(1);
        }
        return total;
    }

    /** Reads a blob the way the plugin does: from the row if it is there, from its group if not. */
    private byte[] blobFor(long rowId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT data FROM co_entity WHERE rowid = ?")) {
            statement.setLong(1, rowId);
            try (ResultSet results = statement.executeQuery()) {
                if (results.next()) {
                    byte[] stored = results.getBytes(1);
                    if (stored != null && stored.length > 0) {
                        return BlobCompression.decompress(stored);
                    }
                }
            }
        }
        return ColdBlobStore.load(connection, "entity", rowId);
    }

    private long storedBytesUncompressed() {
        long total = 0;
        for (byte[] blob : written.values()) {
            total = total + blob.length;
        }
        return total;
    }

    /**
     * A blob shaped like the entity data this exists for: mostly the same structure and field names
     * every time, with a little that differs from row to row. That is what makes a dictionary worth
     * so much more here than a higher compression level.
     */
    private static byte[] entityBlob(int row) {
        Random random = new Random(row);
        StringBuilder blob = new StringBuilder();
        blob.append("¬í sr java.util.ArrayList");
        String[] attributes = { "GENERIC_MAX_HEALTH", "GENERIC_MOVEMENT_SPEED", "GENERIC_ARMOR", "GENERIC_ATTACK_DAMAGE",
                "GENERIC_FOLLOW_RANGE", "GENERIC_KNOCKBACK_RESISTANCE" };
        for (String attribute : attributes) {
            blob.append("sr org.bukkit.attribute.Attribute").append(attribute)
                    .append("xpw   sr java.lang.Double")
                    .append(random.nextInt(40)).append(".0");
        }
        List<String> tags = new ArrayList<>();
        for (int index = 0; index < 12; index++) {
            tags.add("minecraft:custom_tag_" + random.nextInt(20));
        }
        blob.append(String.join(",", tags));
        blob.append("CustomNameVisibleNoAIPersistenceRequiredHealthFireTicksAirLevel");
        blob.append(row).append('|').append(random.nextInt(1000000));
        return blob.toString().getBytes(StandardCharsets.UTF_8);
    }
}
