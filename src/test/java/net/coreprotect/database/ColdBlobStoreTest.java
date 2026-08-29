package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * The gate for packing entity blobs into groups of neighbouring rows.
 *
 * <p>
 * A blob compressed on its own barely shrinks; sixty four of them compressed together shrink by more
 * than three times as much, because what repeats in this data repeats between rows. The cost of that
 * is having to unpack sixty four to read one, which is only acceptable because finding the right
 * group and the right blob inside it is arithmetic rather than a search. These tests hold both ends:
 * every blob has to come back exactly wherever it now lives, and it has to be found without reading
 * anything it does not need.
 * </p>
 */
class ColdBlobStoreTest {

    /** Enough rows to fill many groups and leave a tail that is deliberately left unpacked. */
    private static final int ROWS = 5000;

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
        ColdBlobStore.clearCache();

        file = directory.resolve("database.db");
        connection = DriverManager.getConnection("jdbc:sqlite:" + file);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("CREATE TABLE co_entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            statement.executeUpdate("CREATE TABLE co_entity_spawn (id INTEGER PRIMARY KEY ASC, time INTEGER, uuid TEXT, removed INTEGER, data BLOB);");
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
        ColdBlobStore.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void aGroupIsFoundByArithmeticRatherThanBySearching() {
        // The group a row belongs to is a division, and the same for every row in it.
        assertEquals(0, ColdBlobStore.groupOf(0));
        assertEquals(0, ColdBlobStore.groupOf(ColdBlobStore.GROUP_ROWS - 1));
        assertEquals(ColdBlobStore.GROUP_ROWS, ColdBlobStore.groupOf(ColdBlobStore.GROUP_ROWS));
        assertEquals(ColdBlobStore.GROUP_ROWS * 3, ColdBlobStore.groupOf((ColdBlobStore.GROUP_ROWS * 3) + 5));
    }

    @Test
    void aBlobIsFoundInsideAGroupByAddingUpTheLengthsBeforeIt() {
        Map<Long, byte[]> blobs = new HashMap<>();
        for (int index = 0; index < ColdBlobStore.GROUP_ROWS; index++) {
            blobs.put(100L + index, ("blob-" + index + "-" + repeat('x', index)).getBytes(StandardCharsets.UTF_8));
        }
        long first = ColdBlobStore.groupOf(100);
        ColdBlobStore.Group group = ColdBlobStore.pack(blobs, first);

        for (int index = 0; index < ColdBlobStore.GROUP_ROWS; index++) {
            long rowId = first + index;
            byte[] expected = blobs.get(rowId);
            byte[] found = ColdBlobStore.extract(null, group.frame, first, rowId);
            if (expected == null) {
                assertNull(found, "row " + rowId + " holds nothing");
            }
            else {
                assertArrayEquals(expected, found, "row " + rowId + " comes back as it went in");
            }
        }
    }

    @Test
    void gapsInAGroupDoNotShiftWhatFollowsThem() {
        // Purging leaves holes. The length list records them so later blobs keep their offsets.
        Map<Long, byte[]> blobs = new HashMap<>();
        blobs.put(0L, "first".getBytes(StandardCharsets.UTF_8));
        blobs.put(5L, "sixth".getBytes(StandardCharsets.UTF_8));
        blobs.put(63L, "last".getBytes(StandardCharsets.UTF_8));
        ColdBlobStore.Group group = ColdBlobStore.pack(blobs, 0);

        assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), ColdBlobStore.extract(null, group.frame, 0, 0));
        assertNull(ColdBlobStore.extract(null, group.frame, 0, 1), "a missing row reads as nothing");
        assertArrayEquals("sixth".getBytes(StandardCharsets.UTF_8), ColdBlobStore.extract(null, group.frame, 0, 5));
        assertArrayEquals("last".getBytes(StandardCharsets.UTF_8), ColdBlobStore.extract(null, group.frame, 0, 63));
    }

    @Test
    void everyBlobReadsBackAfterBeingPackedAway() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });

        long packed = groupedRows();
        assertTrue(packed > 0, "rows were packed into groups");

        for (Map.Entry<Long, byte[]> entry : written.entrySet()) {
            byte[] found = blobFor(entry.getKey());
            assertArrayEquals(entry.getValue(), found, "row " + entry.getKey() + " reads back wherever it now lives");
        }
    }

    @Test
    void theNewestRowsAreLeftWhereTheyAre() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });

        // A group is only worth making once it is complete, so the end of the table stays put.
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_entity WHERE rowid > " + (ROWS - ColdBlobStore.GROUP_ROWS) + " AND data IS NOT NULL")) {
            assertTrue(results.next());
            assertTrue(results.getInt(1) > 0, "the newest rows still carry their own blobs");
        }
    }

    @Test
    void groupingBeatsCompressingEachBlobOnItsOwn() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });

        long grouped;
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(LENGTH(data)),0) FROM co_blob_group")) {
            assertTrue(results.next());
            grouped = results.getLong(1);
        }
        assertTrue(grouped > 0, "something was packed");

        // The same rows, compressed one at a time against the same dictionary. That is what grouping
        // has to beat, and comparing against it rather than against a fixed number keeps the test
        // honest whatever the data happens to compress like.
        long singly = 0;
        for (long rowId : packedRowIds()) {
            singly = singly + net.coreprotect.utility.serialize.BlobCompression.recompress(written.get(rowId)).length;
        }

        // How far ahead it gets depends on how alike neighbouring rows are, and this data is made up
        // rather than logged, so the size of the win is left to the benchmark that runs on real rows.
        // What is checked here is that grouping is wired up and is genuinely ahead.
        assertTrue(grouped * 5 < singly * 4,
                String.format("grouping (%d bytes) has to beat blob at a time (%d bytes)", grouped, singly));
    }

    @Test
    void groupsWhoseRowsHaveBeenPurgedAreDropped() throws Exception {
        BlobRecompressTask.run(connection, () -> {
        });
        long before = groupCount();
        assertTrue(before > 0);

        // Retention removes the oldest rows, which are the lowest numbered.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_entity WHERE rowid <= 1000");
        }
        BlobRecompressTask.run(connection, () -> {
        });

        assertTrue(groupCount() < before, "the groups holding only purged rows are gone");
        for (Map.Entry<Long, byte[]> entry : written.entrySet()) {
            if (entry.getKey() > 1000) {
                assertArrayEquals(entry.getValue(), blobFor(entry.getKey()), "row " + entry.getKey() + " is untouched");
            }
        }
    }

    @Test
    void theFileGetsSmaller() throws Exception {
        long before = Files.size(file);
        BlobRecompressTask.run(connection, () -> {
        });
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
        }

        assertTrue(Files.size(file) * 2 < before, "the file shrank: " + before + " -> " + Files.size(file));
    }

    /** Reads a blob the way the plugin does: from the row if it is there, from its group if not. */
    private byte[] blobFor(long rowId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT data FROM co_entity WHERE rowid = ?")) {
            statement.setLong(1, rowId);
            try (ResultSet results = statement.executeQuery()) {
                if (results.next()) {
                    byte[] stored = results.getBytes(1);
                    if (stored != null && stored.length > 0) {
                        return net.coreprotect.utility.serialize.BlobCompression.decompress(stored);
                    }
                }
            }
        }
        return ColdBlobStore.load(connection, "entity", rowId);
    }

    private long groupedRows() throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(MAX(first_rowid),0) FROM co_blob_group")) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    /** The rows whose blobs have been packed away, which are the ones the table no longer carries. */
    private List<Long> packedRowIds() throws SQLException {
        List<Long> rowIds = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT rowid FROM co_entity WHERE data IS NULL ORDER BY rowid")) {
            while (results.next()) {
                rowIds.add(results.getLong(1));
            }
        }
        return rowIds;
    }

    private long groupCount() throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_blob_group")) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < count; index++) {
            builder.append(value);
        }
        return builder.toString();
    }

    /**
     * Shaped like the entity data this feature exists for: about two kilobytes, nearly all of it the
     * same field names and class descriptions as every other row, with only a few values differing.
     * That shape is the whole reason grouping wins, so a test using anything else would not be
     * measuring the thing it claims to.
     */
    private static byte[] entityBlob(int row) {
        Random random = new Random(row);
        StringBuilder blob = new StringBuilder();
        blob.append("\u00ac\u00ed\u0000\u0005sr\u0000\u0013java.util.ArrayListx\u0081\u00d2\u001d\u0099\u00c7a\u009d\u0003");
        String[] attributes = { "GENERIC_MAX_HEALTH", "GENERIC_MOVEMENT_SPEED", "GENERIC_ARMOR", "GENERIC_ARMOR_TOUGHNESS",
                "GENERIC_ATTACK_DAMAGE", "GENERIC_ATTACK_KNOCKBACK", "GENERIC_ATTACK_SPEED", "GENERIC_FLYING_SPEED",
                "GENERIC_FOLLOW_RANGE", "GENERIC_KNOCKBACK_RESISTANCE", "GENERIC_LUCK", "GENERIC_MAX_ABSORPTION" };
        for (String attribute : attributes) {
            blob.append("sr\u0000\u0027org.bukkit.craftbukkit.attribute.CraftAttributeInstance")
                    .append(attribute)
                    .append("xpw\u0004\u0000\u0000\u0000sr\u0000\u0010java.lang.Double\u0080\u00b3\u00c2J")
                    .append(random.nextInt(40)).append(".0");
        }
        String[] fields = { "CustomName", "CustomNameVisible", "NoAI", "PersistenceRequired", "Health", "Fire", "Air",
                "OnGround", "Invulnerable", "PortalCooldown", "FallDistance", "Motion", "Rotation", "Pos", "UUID" };
        while (blob.length() < 2000) {
            for (String name : fields) {
                blob.append("minecraft:").append(name).append("=\u0000\u0000");
            }
        }
        blob.append(row).append('|').append(random.nextInt(1000));
        return blob.toString().getBytes(StandardCharsets.UTF_8);
    }
}
