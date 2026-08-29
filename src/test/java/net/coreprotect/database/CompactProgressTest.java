package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * The gate for a compact saying what it is doing while it does it.
 *
 * <p>
 * Compacting a large database takes minutes, and until it finished there was nothing to say whether
 * it was working or stuck. Each phase records what it is on; whoever asked reads that every so often.
 * What matters is that the phases actually set it, since a reporter reading something nothing writes
 * would look exactly like a compact with nothing to say.
 * </p>
 */
class CompactProgressTest {

    private static final int ROWS = 3000;

    private Connection connection;
    private DatabaseType previousType;

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
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        BlobDictionary.clear();
        ColdBlobStore.clearCache();
        CompactProgress.clear();

        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("CREATE TABLE co_entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            statement.executeUpdate("CREATE TABLE co_entity_spawn (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            SQLiteSchema.createTables("co_", statement);
        }

        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity (id, time, data) VALUES (?,?,?)")) {
            for (int row = 1; row <= ROWS; row++) {
                statement.setInt(1, row);
                statement.setInt(2, row);
                statement.setBytes(3, blob(row));
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        CompactProgress.clear();
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        BlobDictionary.clear();
        ColdBlobStore.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void nothingIsReportedWhenNothingIsHappening() {
        assertNull(CompactProgress.line(), "an idle server has nothing to say");
    }

    @Test
    void aPhaseWithAKnownEndReportsHowFarThroughItIs() {
        CompactProgress.set("packing entity data", 25, 100);
        assertEquals("packing entity data (25%)", CompactProgress.line());

        CompactProgress.set("packing entity data", 100, 100);
        assertEquals("packing entity data (100%)", CompactProgress.line());

        // More than expected still reads as finished rather than as more than everything.
        CompactProgress.set("packing entity data", 150, 100);
        assertEquals("packing entity data (100%)", CompactProgress.line());
    }

    @Test
    void aPhaseWithNoKnownEndReportsWhatItHasDone() {
        // Sealing does not know how many rows it will reach until it gets there.
        CompactProgress.set("packing block into compressed storage", 12345, 0);
        assertTrue(CompactProgress.line().contains("12,345"), CompactProgress.line());

        CompactProgress.set("checking row numbering", 0, 0);
        assertEquals("checking row numbering", CompactProgress.line());
    }

    @Test
    void aPhaseTooLargeForAPercentageSaysHowMuchItHasDone() {
        // Returning freed space works through millions of pages a few thousand at a time. As a share
        // of the whole that is a fraction of one percent a go, so it reads as nought for minutes and
        // looks like nothing is happening. A size moves.
        CompactProgress.set("returning freed space", "2.10 GB of 74.8 GB");
        assertEquals("returning freed space (2.10 GB of 74.8 GB)", CompactProgress.line());

        // And setting it the other way again drops what was said before, rather than keeping it.
        CompactProgress.set("packing entity data", 1, 4);
        assertEquals("packing entity data (25%)", CompactProgress.line());
    }

    @Test
    void reclaimingSpaceReportsSomethingThatMoves() throws Exception {
        // Enough pages that a percentage would round to nothing for the first several batches.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_entity");
        }

        List<String> seen = new ArrayList<>();
        Database.reclaimFreePages(connection, () -> {
            String line = CompactProgress.line();
            if (line != null && (seen.isEmpty() || !seen.get(seen.size() - 1).equals(line))) {
                seen.add(line);
            }
        });

        assertTrue(!seen.isEmpty(), "returning space said what it was doing: " + seen);
        assertTrue(seen.get(seen.size() - 1).contains(" of "), "and said it as an amount: " + seen.get(seen.size() - 1));
    }

    @Test
    void packingActuallyReportsWhileItRuns() throws Exception {
        // The reporter is only worth anything if the work sets it, so this runs the real packing and
        // collects what it would have said.
        List<String> seen = new ArrayList<>();
        BlobRecompressTask.run(connection, () -> {
            String line = CompactProgress.line();
            if (line != null && (seen.isEmpty() || !seen.get(seen.size() - 1).equals(line))) {
                seen.add(line);
            }
        });

        assertTrue(!seen.isEmpty(), "packing said what it was doing");
        assertTrue(seen.stream().anyMatch(line -> line.contains("entity")), "and named the table it was on: " + seen);
    }

    private static byte[] blob(int row) {
        StringBuilder builder = new StringBuilder();
        while (builder.length() < 1200) {
            builder.append("minecraft:CustomNameVisible NoAI PersistenceRequired Health Fire Air ");
        }
        return builder.append(row).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
