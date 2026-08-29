package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * The gate for logging carrying on while a compact runs.
 *
 * <p>
 * A compact is a long run of write transactions. SQLite allows one writer and does not hand the lock
 * out in turn: a writer that is waiting sleeps, wakes to find the lock taken again, and can go on
 * missing it until it gives up. A compact that asks for the lock back the instant it lets go can
 * therefore keep another writer out indefinitely, however short each of its own transactions is.
 * </p>
 *
 * <p>
 * That is what the server saw. It cannot be caught by running a compact on its own, which is how the
 * other tests here run it, so this one runs a writer alongside and requires every one of its writes
 * to get through. The writer is given a short time to wait so the test is quick; the real consumer
 * waits thirty seconds and was still refused, so anything that starves this writer for a second would
 * starve that one for half a minute.
 * </p>
 */
class CompactContentionTest {

    /** Rows to pack. Enough that packing takes long enough for starvation to show. */
    private static final int ROWS = 20000;

    /** How long the competing writer waits for the lock before giving up. */
    private static final int WRITER_TIMEOUT_MILLISECONDS = 250;

    private Path file;
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
        ConfigHandler.serverRunning = true;
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        BlobDictionary.clear();
        ColdBlobStore.clearCache();

        file = directory.resolve("database.db");
        connection = open(30000);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("CREATE TABLE co_entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            statement.executeUpdate("CREATE TABLE co_entity_spawn (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
        }

        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity (id, time, data) VALUES (?,?,?)")) {
            for (int row = 1; row <= ROWS; row++) {
                statement.setInt(1, row);
                statement.setInt(2, row);
                statement.setBytes(3, entityBlob(row));
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    @AfterEach
    void closeDatabase() throws SQLException {
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
    void everyLoggedRowGetsThroughWhilePackingRuns() throws Exception {
        AtomicBoolean packing = new AtomicBoolean(true);
        List<String> refused = new ArrayList<>();
        List<Long> waits = new ArrayList<>();

        Thread writer = new Thread(() -> {
            try (Connection other = open(WRITER_TIMEOUT_MILLISECONDS); Statement statement = other.createStatement()) {
                while (packing.get()) {
                    long started = System.currentTimeMillis();
                    try {
                        // What the consumer does every time it has something to record.
                        statement.executeUpdate("BEGIN IMMEDIATE TRANSACTION");
                        statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (1,1,1,0,64,0,10,0,NULL,NULL,0,0)");
                        statement.executeUpdate("COMMIT");
                        waits.add(System.currentTimeMillis() - started);
                    }
                    catch (SQLException exception) {
                        refused.add(exception.getMessage());
                        try {
                            statement.executeUpdate("ROLLBACK");
                        }
                        catch (SQLException ignored) {
                            // Nothing was open.
                        }
                    }
                    Thread.sleep(2);
                }
            }
            catch (Exception exception) {
                refused.add("writer stopped: " + exception);
            }
        }, "test-consumer");

        writer.start();
        try {
            long saved = BlobRecompressTask.run(connection, () -> {
            });
            assertTrue(saved > 0, "the packing did real work to contend with");
        }
        finally {
            packing.set(false);
            writer.join();
        }

        long longest = waits.stream().mapToLong(Long::longValue).max().orElse(0);
        System.out.printf("writer: %,d writes, %,d refused, longest wait %,d ms%n", waits.size(), refused.size(), longest);

        // Enough turns that a refusal would have had somewhere to happen, without pinning the count
        // to how fast the machine running this happens to be.
        assertTrue(waits.size() > 3, "the writer got enough turns to be a fair test: " + waits.size());
        assertEquals(0, refused.size(), "every write got through, but " + refused.size() + " were refused, first: "
                + (refused.isEmpty() ? "" : refused.get(0)));
    }

    @Test
    void everyLoggedRowGetsThroughWhileFreedPagesAreHandedBack() throws Exception {
        // What the server was actually doing when logging was refused. Once everything is packed, a
        // compact's remaining work is handing back the pages that freed, and after a large one there
        // can be millions of them. Each hand back is a write transaction, so a run of them with no
        // gap keeps every other writer out for as long as it lasts.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_entity");
        }
        long freed = freePages();
        assertTrue(freed > 4000, "there are enough pages to hand back for this to take a while: " + freed);

        AtomicBoolean running = new AtomicBoolean(true);
        List<String> refused = new ArrayList<>();
        List<Long> waits = new ArrayList<>();
        Thread writer = writer(running, refused, waits);

        writer.start();
        try {
            Database.reclaimFreePages(connection);
        }
        finally {
            running.set(false);
            writer.join();
        }

        assertEquals(0, freePages(), "every page was handed back");
        System.out.printf("reclaim: %,d pages, writer %,d writes, %,d refused, longest wait %,d ms%n",
                freed, waits.size(), refused.size(), waits.stream().mapToLong(Long::longValue).max().orElse(0));
        assertEquals(0, refused.size(), "every write got through, first refusal: "
                + (refused.isEmpty() ? "" : refused.get(0)));
    }

    private long freePages() throws SQLException {
        try (Statement statement = connection.createStatement();
                java.sql.ResultSet results = statement.executeQuery("PRAGMA freelist_count")) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    /** A writer standing in for the consumer, which writes constantly while a compact runs. */
    private Thread writer(AtomicBoolean running, List<String> refused, List<Long> waits) {
        return new Thread(() -> {
            try (Connection other = open(WRITER_TIMEOUT_MILLISECONDS); Statement statement = other.createStatement()) {
                while (running.get()) {
                    long started = System.currentTimeMillis();
                    try {
                        statement.executeUpdate("BEGIN IMMEDIATE TRANSACTION");
                        statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (1,1,1,0,64,0,10,0,NULL,NULL,0,0)");
                        statement.executeUpdate("COMMIT");
                        waits.add(System.currentTimeMillis() - started);
                    }
                    catch (SQLException exception) {
                        refused.add(exception.getMessage());
                        try {
                            statement.executeUpdate("ROLLBACK");
                        }
                        catch (SQLException ignored) {
                            // Nothing was open.
                        }
                    }
                    Thread.sleep(5);
                }
            }
            catch (Exception exception) {
                refused.add("writer stopped: " + exception);
            }
        }, "test-consumer");
    }

    private Connection open(int busyTimeout) throws SQLException {
        Connection opened = DriverManager.getConnection("jdbc:sqlite:" + file);
        try (Statement statement = opened.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=" + busyTimeout);
        }
        return opened;
    }

    /** Shaped like real entity data, so packing costs what it costs on a server. */
    private static byte[] entityBlob(int row) {
        Random random = new Random(row);
        StringBuilder blob = new StringBuilder();
        blob.append("¬í sr java.util.ArrayList");
        String[] attributes = { "GENERIC_MAX_HEALTH", "GENERIC_MOVEMENT_SPEED", "GENERIC_ARMOR", "GENERIC_ARMOR_TOUGHNESS",
                "GENERIC_ATTACK_DAMAGE", "GENERIC_ATTACK_KNOCKBACK", "GENERIC_ATTACK_SPEED", "GENERIC_FLYING_SPEED",
                "GENERIC_FOLLOW_RANGE", "GENERIC_KNOCKBACK_RESISTANCE", "GENERIC_LUCK", "GENERIC_MAX_ABSORPTION" };
        for (String attribute : attributes) {
            blob.append("sr 'org.bukkit.craftbukkit.attribute.CraftAttributeInstance").append(attribute)
                    .append("xpw   sr java.lang.Double").append(random.nextInt(40)).append(".0");
        }
        String[] fields = { "CustomName", "CustomNameVisible", "NoAI", "PersistenceRequired", "Health", "Fire", "Air",
                "OnGround", "Invulnerable", "PortalCooldown", "FallDistance", "Motion", "Rotation", "Pos", "UUID" };
        while (blob.length() < 2000) {
            for (String name : fields) {
                blob.append("minecraft:").append(name).append("=  ");
            }
        }
        blob.append(row).append('|').append(random.nextInt(1000));
        return blob.toString().getBytes(StandardCharsets.UTF_8);
    }
}
