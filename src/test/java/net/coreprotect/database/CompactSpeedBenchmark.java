package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * Measures how long the compressing parts of a compact take on one core and on several.
 *
 * <p>
 * A compact is dominated by compression at the level used for long term storage, which is slow on
 * purpose: the result is written once and read for years. That work depends on nothing but the bytes
 * handed to it, so it is the one part of a compact that more cores can help with, and this says by
 * how much on the machine it is run on.
 * </p>
 *
 * <p>
 * Run with <code>mvn test -Dcoreprotect.benchmark=true -Dtest=CompactSpeedBenchmark</code>. It is
 * off by default because it takes minutes.
 * </p>
 */
@EnabledIfSystemProperty(named = "coreprotect.benchmark", matches = "true")
class CompactSpeedBenchmark {

    private static final int BLOCK_ROWS = 400000;
    private static final int ENTITY_ROWS = 60000;
    private static final long DAY = 86400L;

    @BeforeAll
    static void prepareServer() {
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = org.mockito.Mockito.mock(org.bukkit.Server.class);
            org.mockito.Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
    }

    @Test
    void sealingAndPackingScaleWithCores(@TempDir Path directory) throws Exception {
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.printf("%d cores available%n", cores);

        long oneThread = measure(directory.resolve("one.db"), 1);
        long manyThreads = measure(directory.resolve("many.db"), 0);

        System.out.printf("sealing and packing: one core %,d ms, %d cores %,d ms (%.1fx)%n",
                oneThread, CompactWorkers.threads(), manyThreads, oneThread / (double) manyThreads);
        assertTrue(manyThreads > 0);
    }

    /** Builds a database, compacts it, and says how long the compacting took. */
    private long measure(Path file, int threads) throws Exception {
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        BlobDictionary.clear();
        ColdBlobStore.clearCache();
        Config.getGlobal().COMPACT_THREADS = threads;

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file)) {
            build(connection);

            long started = System.currentTimeMillis();
            ColdRollupTask.rollUp(connection, () -> {
            }, (System.currentTimeMillis() / 1000L) + 1);
            long sealing = System.currentTimeMillis() - started;

            long packingStarted = System.currentTimeMillis();
            BlobRecompressTask.run(connection, () -> {
            });
            long packing = System.currentTimeMillis() - packingStarted;
            System.out.printf("  %s: sealing %,d ms, packing %,d ms%n", threads == 1 ? "one core" : "several cores", sealing, packing);
            return sealing + packing;
        }
        finally {
            Config.getGlobal().COMPACT_THREADS = 0;
        }
    }

    private void build(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            statement.executeUpdate("CREATE TABLE co_entity_spawn (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            SQLiteSchema.createTables("co_", statement);
        }

        long base = (((System.currentTimeMillis() / 1000L) - (60 * DAY)) / DAY) * DAY;
        Random random = new Random(7);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,?,?,?,0,NULL,NULL,?,0)")) {
            for (int row = 0; row < BLOCK_ROWS; row++) {
                statement.setLong(1, base + (row / 500));
                statement.setInt(2, random.nextInt(200));
                statement.setInt(3, random.nextInt(4000) - 2000);
                statement.setInt(4, random.nextInt(200));
                statement.setInt(5, random.nextInt(4000) - 2000);
                statement.setInt(6, random.nextInt(600));
                statement.setInt(7, random.nextInt(3));
                statement.addBatch();
                if (row % 20000 == 0) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
        }
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity (id, time, data) VALUES (?,?,?)")) {
            for (int row = 1; row <= ENTITY_ROWS; row++) {
                statement.setInt(1, row);
                statement.setLong(2, base + row);
                statement.setBytes(3, entityBlob(row));
                statement.addBatch();
                if (row % 10000 == 0) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    /** Shaped like real entity data, so compressing it costs what it costs on a server. */
    private static byte[] entityBlob(int row) {
        Random random = new Random(row);
        StringBuilder blob = new StringBuilder("¬í sr java.util.ArrayList");
        String[] attributes = { "GENERIC_MAX_HEALTH", "GENERIC_MOVEMENT_SPEED", "GENERIC_ARMOR", "GENERIC_ATTACK_DAMAGE",
                "GENERIC_FOLLOW_RANGE", "GENERIC_KNOCKBACK_RESISTANCE", "GENERIC_LUCK", "GENERIC_MAX_ABSORPTION" };
        for (String attribute : attributes) {
            blob.append("sr 'org.bukkit.craftbukkit.attribute.CraftAttributeInstance").append(attribute)
                    .append("xpw   sr java.lang.Double").append(random.nextInt(40)).append(".0");
        }
        while (blob.length() < 2000) {
            blob.append("minecraft:CustomNameVisible NoAI PersistenceRequired Health Fire Air Motion Rotation Pos UUID ");
        }
        return blob.append(row).toString().getBytes(StandardCharsets.UTF_8);
    }
}
