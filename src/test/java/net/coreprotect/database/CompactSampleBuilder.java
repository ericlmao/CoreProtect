package net.coreprotect.database;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import net.coreprotect.config.ConfigHandler;

/**
 * Builds a database of the shape and size a busy server produces, for the real compact run to use.
 *
 * <p>
 * The unit tests work on a few thousand rows, which is enough to say whether a thing is right and
 * nothing about whether it is fast or whether it survives a file of tens of gigabytes. This writes
 * one that behaves like the real thing: months of block history, entity data that is nearly all of
 * the bytes, and containers, all with timestamps spread over the past so the roll-up has whole days
 * to seal.
 * </p>
 *
 * <p>
 * Run with <code>mvn test -Dcoreprotect.sample=true -Dtest=CompactSampleBuilder</code>, then
 * <code>-Dtest=RealCompactRun</code> to compact it.
 * </p>
 */
@EnabledIfSystemProperty(named = "coreprotect.sample", matches = "true")
class CompactSampleBuilder {

    private static final Path DATABASE = Paths.get(System.getProperty("user.home"), "Downloads", "compact-test.db");

    private static final int BLOCK_ROWS = 6000000;
    private static final int CONTAINER_ROWS = 400000;
    private static final int ENTITY_ROWS = 600000;
    private static final int SPAWN_ROWS = 600000;
    private static final long DAY = 86400L;

    @Test
    void buildSample() throws Exception {
        Files.deleteIfExists(DATABASE);
        Files.deleteIfExists(Paths.get(DATABASE + "-wal"));
        Files.deleteIfExists(Paths.get(DATABASE + "-shm"));

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";

        long started = System.currentTimeMillis();

        // The plugin's own schema, so the sample has every table a server has rather than only the
        // ones this fills. Creating them closes the connection it is given, so the writing below is
        // done on another.
        Connection schema = DriverManager.getConnection("jdbc:sqlite:" + DATABASE);
        try (Statement statement = schema.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
        }
        Database.createDatabaseTables("co_", true, schema, DatabaseType.SQLITE, false);

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + DATABASE)) {
            try (Statement statement = connection.createStatement()) {
                SQLiteSchema.createTables("co_", statement);
                statement.executeUpdate("PRAGMA journal_mode=WAL");
                statement.executeUpdate("PRAGMA synchronous=OFF");
            }

            long base = (((System.currentTimeMillis() / 1000L) - (120 * DAY)) / DAY) * DAY;
            writeBlocks(connection, base);
            writeContainers(connection, base);
            writeEntities(connection, base);
            writeSpawns(connection, base);

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }
        }

        System.out.printf("built %s: %,d bytes in %,d ms%n", DATABASE, Files.size(DATABASE), System.currentTimeMillis() - started);
    }

    private void writeBlocks(Connection connection, long base) throws SQLException {
        Random random = new Random(11);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,0,?,NULL,?,0)")) {
            for (int row = 0; row < BLOCK_ROWS; row++) {
                statement.setLong(1, base + (long) (row / (BLOCK_ROWS / (100.0 * 86400))));
                statement.setInt(2, random.nextInt(400));
                statement.setInt(3, random.nextInt(3));
                statement.setInt(4, random.nextInt(8000) - 4000);
                statement.setInt(5, random.nextInt(250));
                statement.setInt(6, random.nextInt(8000) - 4000);
                statement.setInt(7, random.nextInt(900));
                // One block in twenty carries the sign text or container name a real server records.
                statement.setBytes(8, row % 20 == 0 ? text(random, 120) : null);
                statement.setInt(9, random.nextInt(3));
                statement.addBatch();
                if (row % 50000 == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void writeContainers(Connection connection, long base) throws SQLException {
        Random random = new Random(13);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?,?,?,?,?,?,0,?,?,?,0)")) {
            for (int row = 0; row < CONTAINER_ROWS; row++) {
                statement.setLong(1, base + (long) (row / (CONTAINER_ROWS / (100.0 * 86400))));
                statement.setInt(2, random.nextInt(400));
                statement.setInt(3, random.nextInt(3));
                statement.setInt(4, random.nextInt(8000) - 4000);
                statement.setInt(5, random.nextInt(250));
                statement.setInt(6, random.nextInt(8000) - 4000);
                statement.setInt(7, random.nextInt(900));
                statement.setInt(8, random.nextInt(64));
                statement.setBytes(9, text(random, 400));
                statement.setInt(10, random.nextInt(2));
                statement.addBatch();
                if (row % 20000 == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void writeEntities(Connection connection, long base) throws SQLException {
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_entity (id,time,data) VALUES (?,?,?)")) {
            for (int row = 1; row <= ENTITY_ROWS; row++) {
                statement.setInt(1, row);
                statement.setLong(2, base + (long) (row / (ENTITY_ROWS / (100.0 * 86400))));
                statement.setBytes(3, entityBlob(row));
                statement.addBatch();
                if (row % 10000 == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void writeSpawns(Connection connection, long base) throws SQLException {
        Random random = new Random(17);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO co_entity_spawn (id,time,block_rowid,kill_rowid,uuid,wid,current_wid,origin_x,origin_y,origin_z,x,y,z,yaw,pitch,data,removed) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            for (int row = 1; row <= SPAWN_ROWS; row++) {
                statement.setInt(1, row);
                statement.setLong(2, base + (long) (row / (SPAWN_ROWS / (100.0 * 86400))));
                statement.setLong(3, random.nextInt(BLOCK_ROWS) + 1L);
                statement.setNull(4, java.sql.Types.INTEGER);
                statement.setString(5, java.util.UUID.nameUUIDFromBytes(Integer.toString(row).getBytes(StandardCharsets.UTF_8)).toString());
                statement.setInt(6, random.nextInt(3));
                statement.setInt(7, random.nextInt(3));
                statement.setDouble(8, random.nextInt(8000) - 4000);
                statement.setDouble(9, random.nextInt(250));
                statement.setDouble(10, random.nextInt(8000) - 4000);
                statement.setDouble(11, random.nextInt(8000) - 4000);
                statement.setDouble(12, random.nextInt(250));
                statement.setDouble(13, random.nextInt(8000) - 4000);
                statement.setDouble(14, random.nextInt(360));
                statement.setDouble(15, random.nextInt(180) - 90);
                statement.setBytes(16, row % 3 == 0 ? entityBlob(row) : null);
                statement.setInt(17, row % 5 == 0 ? 1 : 0);
                statement.addBatch();
                if (row % 10000 == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    /** Shaped like the serialized entity data a server stores, which is most of a real file. */
    private static byte[] entityBlob(int row) {
        Random random = new Random(row);
        StringBuilder blob = new StringBuilder("¬í sr java.util.ArrayList");
        String[] attributes = { "GENERIC_MAX_HEALTH", "GENERIC_MOVEMENT_SPEED", "GENERIC_ARMOR", "GENERIC_ARMOR_TOUGHNESS",
                "GENERIC_ATTACK_DAMAGE", "GENERIC_ATTACK_KNOCKBACK", "GENERIC_ATTACK_SPEED", "GENERIC_FLYING_SPEED",
                "GENERIC_FOLLOW_RANGE", "GENERIC_KNOCKBACK_RESISTANCE", "GENERIC_LUCK", "GENERIC_MAX_ABSORPTION" };
        for (String attribute : attributes) {
            blob.append("sr 'org.bukkit.craftbukkit.attribute.CraftAttributeInstance").append(attribute)
                    .append("xpw   sr java.lang.Double").append(random.nextInt(40)).append('.').append(random.nextInt(99));
        }
        String[] fields = { "CustomName", "CustomNameVisible", "NoAI", "PersistenceRequired", "Health", "Fire", "Air",
                "OnGround", "Invulnerable", "PortalCooldown", "FallDistance", "Motion", "Rotation", "Pos", "UUID" };
        while (blob.length() < 2200) {
            for (String name : fields) {
                blob.append("minecraft:").append(name).append('=').append(random.nextInt(1000)).append(' ');
            }
        }
        return blob.append(row).toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] text(Random random, int length) {
        StringBuilder builder = new StringBuilder();
        String[] words = { "chest", "sign", "hopper", "barrel", "shulker_box", "furnace", "player", "diamond_sword",
                "enchanted_book", "netherite_ingot", "oak_planks", "stone_bricks" };
        while (builder.length() < length) {
            builder.append(words[random.nextInt(words.length)]).append(':').append(random.nextInt(64)).append(' ');
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }
}
