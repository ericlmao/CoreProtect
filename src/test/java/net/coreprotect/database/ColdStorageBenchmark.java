package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.ItemDataCodec;

/**
 * Measures what the version 2 storage layout does to the size of the database file.
 *
 * <p>
 * This is a measurement, not a check, so it is skipped unless it is asked for:
 * {@code mvn -DskipTests=false -Dcoreprotect.benchmark=true -Dtest=ColdStorageBenchmark test}
 * </p>
 */
class ColdStorageBenchmark {

    private static final long DAY = 86400L;
    private static final Random RANDOM = new Random(20260828L);

    private static final String[] ITEM_NAMES = { "Dragon Blade", "Miner's Pick", "Hunter Bow", "Chestplate of the Deep", "Traveler's Boots", "Ancient Helm", "Ender Wand", "Frost Axe" };
    private static final String[] ENCHANTS = { "minecraft:sharpness", "minecraft:unbreaking", "minecraft:mending", "minecraft:efficiency", "minecraft:fortune", "minecraft:protection" };
    private static final String[] LORE = { "Forged in the nether", "A relic of the old world", "Bound to its owner", "Hums with quiet power" };

    @Test
    void reportsDatabaseSizeAcrossLayouts(@TempDir Path directory) throws Exception {
        assumeTrue(Boolean.getBoolean("coreprotect.benchmark"), "benchmark only runs when asked for");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        int blockRows = Integer.getInteger("coreprotect.benchmark.block", 400000);
        int containerRows = Integer.getInteger("coreprotect.benchmark.container", 80000);
        int itemRows = Integer.getInteger("coreprotect.benchmark.item", 60000);

        Path legacy = directory.resolve("legacy.db");
        Path modern = directory.resolve("modern.db");

        long legacySize = buildLegacy(legacy, blockRows, containerRows, itemRows);
        long[] modernSizes = buildModern(modern, blockRows, containerRows, itemRows);

        System.out.println("rows_block=" + blockRows + " rows_container=" + containerRows + " rows_item=" + itemRows);
        System.out.println("db_v1_java_serialization=" + legacySize);
        System.out.println("db_v2_hot_only=" + modernSizes[0]);
        System.out.println("db_v2_after_rollup=" + modernSizes[1]);
        System.out.printf("saved_hot_only=%.1f%%%n", 100.0 * (1 - (modernSizes[0] / (double) legacySize)));
        System.out.printf("saved_after_rollup=%.1f%%%n", 100.0 * (1 - (modernSizes[1] / (double) legacySize)));
        System.out.printf("shrink_factor=%.2fx%n", legacySize / (double) modernSizes[1]);
    }

    /** Builds a database the way version 1 stored data: Java serialized payloads, full indexes. */
    private long buildLegacy(Path database, int blockRows, int containerRows, int itemRows) throws Exception {
        try (Connection connection = open(database, false)) {
            createTables(connection, false);
            fill(connection, blockRows, containerRows, itemRows, false, oldTime());
            createIndexes(connection);
            checkpoint(connection);
        }
        return Files.size(database);
    }

    /** Builds a database the way version 2 stores data, before and after the roll-up. */
    private long[] buildModern(Path database, int blockRows, int containerRows, int itemRows) throws Exception {
        long hotOnly;
        long afterRollUp;

        try (Connection connection = open(database, true)) {
            createTables(connection, true);
            fill(connection, blockRows, containerRows, itemRows, true, oldTime());
            createIndexes(connection);
            checkpoint(connection);
            hotOnly = Files.size(database);

            long sealed = ColdRollupTask.rollUp(connection, () -> {
            });
            System.out.println("sealed_rows=" + sealed);
            try (Statement statement = connection.createStatement(); java.sql.ResultSet results = statement.executeQuery("SELECT COUNT(*), SUM(LENGTH(scalars)), SUM(LENGTH(COALESCE(payload,''))), SUM(row_count) FROM co_segment")) {
                results.next();
                System.out.println("segments=" + results.getLong(1) + " scalar_bytes=" + results.getLong(2) + " payload_bytes=" + results.getLong(3) + " segment_rows=" + results.getLong(4));
            }
            try (Statement statement = connection.createStatement(); java.sql.ResultSet results = statement.executeQuery("PRAGMA freelist_count")) {
                results.next();
                System.out.println("freelist_pages=" + results.getLong(1));
            }
            try (Statement statement = connection.createStatement(); java.sql.ResultSet results = statement.executeQuery("PRAGMA auto_vacuum")) {
                results.next();
                System.out.println("auto_vacuum=" + results.getLong(1));
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA incremental_vacuum");
            }
            checkpoint(connection);
            afterRollUp = Files.size(database);
        }

        return new long[] { hotOnly, afterRollUp };
    }

    private static long oldTime() {
        long now = System.currentTimeMillis() / 1000L;
        return ((now - (30 * DAY)) / DAY) * DAY + 60;
    }

    private Connection open(Path database, boolean modern) throws SQLException {
        if (modern) {
            // The version 2 layout chooses file level settings before anything is written, which is
            // what the plugin does on a new database.
            ConfigHandler.path = database.getParent().toString() + "/";
            ConfigHandler.sqlite = database.getFileName().toString();
            SQLiteSchema.prepareDatabaseFile();
        }

        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("PRAGMA synchronous=NORMAL");
        }
        return connection;
    }

    private void createTables(Connection connection, boolean modern) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_container (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, amount INTEGER, metadata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_item (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data BLOB, amount INTEGER, action INTEGER, rolled_back INTEGER);");
            if (modern) {
                SQLiteSchema.createTables("co_", statement);
            }
        }
    }

    private void createIndexes(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String table : Arrays.asList("block", "container", "item")) {
                statement.executeUpdate("CREATE INDEX " + table + "_index ON co_" + table + "(wid,x,z,time)");
                statement.executeUpdate("CREATE INDEX " + table + "_user_index ON co_" + table + "(user,time)");
                statement.executeUpdate("CREATE INDEX " + table + "_type_index ON co_" + table + "(type,time)");
            }
        }
    }

    private void checkpoint(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
        }
    }

    private void fill(Connection connection, int blockRows, int containerRows, int itemRows, boolean modern, long baseTime) throws Exception {
        connection.setAutoCommit(false);

        insert(connection, "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                blockRows, baseTime, 0.02, modern, 9, 12);
        insert(connection, "INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                containerRows, baseTime, 0.45, modern, 10, 12);
        insert(connection, "INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                itemRows, baseTime, 0.30, modern, 8, 11);

        connection.commit();
        connection.setAutoCommit(true);
    }

    private void insert(Connection connection, String sql, int rows, long baseTime, double payloadShare, boolean modern, int payloadColumn, int columns) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < rows; index++) {
                // Spread the history over three weeks so the roll-up produces day aligned segments.
                statement.setLong(1, baseTime + (long) ((index / (double) rows) * 21 * DAY));
                statement.setInt(2, RANDOM.nextInt(200) + 1);
                statement.setInt(3, 1);
                statement.setInt(4, RANDOM.nextInt(20000) - 10000);
                statement.setInt(5, RANDOM.nextInt(320) - 64);
                statement.setInt(6, RANDOM.nextInt(20000) - 10000);
                statement.setInt(7, RANDOM.nextInt(900) + 1);

                for (int column = 8; column <= columns; column++) {
                    if (column == payloadColumn) {
                        byte[] payload = RANDOM.nextDouble() < payloadShare ? payload(modern) : null;
                        statement.setBytes(column, payload);
                    }
                    else {
                        statement.setInt(column, RANDOM.nextInt(4));
                    }
                }

                statement.addBatch();
                if (index % 20000 == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
            connection.commit();
        }
    }

    /** Builds one payload in whichever encoding the layout being measured writes. */
    private byte[] payload(boolean modern) throws Exception {
        Object metadata = metadata();
        if (modern) {
            return ItemDataCodec.encode(metadata);
        }

        try (java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream(); java.io.ObjectOutputStream output = new java.io.ObjectOutputStream(bytes)) {
            output.writeObject(metadata);
            output.flush();
            return bytes.toByteArray();
        }
    }

    private Object metadata() {
        List<Object> metadata = new ArrayList<>();
        List<Object> stack = new ArrayList<>();
        Map<String, Object> meta = new LinkedHashMap<>();

        meta.put("meta-type", "UNSPECIFIC");
        meta.put("display-name", ITEM_NAMES[RANDOM.nextInt(ITEM_NAMES.length)]);

        int enchantCount = RANDOM.nextInt(4);
        if (enchantCount > 0) {
            Map<String, Object> enchants = new LinkedHashMap<>();
            for (int index = 0; index < enchantCount; index++) {
                enchants.put(ENCHANTS[RANDOM.nextInt(ENCHANTS.length)], RANDOM.nextInt(5) + 1);
            }
            meta.put("enchants", enchants);
        }

        int loreCount = RANDOM.nextInt(3);
        if (loreCount > 0) {
            List<Object> lore = new ArrayList<>();
            for (int index = 0; index < loreCount; index++) {
                lore.add(LORE[RANDOM.nextInt(LORE.length)]);
            }
            meta.put("lore", lore);
        }

        meta.put("Damage", RANDOM.nextInt(250));
        meta.put("repair-cost", RANDOM.nextInt(40));

        stack.add(meta);
        metadata.add(stack);
        return metadata;
    }
}
