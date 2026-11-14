package net.coreprotect.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.ColdRollupTask;
import net.coreprotect.database.Database;
import net.coreprotect.database.DatabaseType;
import net.coreprotect.database.SQLiteColdIndex;
import net.coreprotect.database.SQLiteSchema;

/**
 * Plugins asking through the API see a place's or a player's whole history, packed or not. Activity
 * is packed minutes after it is logged, so an API that read only the live tables would answer with
 * almost nothing.
 */
class PackedApiLookupTest {

    private static final long DAY = 86400L;
    private static final int X = 40;
    private static final int Y = 70;
    private static final int Z = -12;

    private DatabaseType previousType;
    private boolean previousApi;
    private World world;

    @BeforeAll
    static void prepareServer() {
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = Mockito.mock(org.bukkit.Server.class);
            ConsoleCommandSender console = Mockito.mock(ConsoleCommandSender.class);
            Mockito.when(server.getConsoleSender()).thenReturn(console);
            Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
    }

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws Exception {
        previousType = ConfigHandler.databaseType;
        previousApi = Config.getGlobal().API_ENABLED;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.storagePath = directory;
        ConfigHandler.sqlite = "database.db";
        ConfigHandler.sqliteDataSource = null;
        ConfigHandler.worlds.put("world", 1);
        ConfigHandler.worldsReversed.put(1, "world");
        ConfigHandler.playerIdCache.put("builder", 3);
        ConfigHandler.playerIdCacheReversed.put(3, "builder");
        Config.getGlobal().API_ENABLED = true;
        SQLiteColdIndex.invalidate();

        world = Mockito.mock(World.class);
        Mockito.when(world.getName()).thenReturn("world");

        Path database = directory.resolve("database.db");
        Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + database), DatabaseType.SQLITE, false);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database)) {
            try (Statement statement = connection.createStatement()) {
                SQLiteSchema.applyFileSettings(statement);
                statement.executeUpdate("PRAGMA journal_mode=WAL");
                SQLiteSchema.createTables("co_", statement);
                statement.executeUpdate("INSERT INTO co_user (time,user,uuid) VALUES (1,'other',NULL),(1,'someone',NULL),(1,'builder',NULL)");
            }

            long yesterday = ((System.currentTimeMillis() / 1000L) / DAY - 1) * DAY + 100;
            connection.setAutoCommit(false);
            try (PreparedStatement block = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,3,1,?,?,?,5,0,NULL,NULL,?,0)");
                    PreparedStatement session = connection.prepareStatement("INSERT INTO co_session (time,user,wid,x,y,z,action) VALUES (?,3,1,0,64,0,?)")) {
                for (int row = 0; row < 2000; row++) {
                    boolean here = row % 10 == 0;
                    block.setLong(1, yesterday + row);
                    block.setInt(2, here ? X : X + 1 + row % 50);
                    block.setInt(3, Y);
                    block.setInt(4, Z);
                    block.setInt(5, row % 2);
                    block.addBatch();
                    if (row % 20 == 0) {
                        session.setLong(1, yesterday + row);
                        session.setInt(2, (row / 20) % 2);
                        session.addBatch();
                    }
                }
                block.executeBatch();
                session.executeBatch();
            }
            connection.commit();
            connection.setAutoCommit(true);

            ColdRollupTask.rollUp(connection, () -> {
            }, System.currentTimeMillis() / 1000L);
        }
    }

    @AfterEach
    void closeDatabase() {
        SQLiteColdIndex.invalidate();
        ConfigHandler.databaseType = previousType;
        Config.getGlobal().API_ENABLED = previousApi;
    }

    @Test
    void aBlockLookupSeesPackedHistory() {
        Block block = Mockito.mock(Block.class);
        Mockito.when(block.getX()).thenReturn(X);
        Mockito.when(block.getY()).thenReturn(Y);
        Mockito.when(block.getZ()).thenReturn(Z);
        Mockito.when(block.getWorld()).thenReturn(world);

        assertEquals(200, BlockAPI.performLookup(block, 0).size(), "every change to the block, packed or live");
        assertEquals(200, BlockAPI.performLookup(block, LookupOptions.builder().build()).size());
        assertEquals(7, BlockAPI.performLookup(block, LookupOptions.builder().limit(0, 7).build()).size());
    }

    @Test
    void aSessionLookupSeesPackedHistory() {
        assertEquals(100, SessionLookup.performLookup("builder", 0).size());
        assertEquals(100, SessionLookup.performLookup(LookupOptions.builder().user("builder").build()).size());
        Location near = new Location(world, 0, 64, 0);
        assertEquals(100, SessionLookup.performLookup(LookupOptions.builder().user("builder").radius(near, 5).build()).size(), "narrowed by place as well");
    }

    @Test
    void lookupsLeaveNothingOpen() throws Exception {
        SessionLookup.performLookup(LookupOptions.builder().user("builder").build());
        // An API lookup that never ended would keep packing waiting for it indefinitely.
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + ConfigHandler.storagePath.resolve("database.db"))) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,action,rolled_back) VALUES (" + (System.currentTimeMillis() / 1000L) + ",3,1,0,0,0,5,0,1,0)");
            }
            long started = System.nanoTime();
            ColdRollupTask.rollUp(connection, () -> {
            }, System.currentTimeMillis() / 1000L + 1);
            assertEquals(true, (System.nanoTime() - started) < 1_500_000_000L, "packing was not kept waiting");
        }
    }
}
