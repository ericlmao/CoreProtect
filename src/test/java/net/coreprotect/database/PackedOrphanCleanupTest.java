package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * The nightly cleanup removes links to rows that are gone. A row that has been packed is not gone,
 * and with packing happening minutes after a row is written, nearly every row a link points at is
 * packed.
 */
class PackedOrphanCleanupTest {

    private static final long DAY = 86400L;

    private Connection connection;
    private DatabaseType previousType;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        RealisticActivity.createBlockTable(connection);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_entity (time INTEGER, data BLOB)");
            statement.executeUpdate("CREATE TABLE co_entity_spawn (time INTEGER, block_rowid INTEGER, kill_rowid INTEGER, removed INTEGER)");
            statement.executeUpdate("CREATE TABLE co_entity_container (time INTEGER, user INTEGER, entity_spawn_rowid INTEGER NOT NULL, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, amount INTEGER, metadata BLOB, action INTEGER, rolled_back INTEGER)");
            statement.executeUpdate("CREATE TABLE co_entity_interaction (time INTEGER, user INTEGER, entity_spawn_rowid INTEGER NOT NULL, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, action INTEGER, metadata BLOB, rolled_back INTEGER)");
        }
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void linksToPackedRowsSurviveTheCleanup() throws Exception {
        long time = ((System.currentTimeMillis() / 1000L) / DAY - 1) * DAY + 60;
        try (Statement statement = connection.createStatement()) {
            for (int row = 0; row < 300; row++) {
                statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,action,rolled_back) VALUES (" + (time + row) + ",1,1," + row + ",64,0,5,0,1,0)");
            }
            // 1: its spawn was a block row that is about to be packed.
            statement.executeUpdate("INSERT INTO co_entity_spawn (time,block_rowid,kill_rowid,removed) VALUES (" + time + ",50,NULL,1)");
            // 2: its only history is an entity container row that is about to be packed.
            statement.executeUpdate("INSERT INTO co_entity_spawn (time,block_rowid,kill_rowid,removed) VALUES (" + time + ",NULL,NULL,1)");
            // 3: nothing refers to it any more.
            statement.executeUpdate("INSERT INTO co_entity_spawn (time,block_rowid,kill_rowid,removed) VALUES (" + time + ",NULL,NULL,1)");
            // 4: its block row was purged, not packed.
            statement.executeUpdate("INSERT INTO co_entity_spawn (time,block_rowid,kill_rowid,removed) VALUES (" + time + ",999999,NULL,0)");
            for (int row = 0; row < 20; row++) {
                statement.executeUpdate("INSERT INTO co_entity_container (time,user,entity_spawn_rowid,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (" + (time + row) + ",1,2,1,0,64,0,5,0,1,0,0)");
            }
            statement.executeUpdate("INSERT INTO co_entity_container (time,user,entity_spawn_rowid,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (" + (time + 5000) + ",1,4,1,0,64,0,5,0,1,0,0)");
        }

        ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        assertEquals(1, count("SELECT COUNT(*) FROM co_block"), "the block rows are packed");
        assertEquals(1, count("SELECT COUNT(*) FROM co_entity_container"), "the entity rows are packed");

        PurgeExecutor.removeOrphanedRows(connection, Connection::prepareStatement, () -> {
        });

        assertEquals(50, count("SELECT block_rowid FROM co_entity_spawn WHERE rowid = 1"), "a link to a packed block row is kept");
        assertEquals(1, count("SELECT COUNT(*) FROM co_entity_spawn WHERE rowid = 2"), "an entity whose history is packed is kept");
        assertEquals(0, count("SELECT COUNT(*) FROM co_entity_spawn WHERE rowid = 3"), "an entity nothing refers to is still removed");
        assertEquals(0, count("SELECT COUNT(*) FROM co_entity_spawn WHERE rowid = 4 AND block_rowid IS NOT NULL"), "a link to a purged row is still cleared");
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
