package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * Packing runs while the server is up, so it runs while lookups and rollbacks do. A lookup must see
 * every row exactly once however the two overlap, and a change to a row that has just been packed
 * must land beside its segment rather than go missing.
 */
class PackingConsistencyTest {

    private static final long DAY = 86400L;

    private Path database;
    private Connection connection;
    private DatabaseType previousType;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        database = directory.resolve("database.db");
        connection = open();
        RealisticActivity.createBlockTable(connection);
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
    void packingWaitsForALookupInProgress() throws Exception {
        insert(yesterday(), 3000);
        insert(now() - 5, 1);

        CountDownLatch looking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Long> lookup = CompletableFuture.supplyAsync(() -> {
            try (Connection reader = open()) {
                SQLiteColdIndex.beginLookup(0, 0);
                try {
                    String source = SQLiteColdIndex.sourceExpression(reader, "block", 0, null);
                    looking.countDown();
                    // Everything the lookup reads after this point must agree with what it chose
                    // above, however long it takes.
                    release.await(10, TimeUnit.SECONDS);
                    return count(reader, "SELECT COUNT(*) FROM " + source);
                }
                finally {
                    SQLiteColdIndex.endLookup(reader);
                }
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });
        assertTrue(looking.await(10, TimeUnit.SECONDS));

        CompletableFuture<Long> packing = CompletableFuture.supplyAsync(() -> {
            try (Connection writer = open()) {
                return ColdRollupTask.rollUp(writer, () -> {
                }, now());
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });

        Thread.sleep(400);
        assertFalse(packing.isDone(), "packing does not move rows out from under the lookup");
        release.countDown();

        assertEquals(3001, lookup.get(10, TimeUnit.SECONDS), "the lookup saw every row, once");
        assertEquals(3000, packing.get(10, TimeUnit.SECONDS), "packing went ahead once the lookup finished");
        assertEquals(1, count(connection, "SELECT COUNT(*) FROM co_block"));

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            assertEquals(3001, count(connection, "SELECT COUNT(*) FROM " + SQLiteColdIndex.sourceExpression(connection, "block", 0, null)),
                    "a lookup afterwards sees every row, once");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void packingGivesWayToALookupThatRunsLong() throws Exception {
        insert(yesterday(), 2000);
        insert(now() - 5, 1);

        CountDownLatch looking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> lookup = CompletableFuture.runAsync(() -> {
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                looking.countDown();
                release.await(20, TimeUnit.SECONDS);
            }
            catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            finally {
                SQLiteColdIndex.endLookup(null);
            }
        });
        assertTrue(looking.await(10, TimeUnit.SECONDS));

        try (Connection writer = open()) {
            long packed = ColdRollupTask.rollUp(writer, () -> {
            }, now());
            assertEquals(0, packed, "packing stops rather than holding the database while it waits");
        }
        assertEquals(2001, count(connection, "SELECT COUNT(*) FROM co_block"), "nothing was moved");

        release.countDown();
        lookup.get(10, TimeUnit.SECONDS);
        assertEquals(2000, ColdRollupTask.rollUp(connection, () -> {
        }, now()), "and it is packed on the next run");
    }

    @Test
    void aRollbackWhileRowsAreBeingPackedIsNotLost() throws Exception {
        insert(yesterday(), 3000);
        insert(now() - 5, 1);

        // A lookup holds packing back at the point where its rows are already read but not yet
        // written, which is when a rollback can still change them in the live table.
        CountDownLatch looking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> lookup = CompletableFuture.runAsync(() -> {
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                looking.countDown();
                release.await(20, TimeUnit.SECONDS);
            }
            catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            finally {
                SQLiteColdIndex.endLookup(null);
            }
        });
        assertTrue(looking.await(10, TimeUnit.SECONDS));

        CompletableFuture<Long> packing = CompletableFuture.supplyAsync(() -> {
            try (Connection writer = open()) {
                return ColdRollupTask.rollUp(writer, () -> {
                }, now());
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });
        Thread.sleep(600);
        try (Statement statement = connection.createStatement()) {
            assertEquals(1, statement.executeUpdate("UPDATE co_block SET rolled_back = 1 WHERE rowid = 77"), "the row is still live when the rollback reaches it");
        }
        release.countDown();
        lookup.get(10, TimeUnit.SECONDS);
        assertEquals(3000, packing.get(10, TimeUnit.SECONDS));

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            assertEquals(1, count(connection, "SELECT rolled_back FROM " + source + " WHERE rowid = 77"), "the rollback survives the packing");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void theRollbackStateOfAPackedRowIsRecordedBesideItsSegment() throws Exception {
        insert(yesterday(), 500);
        insert(now() - 5, 1);
        ColdRollupTask.rollUp(connection, () -> {
        }, now());

        long packedRow = 42;
        long liveRow = count(connection, "SELECT MAX(rowid) FROM co_block");
        assertTrue(Database.recordPackedRollbackState(connection, "block", 1, packedRow), "a packed row has its state recorded");
        assertFalse(Database.recordPackedRollbackState(connection, "block", 1, liveRow), "a live row is left for the live table");

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            assertEquals(1, count(connection, "SELECT rolled_back FROM " + source + " WHERE rowid = " + packedRow), "a lookup reads the recorded state");
            assertEquals(0, count(connection, "SELECT COUNT(*) FROM " + source + " WHERE rolled_back = 1 AND rowid <> " + packedRow));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void theRolledBackUpdateFindsRowsPackedByAnotherConnection() throws Exception {
        insert(yesterday(), 400);
        insert(now() - 5, 1);

        // Packed on another connection: the update below has to learn that from the database, as a
        // rollback running beside the packing would.
        try (Connection writer = open()) {
            ColdRollupTask.rollUp(writer, () -> {
            }, now());
        }

        try (Statement statement = connection.createStatement()) {
            Database.performRolledBackUpdateChecked(statement, 1, java.util.Arrays.asList(10L, 11L, 12L), 0);
        }
        assertEquals(3, count(connection, "SELECT COUNT(*) FROM co_cold_flag WHERE rolled_back = 1"));
    }

    private Connection open() throws SQLException {
        Connection opened = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = opened.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=10000");
        }
        return opened;
    }

    private static long now() {
        return System.currentTimeMillis() / 1000L;
    }

    private static long yesterday() {
        return ((now() / DAY) - 1) * DAY + 30;
    }

    private void insert(long firstTime, int rows) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,5,0,NULL,NULL,1,0)")) {
            for (int row = 0; row < rows; row++) {
                statement.setLong(1, firstTime + row / 20);
                statement.setInt(2, 1 + row % 4);
                statement.setInt(3, row % 200);
                statement.setInt(4, (row * 3) % 200);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(autoCommit);
    }

    private static long count(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
