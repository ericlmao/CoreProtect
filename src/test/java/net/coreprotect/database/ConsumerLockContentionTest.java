package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * The gate for the consumer being able to write while a background job is writing too.
 *
 * <p>
 * Compacting, purging and importing all hold the write lock in bursts for as long as they run. The
 * consumer writes throughout, and has to wait its turn rather than give up: a wait costs a moment,
 * whereas giving up loses whatever it was about to record.
 * </p>
 *
 * <p>
 * Waiting is what a busy timeout is for, but it does not cover every way of asking. A transaction
 * that begins without saying it intends to write takes a snapshot of the database as it reads, and
 * if anything else commits before it gets around to writing, its snapshot can no longer be extended
 * into a write. There is no waiting for that — the write would have to be against a database that
 * has already moved on — so SQLite refuses at once and the timeout never comes into it. Saying up
 * front that the transaction will write takes the lock at the start, which is a thing that can be
 * waited for.
 * </p>
 */
class ConsumerLockContentionTest {

    private Path database;

    @BeforeEach
    void createDatabase(@TempDir Path directory) throws SQLException {
        database = directory.resolve("database.db");
        try (Connection connection = open(0); Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("CREATE TABLE co_database_lock (status INTEGER, time INTEGER)");
            statement.executeUpdate("INSERT INTO co_database_lock (rowid, status, time) VALUES (1, 0, 0)");
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER)");
        }
    }

    @AfterEach
    void removeDatabase() {
        database = null;
    }

    private Connection open(int busyTimeout) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=" + busyTimeout);
        }
        return connection;
    }

    /**
     * Reproduces the failure: a transaction that reads before it writes, with another writer in
     * between, and a busy timeout long enough that a wait would have been plenty.
     */
    @Test
    void aTransactionThatDoesNotSayItWillWriteIsRefusedRatherThanMadeToWait() throws Exception {
        try (Connection consumer = open(30000); Connection background = open(30000)) {
            try (Statement statement = consumer.createStatement()) {
                statement.executeUpdate("BEGIN TRANSACTION");
                // Reading is what fixes the snapshot the rest of the transaction sees.
                try (ResultSet results = statement.executeQuery("SELECT status FROM co_database_lock WHERE rowid=1")) {
                    assertTrue(results.next());
                }
            }

            write(background);

            try (Statement statement = consumer.createStatement()) {
                long start = System.currentTimeMillis();
                SQLException refused = assertThrows(SQLException.class,
                        () -> statement.executeUpdate("UPDATE co_database_lock SET status=1, time=1 WHERE rowid=1"),
                        "the write is refused");
                long waited = System.currentTimeMillis() - start;

                assertTrue(refused.getMessage().contains("database is locked"), "for the reason seen in the wild: " + refused.getMessage());
                assertTrue(waited < 5000, "and refused at once, without the timeout being used: waited " + waited + "ms");
            }

            rollback(consumer);
        }
    }

    /**
     * The fix: the same sequence, with the transaction saying up front that it intends to write.
     */
    @Test
    void sayingTheTransactionWillWriteMakesItWaitInstead() throws Exception {
        try (Connection consumer = open(30000); Connection background = open(30000)) {
            try (Statement statement = consumer.createStatement()) {
                statement.executeUpdate("BEGIN IMMEDIATE TRANSACTION");
                try (ResultSet results = statement.executeQuery("SELECT status FROM co_database_lock WHERE rowid=1")) {
                    assertTrue(results.next());
                }
            }

            // The other writer now has to wait for the consumer, rather than the other way round.
            try (Statement statement = consumer.createStatement()) {
                assertDoesNotThrow(() -> statement.executeUpdate("UPDATE co_database_lock SET status=1, time=1 WHERE rowid=1"),
                        "the write goes through");
                statement.executeUpdate("COMMIT");
            }

            write(background);

            try (Connection reader = open(0); Statement statement = reader.createStatement();
                    ResultSet results = statement.executeQuery("SELECT status FROM co_database_lock WHERE rowid=1")) {
                assertTrue(results.next());
                assertEquals(1, results.getInt(1), "the consumer's write is the one that stuck");
            }
        }
    }

    /**
     * Two writers taking turns, each saying it will write, both get through. This is the shape the
     * consumer and a compacting job are actually in.
     */
    @Test
    void twoWritersTakingTurnsBothGetThrough() throws Exception {
        try (Connection consumer = open(30000); Connection background = open(30000)) {
            for (int round = 0; round < 5; round++) {
                try (Statement statement = consumer.createStatement()) {
                    statement.executeUpdate("BEGIN IMMEDIATE TRANSACTION");
                    statement.executeUpdate("UPDATE co_database_lock SET time=" + round + " WHERE rowid=1");
                    statement.executeUpdate("COMMIT");
                }
                write(background);
            }

            try (Connection reader = open(0); Statement statement = reader.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_block")) {
                assertTrue(results.next());
                assertEquals(5, results.getInt(1), "every background write landed as well");
            }
        }
    }

    /**
     * The same check, but through the call the consumer actually makes, so that the fix cannot be
     * undone in the one place that matters while the tests above still pass.
     */
    @Test
    void theTransactionCoreProtectStartsSaysItWillWrite() throws Exception {
        try (Connection consumer = open(30000); Connection other = open(0)) {
            try (Statement statement = consumer.createStatement()) {
                Database.beginTransaction(statement, DatabaseType.SQLITE);

                // Holding the write lock from the start is what makes another writer wait rather than
                // the consumer fail later, so that is what is checked for here.
                try (Statement contending = other.createStatement()) {
                    SQLException refused = assertThrows(SQLException.class,
                            () -> contending.executeUpdate("BEGIN IMMEDIATE TRANSACTION"),
                            "the write lock is already taken");
                    assertTrue(refused.getMessage().contains("database is locked"), refused.getMessage());
                }

                statement.executeUpdate("UPDATE co_database_lock SET status=1 WHERE rowid=1");
                statement.executeUpdate("COMMIT");
            }
        }
    }

    /**
     * The failure seen in the wild: compacting freed several gigabytes of pages, handing them all
     * back in one call held the write lock for minutes, and everything else gave up waiting. They are
     * handed back a bounded number at a time now, which has to still return all of them.
     */
    @Test
    void everyFreedPageIsHandedBackABoundedNumberAtATime(@TempDir Path directory) throws Exception {
        // Whether pages can be handed back at all is a property of the file, fixed before it holds
        // anything, so this one is built rather than borrowed from the shared setup.
        Path file = directory.resolve("reclaim.db");
        DatabaseType previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;

        try (Connection working = DriverManager.getConnection("jdbc:sqlite:" + file)) {
            try (Statement statement = working.createStatement()) {
                statement.executeUpdate("PRAGMA page_size=4096");
                statement.executeUpdate("PRAGMA auto_vacuum=INCREMENTAL");
                statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, filler BLOB)");
            }

            byte[] filler = new byte[512];
            working.setAutoCommit(false);
            try (PreparedStatement statement = working.prepareStatement("INSERT INTO co_block (time, user, filler) VALUES (?,?,?)")) {
                for (int row = 0; row < 100000; row++) {
                    statement.setInt(1, row);
                    statement.setInt(2, row);
                    statement.setBytes(3, filler);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            working.commit();
            working.setAutoCommit(true);

            try (Statement statement = working.createStatement()) {
                statement.executeUpdate("DELETE FROM co_block");
            }

            long freed = freePages(working);
            assertTrue(freed > 5000, "there are plenty of pages to hand back: " + freed);

            Database.reclaimFreePages(working);

            assertEquals(0, freePages(working), "every one of them was handed back");
        }
        finally {
            ConfigHandler.databaseType = previousType;
        }
    }

    private static long freePages(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("PRAGMA freelist_count")) {
            assertTrue(results.next());
            return results.getLong(1);
        }
    }

    private static void write(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_block (time, user) VALUES (1, 1)");
        }
    }

    private static void rollback(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("ROLLBACK");
        }
        catch (SQLException exception) {
            // Nothing to roll back.
        }
    }
}
