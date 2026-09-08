package net.coreprotect.database;

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
 * Reading compressed rows from inside a transaction somebody else started.
 *
 * <p>
 * The consumer writes in batches, and it opens each batch with BEGIN IMMEDIATE TRANSACTION run as
 * an ordinary statement, so that the write lock is taken up front. The driver never sees that, and
 * goes on reporting the connection as being in auto-commit. Part way through such a batch the
 * consumer can look up who placed a block, which reads compressed storage on the very same
 * connection.
 * </p>
 *
 * <p>
 * The read used to ask the driver for a transaction of its own before building its scratch table.
 * On an idle connection that was merely a way of avoiding a commit per batch of rows; inside the
 * consumer's transaction the driver answered with BEGIN, and SQLite refused:
 * <code>[SQLITE_ERROR] SQL error or missing database (cannot start a transaction within a transaction)</code>.
 * Had it been allowed, the commit at the end of the read would have been worse still, ending the
 * consumer's transaction half way through the rows it was writing.
 * </p>
 *
 * <p>
 * A savepoint says the same thing without any of that: on an idle connection it opens a transaction
 * and releasing it commits, and inside a transaction it simply nests and folds back into the one
 * already open.
 * </p>
 */
class ColdReadInsideTransactionTest {

    private static final long DAY = 86400L;

    private Path databaseFile;
    private Connection connection;
    private DatabaseType previousType;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        databaseFile = directory.resolve("database.db");
        connection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
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
    void readsColdRowsInsideAStatementStartedTransaction() throws Exception {
        sealOldRows();

        // Exactly what the consumer does before it writes a batch.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("BEGIN IMMEDIATE TRANSACTION");
        }
        insertBlocks(System.currentTimeMillis() / 1000L, 1);

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertTrue(source.startsWith("("), "the compressed rows were read rather than skipped");
            assertEquals(501, count("SELECT COUNT(*) FROM " + source + " WHERE wid = 1"), "every row is visible, compressed and live");
            assertTrue(connection.getAutoCommit(), "the read left the driver's idea of the connection alone");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        // Still the consumer's transaction to end, and nothing of it has been written away yet.
        assertEquals(1, committedBlocks(), "the read committed nothing of the batch it interrupted");
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("COMMIT TRANSACTION");
        }
        assertEquals(2, committedBlocks(), "the live row went in with the consumer's transaction");
    }

    @Test
    void aFailedReadInsideATransactionLeavesTheTransactionOpen() throws Exception {
        sealOldRows();

        // A segment whose payload no longer decodes, which is what a truncated or damaged file
        // looks like from here.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE co_segment SET payload = X'00112233445566778899'");
        }

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("BEGIN IMMEDIATE TRANSACTION");
        }
        insertBlocks(System.currentTimeMillis() / 1000L, 1);

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            // A payload that will not decode fails as a decoding error rather than an SQL one, so
            // it comes back out of the lookup instead of falling back to live rows.
            assertThrows(Exception.class, () -> SQLiteColdIndex.sourceExpression(connection, "block", 1, null));
            assertTrue(connection.getAutoCommit(), "the failed read left the driver's idea of the connection alone");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        // The failure took back what the read had written and nothing else. The consumer's
        // transaction is still open, still holds its own row, and still ends on its own terms.
        assertEquals(2, count("SELECT COUNT(*) FROM co_block"), "the row the consumer wrote is still there");
        assertEquals(1, committedBlocks(), "and nothing of the batch has been committed");
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("COMMIT TRANSACTION");
        }
        assertEquals(2, committedBlocks(), "the live row survived the failed read");
    }

    @Test
    void readsColdRowsOnAnIdleConnection() throws Exception {
        sealOldRows();
        insertBlocks(System.currentTimeMillis() / 1000L, 1);

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            assertTrue(source.startsWith("("), "the compressed rows were read rather than skipped");
            assertEquals(501, count("SELECT COUNT(*) FROM " + source + " WHERE wid = 1"), "every row is visible, compressed and live");
            assertTrue(connection.getAutoCommit(), "and the connection is left ready for whatever runs next");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    /**
     * Writes five hundred rows old enough to be packed away, then packs them.
     */
    private void sealOldRows() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 500);

        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);

        assertEquals(1, count("SELECT COUNT(*) FROM co_segment"), "the old rows were packed into a segment");
    }

    /**
     * Counts the rows another connection can see, which is only what has been committed.
     */
    private long committedBlocks() throws SQLException {
        try (Connection reader = DriverManager.getConnection("jdbc:sqlite:" + databaseFile);
                Statement statement = reader.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_block")) {
            results.next();
            return results.getLong(1);
        }
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            results.next();
            return results.getLong(1);
        }
    }

    private void insertBlocks(long baseTime, int rows) throws SQLException {
        String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int index = 0; index < rows; index++) {
                statement.setLong(1, baseTime + index);
                statement.setInt(2, 1 + (index % 25));
                statement.setInt(3, 1);
                statement.setInt(4, index % 200);
                statement.setInt(5, 64);
                statement.setInt(6, index % 200);
                statement.setInt(7, 10 + (index % 40));
                statement.setInt(8, 0);
                statement.setBytes(9, new byte[] { (byte) index, 1, 2, 3 });
                statement.setNull(10, java.sql.Types.BLOB);
                statement.setInt(11, index % 2);
                statement.setInt(12, 0);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }
}
