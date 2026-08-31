package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * What happens when the driver and the database disagree about whether a transaction is open.
 *
 * <p>
 * Transactions are started two ways in this code base. The consumer runs BEGIN and COMMIT as
 * ordinary statements, because saying up front that a transaction will write is something only the
 * statement can say; compressing runs them through the driver. The driver keeps its own record of
 * whether a transaction is open, and a COMMIT it did not issue does not reach that record. From then
 * on the connection is in a state where asking for a transaction does nothing, every write goes in
 * on its own, and the commit at the end fails.
 * </p>
 *
 * <p>
 * That is what took a compact down on the server, four seconds in:
 * <code>[SQLITE_ERROR] SQL error or missing database (cannot commit - no transaction is active)</code>,
 * raised from the point where a segment had been written and the rows it replaced had been deleted.
 * The delete had already committed on its own by then, which is the part that matters: the failure
 * was not only noisy, it left the compact half done.
 * </p>
 */
class TransactionStateTest {

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

    /**
     * Leaves the connection in the state the server's was in: the driver believes a transaction is
     * open, the database has none. This is what a COMMIT run as a statement does, and the consumer
     * runs COMMIT as a statement every time it writes.
     */
    private void desynchronise() throws SQLException {
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (1,1,1,0,64,0,10,0,NULL,NULL,0,0)");
            statement.executeUpdate("COMMIT");
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_block");
        }
        assertFalse(connection.getAutoCommit(), "the driver still believes it is inside a transaction");
    }

    @Test
    void aCommitRunAsAStatementLeavesTheDriverBelievingATransactionIsOpen() throws Exception {
        desynchronise();

        // The failure this exists to prevent, raised by the plain sequence the compact used to run.
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (2,1,1,0,64,0,10,0,NULL,NULL,0,0)");
        }
        SQLException failure = assertThrows(SQLException.class, () -> connection.commit());
        assertTrue(failure.getMessage().contains("no transaction is active"), failure.getMessage());

        // And the write went in anyway, on its own, which is what makes it worse than a clean stop.
        assertEquals(1, count("SELECT COUNT(*) FROM co_block"));
    }

    @Test
    void aUnitOfWorkStartsFromAKnownStateAndCommits() throws Exception {
        desynchronise();

        Database.inTransaction(connection, () -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (3,1,1,0,64,0,10,0,NULL,NULL,0,0)");
            }
        });

        assertEquals(1, count("SELECT COUNT(*) FROM co_block"));
        assertTrue(connection.getAutoCommit(), "the connection is left in auto-commit for whatever runs next");
    }

    @Test
    void aFailedUnitOfWorkTakesBackEverythingItDid() throws Exception {
        desynchronise();

        assertThrows(SQLException.class, () -> Database.inTransaction(connection, () -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (4,1,1,0,64,0,10,0,NULL,NULL,0,0)");
            }
            throw new SQLException("stopped part way");
        }));

        assertEquals(0, count("SELECT COUNT(*) FROM co_block"), "the row went back");
        assertTrue(connection.getAutoCommit(), "and the connection is usable again");
    }

    @Test
    void packingSucceedsOnADesynchronisedConnection() throws Exception {
        long now = System.currentTimeMillis() / 1000L;
        long oldTime = ((now - (30 * DAY)) / DAY) * DAY + 60;
        insertBlocks(oldTime, 500);

        desynchronise2();

        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });

        // The newest row stays behind so the table carries on numbering from it, which is what
        // keeps live writes above everything a segment already holds.
        assertEquals(499, sealed, "every old row but the newest was packed away");
        assertEquals(1, count("SELECT COUNT(*) FROM co_block"), "and removed from the live table");
        assertEquals(1, count("SELECT COUNT(*) FROM co_segment"));
    }

    /** The same disagreement, arranged without disturbing the rows that are to be packed. */
    private void desynchronise2() throws SQLException {
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_schema (name, value) VALUES ('transaction_state_test', '1')");
            statement.executeUpdate("COMMIT");
        }
        assertFalse(connection.getAutoCommit(), "the driver still believes it is inside a transaction");
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
