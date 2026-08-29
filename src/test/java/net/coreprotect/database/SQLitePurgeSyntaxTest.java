package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Verifies the SQLite behaviour CoreProtect relies on: the pragmas applied to every pooled
 * connection, and the batched in-place delete used by both the manual and the automatic purge.
 */
class SQLitePurgeSyntaxTest {

    @Test
    void appliesPragmasToEveryPooledConnection(@TempDir Path directory) throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sqlite:" + directory.resolve("database.db"));
        config.setMaximumPoolSize(3);
        config.setMaxLifetime(0);
        config.addDataSourceProperty("journal_mode", "WAL");
        config.addDataSourceProperty("synchronous", "NORMAL");
        config.addDataSourceProperty("busy_timeout", "30000");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            // The first connection creates the database file; later connections reuse it.
            for (int attempt = 0; attempt < 3; attempt++) {
                try (Connection connection = dataSource.getConnection()) {
                    assertEquals("wal", pragma(connection, "journal_mode"));
                    assertEquals("1", pragma(connection, "synchronous"));
                    assertEquals("30000", pragma(connection, "busy_timeout"));
                }
            }
        }
    }

    @Test
    void deletesRowsInBatchesWithoutCopyingTheDatabase(@TempDir Path directory) throws SQLException {
        Path database = directory.resolve("database.db");

        try (Connection connection = open(database)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE co_block (time INTEGER, wid INTEGER)");
            }

            try (Statement statement = connection.createStatement()) {
                for (int i = 0; i < 25; i++) {
                    statement.executeUpdate("INSERT INTO co_block (time, wid) VALUES (" + i + ", 1)");
                }
            }

            String query = "DELETE FROM co_block WHERE rowid IN(SELECT rowid FROM co_block WHERE time < '20' AND time >= '0' LIMIT 10)";
            int first;
            int second;
            int third;
            try (Statement statement = connection.createStatement()) {
                first = statement.executeUpdate(query);
                second = statement.executeUpdate(query);
                third = statement.executeUpdate(query);
            }

            assertEquals(10, first);
            assertEquals(10, second);
            assertEquals(0, third);
            assertEquals(5, count(connection, "SELECT COUNT(*) FROM co_block"));
        }

        assertTrue(database.toFile().exists());
        assertFalse(directory.resolve("database.db.tmp").toFile().exists(), "a purge must never create a second database file");
    }

    private static Connection open(Path database) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("journal_mode", "WAL");
        return java.sql.DriverManager.getConnection("jdbc:sqlite:" + database, properties);
    }

    private static String pragma(Connection connection, String pragma) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("PRAGMA " + pragma)) {
            return results.next() ? results.getString(1) : null;
        }
    }

    private static int count(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getInt(1) : -1;
        }
    }
}
