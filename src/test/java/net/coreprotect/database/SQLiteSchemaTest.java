package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
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

/** Covers how an existing database is treated when the storage layout changes. */
class SQLiteSchemaTest {

    private String previousPath;
    private String previousFile;

    @BeforeEach
    void configure(@TempDir Path directory) {
        previousPath = ConfigHandler.path;
        previousFile = ConfigHandler.sqlite;
        ConfigHandler.path = directory.toString() + "/";
        ConfigHandler.sqlite = "database.db";
        ConfigHandler.prefix = "co_";
    }

    @AfterEach
    void restore() {
        ConfigHandler.path = previousPath;
        ConfigHandler.sqlite = previousFile;
    }

    @Test
    void leavesAMissingDatabaseAlone() throws SQLException {
        assertNull(SQLiteSchema.prepareDatabaseFile());
    }

    @Test
    void leavesACurrentDatabaseAlone() throws SQLException {
        createDatabase(true);
        assertNull(SQLiteSchema.prepareDatabaseFile());
        assertTrue(Files.exists(SQLiteSchema.databaseFile()));
    }

    @Test
    void setsAPreviousLayoutAsideInsteadOfConvertingIt() throws Exception {
        createDatabase(false);
        Path archived = SQLiteSchema.prepareDatabaseFile();

        assertNotNull(archived, "a database from the previous layout is renamed");
        assertTrue(Files.exists(archived), "the previous database is kept, not deleted");
        assertTrue(archived.getFileName().toString().contains(".v1-"));
        assertTrue(Files.exists(SQLiteSchema.databaseFile()), "a fresh database file takes its place");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + SQLiteSchema.databaseFile());
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")) {
            assertTrue(results.next());
            assertEquals(0, results.getInt(1), "the new database starts out empty");
        }

        // The renamed file is the original one, so no copy was ever made.
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + archived);
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_block")) {
            assertTrue(results.next());
            assertEquals(1, results.getInt(1));
        }
    }

    @Test
    void refusesADatabaseFromANewerLayout() throws Exception {
        createDatabase(true);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + SQLiteSchema.databaseFile()); Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE co_schema SET value = '99' WHERE name = 'schema_version'");
        }

        assertThrows(SQLException.class, SQLiteSchema::prepareDatabaseFile);
    }

    private void createDatabase(boolean currentLayout) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + SQLiteSchema.databaseFile()); Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, wid INTEGER);");
            statement.executeUpdate("INSERT INTO co_block (time, wid) VALUES (1, 1);");
            if (currentLayout) {
                SQLiteSchema.createTables("co_", statement);
            }
        }
    }
}
