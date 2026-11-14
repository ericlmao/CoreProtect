package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.zaxxer.hikari.HikariDataSource;

import net.coreprotect.config.ConfigHandler;

/**
 * SQLite's temporary files are written beside the database, not to the system's temporary
 * directory, on every kind of connection the plugin opens.
 *
 * <p>
 * SQLite removes a temporary file from its directory the moment it opens it, so the file is found
 * through the open files of the process instead, where it keeps the name it was created with.
 * </p>
 */
class SQLiteTemporaryStorageTest {

    @TempDir
    Path directory;

    private Path previousPath;
    private String previousFile;
    private DatabaseType previousType;
    private HikariDataSource previousSource;
    private Path storage;

    @BeforeEach
    void pointAtStorage() throws IOException {
        previousPath = ConfigHandler.storagePath;
        previousFile = ConfigHandler.sqlite;
        previousType = ConfigHandler.databaseType;
        previousSource = ConfigHandler.sqliteDataSource;
        storage = Files.createDirectories(directory.resolve("storage"));
        ConfigHandler.storagePath = storage;
        ConfigHandler.sqlite = "database.db";
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.sqliteDataSource = null;
    }

    @AfterEach
    void restore() throws SQLException {
        // The setting belongs to the whole library, so it is put back for the tests that follow.
        try (Connection connection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:"); Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA temp_store_directory = ''");
        }
        ConfigHandler.storagePath = previousPath;
        ConfigHandler.sqlite = previousFile;
        ConfigHandler.databaseType = previousType;
        ConfigHandler.sqliteDataSource = previousSource;
    }

    @Test
    void aConnectionOpenedDirectlyWritesTemporaryFilesBesideTheDatabase() throws Exception {
        try (Connection connection = Database.getConnection(true, 0)) {
            assertEquals(storage.toAbsolutePath().toString(), temporaryDirectory(connection));
            assertTemporaryFileIn(connection);
        }
    }

    @Test
    void aPooledConnectionWritesTemporaryFilesBesideTheDatabase() throws Exception {
        Method create = ConfigHandler.class.getDeclaredMethod("createSQLiteDataSource");
        create.setAccessible(true);
        try (HikariDataSource pool = (HikariDataSource) create.invoke(null); Connection connection = pool.getConnection()) {
            assertEquals(storage.toAbsolutePath().toString(), temporaryDirectory(connection));
            assertTemporaryFileIn(connection);
        }
    }

    private static String temporaryDirectory(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("PRAGMA temp_store_directory")) {
            return results.next() ? results.getString(1) : null;
        }
    }

    /**
     * Writes a temporary table too large for the page cache, so SQLite has to open a file for it,
     * and finds that file among the process's open files while it is open.
     */
    private void assertTemporaryFileIn(Connection connection) throws Exception {
        String listing;
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA temp_store=FILE");
            statement.executeUpdate("PRAGMA temp.cache_size=16");
            statement.executeUpdate("CREATE TEMP TABLE spill (value BLOB)");
            statement.executeUpdate("SAVEPOINT spilling");
            statement.executeUpdate("WITH RECURSIVE counter(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 20000) INSERT INTO spill SELECT randomblob(1000) FROM counter");
            listing = openFiles();
            statement.executeUpdate("RELEASE spilling");
            statement.executeUpdate("DROP TABLE spill");
        }
        assumeTrue(listing != null, "the open files of the process can be listed");
        String expected = storage.toRealPath().toString();
        boolean found = false;
        for (String line : listing.split("\n")) {
            if (line.contains("etilqs_")) {
                assertTrue(line.contains(expected + "/etilqs_"), "a temporary file outside the storage directory: " + line);
                found = true;
            }
        }
        assertTrue(found, "SQLite wrote a temporary file while the table was built");
    }

    private static String openFiles() {
        try {
            Process process = new ProcessBuilder("lsof", "-p", Long.toString(ProcessHandle.current().pid())).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            return process.waitFor() == 0 ? output : null;
        }
        catch (IOException exception) {
            return null;
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
