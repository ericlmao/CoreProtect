package net.coreprotect.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LegacyStorageMigrationTest {

    private static final Logger LOG = Logger.getLogger(LegacyStorageMigrationTest.class.getName());

    @Test
    void doesNothingOnAnInstallationThatHasNeverRun(@TempDir Path server) throws IOException {
        Path dataDirectory = Files.createDirectories(server.resolve("plugins").resolve("CoreProtect"));

        LegacyStorageMigration.run(dataDirectory, LOG);

        assertFalse(Files.exists(server.resolve("storage")));
    }

    @Test
    void doesNothingWhenThePluginFolderIsNotThereYet(@TempDir Path server) {
        LegacyStorageMigration.run(server.resolve("plugins").resolve("CoreProtect"), LOG);

        assertFalse(Files.exists(server.resolve("storage")));
    }

    @Test
    void movesTheWholeLegacyLayoutIntoStorage(@TempDir Path server) throws IOException {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");
        write(dataDirectory.resolve("database.db"), "rows");
        write(dataDirectory.resolve("database.db-wal"), "committed");
        write(dataDirectory.resolve("database.db.tmp"), "purge-scratch");
        write(dataDirectory.resolve("database.db.v1-20240101-000000"), "upstream");
        write(dataDirectory.resolve("database.duckdb"), "columns");
        write(dataDirectory.resolve("database.duckdb.wal"), "duck-committed");
        write(dataDirectory.resolve("database.duckdb.tmp").resolve("spill"), "scratch");
        write(dataDirectory.resolve("old.db"), "import-source");
        write(dataDirectory.resolve("old.db.imported-20240101-000000"), "imported");
        write(dataDirectory.resolve(".clickhouse-writer"), "writer");
        write(dataDirectory.resolve(".language"), "CACHED: yes");

        LegacyStorageMigration.run(dataDirectory, LOG);

        Path storage = server.resolve("storage").resolve("CoreProtect");
        assertEquals("rows", read(storage.resolve("database.db")));
        assertEquals("committed", read(storage.resolve("database.db-wal")));
        assertEquals("purge-scratch", read(storage.resolve("database.db.tmp")));
        assertEquals("upstream", read(storage.resolve("database.db.v1-20240101-000000")));
        assertEquals("columns", read(storage.resolve("database.duckdb")));
        assertEquals("duck-committed", read(storage.resolve("database.duckdb.wal")));
        assertEquals("scratch", read(storage.resolve("database.duckdb.tmp").resolve("spill")));
        assertEquals("import-source", read(storage.resolve("old.db")));
        assertEquals("imported", read(storage.resolve("old.db.imported-20240101-000000")));
        assertEquals("writer", read(storage.resolve(".clickhouse-writer")));
        assertEquals("CACHED: yes", read(storage.resolve(".language")));
        assertEquals(List.of(), names(dataDirectory));
    }

    @Test
    void leavesConfigurationInThePluginFolder(@TempDir Path server) throws IOException {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");
        write(dataDirectory.resolve("config.yml"), "# CoreProtect Config\nverbose: true");
        write(dataDirectory.resolve("language.yml"), "# CoreProtect Language File (en)");
        write(dataDirectory.resolve("blacklist.txt"), "notch");
        write(dataDirectory.resolve("world_nether.yml"), "block-place: false");

        LegacyStorageMigration.run(dataDirectory, LOG);

        assertEquals(List.of("blacklist.txt", "config.yml", "language.yml", "world_nether.yml"), names(dataDirectory));
        assertFalse(Files.exists(server.resolve("storage")));
    }

    @Test
    void splitsTheDatabaseSettingsOutOfTheConfiguration(@TempDir Path server) throws IOException {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");
        write(dataDirectory.resolve("config.yml"),
                "# CoreProtect Config\ndatabase-type: mysql\nmysql-password: hunter2\nverbose: true");

        LegacyStorageMigration.run(dataDirectory, LOG);

        Path databaseConfiguration = server.resolve("storage").resolve("CoreProtect").resolve("database.yml");
        assertTrue(read(databaseConfiguration).contains("mysql-password: hunter2"));
        assertFalse(read(dataDirectory.resolve("config.yml")).contains("hunter2"));
        assertTrue(read(dataDirectory.resolve("config.yml")).contains("verbose: true"));
    }

    @Test
    void keepsTheStorageDatabaseAndShelvesTheOneLeftInThePluginFolder(@TempDir Path server) throws IOException {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");
        Path legacy = write(dataDirectory.resolve("database.db"), "stale");
        Files.setLastModifiedTime(legacy, FileTime.fromMillis(4321000L));
        Path storage = server.resolve("storage").resolve("CoreProtect");
        write(storage.resolve("database.db"), "live");

        LegacyStorageMigration.run(dataDirectory, LOG);

        assertEquals("live", read(storage.resolve("database.db")));
        assertEquals("stale", read(dataDirectory.resolve("database.db.legacy-4321000")));
    }

    @Test
    void runsToTheSameResultOnEveryBoot(@TempDir Path server) throws IOException {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");
        write(dataDirectory.resolve("database.db"), "rows");
        write(dataDirectory.resolve("database.db-wal"), "committed");
        write(dataDirectory.resolve("config.yml"), "# CoreProtect Config\ndatabase-type: sqlite\nverbose: true");

        LegacyStorageMigration.run(dataDirectory, LOG);
        LegacyStorageMigration.run(dataDirectory, LOG);
        LegacyStorageMigration.run(dataDirectory, LOG);

        Path storage = server.resolve("storage").resolve("CoreProtect");
        assertEquals("rows", read(storage.resolve("database.db")));
        assertEquals("committed", read(storage.resolve("database.db-wal")));
        assertEquals(List.of("database.db", "database.db-wal", "database.yml"), names(storage));
        assertEquals(List.of("config.yml"), names(dataDirectory));
    }

    private static List<String> names(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(entry -> entry.getFileName().toString()).sorted().collect(Collectors.toList());
        }
    }

    private static Path write(Path file, String contents) throws IOException {
        Files.createDirectories(file.getParent());
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static String read(Path file) throws IOException {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
}
