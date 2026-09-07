package net.coreprotect.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageRelocationTest {

    private static final Logger LOG = Logger.getLogger(StorageRelocationTest.class.getName());

    @Test
    void movesAFileThatOnlyExistsInTheLegacyLocation(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "rows");
        Path target = directory.resolve("storage").resolve("database.db");

        StorageRelocation.relocateFile(legacy, target, LOG);

        assertFalse(Files.exists(legacy));
        assertEquals("rows", read(target));
    }

    @Test
    void doesNothingWhenThereIsNoLegacyFile(@TempDir Path directory) {
        Path target = directory.resolve("storage").resolve("database.db");

        StorageRelocation.relocateFile(directory.resolve("legacy").resolve("database.db"), target, LOG);

        assertFalse(Files.exists(target));
    }

    @Test
    void keepsTheStorageCopyAndShelvesTheLegacyOne(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "old");
        Files.setLastModifiedTime(legacy, FileTime.fromMillis(1234000L));
        Path target = write(directory.resolve("storage").resolve("database.db"), "live");

        StorageRelocation.relocateFile(legacy, target, LOG);

        assertEquals("live", read(target));
        assertFalse(Files.exists(legacy));
        assertEquals("old", read(legacy.resolveSibling("database.db.legacy-1234000")));
    }

    @Test
    void shelvesUnderADistinctNameWhenTheFirstIsTaken(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "old");
        Files.setLastModifiedTime(legacy, FileTime.fromMillis(1234000L));
        write(legacy.resolveSibling("database.db.legacy-1234000"), "older");
        Path target = write(directory.resolve("storage").resolve("database.db"), "live");

        StorageRelocation.relocateFile(legacy, target, LOG);

        assertEquals("older", read(legacy.resolveSibling("database.db.legacy-1234000")));
        assertEquals("old", read(legacy.resolveSibling("database.db.legacy-1234000-1")));
    }

    @Test
    void movesADatabaseWithItsWriteAheadSidecars(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "rows");
        write(directory.resolve("legacy").resolve("database.db-wal"), "committed");
        write(directory.resolve("legacy").resolve("database.db-shm"), "shared");
        Path target = directory.resolve("storage").resolve("database.db");

        StorageRelocation.relocateDatabase(legacy, target, StorageFiles.SQLITE_SIDECARS, LOG);

        assertEquals("rows", read(target));
        assertEquals("committed", read(target.resolveSibling("database.db-wal")));
        assertEquals("shared", read(target.resolveSibling("database.db-shm")));
        assertFalse(Files.exists(target.resolveSibling("database.db.migration-incomplete")));
    }

    @Test
    void movesADatabaseWithNoSidecarsWithoutLeavingAMarker(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "rows");
        Path target = directory.resolve("storage").resolve("database.db");

        StorageRelocation.relocateDatabase(legacy, target, StorageFiles.SQLITE_SIDECARS, LOG);

        assertEquals("rows", read(target));
        assertFalse(Files.exists(target.resolveSibling("database.db.migration-incomplete")));
    }

    @Test
    void shelvesTheLegacyDatabaseAndItsSidecarsTogether(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "old");
        Path legacyWal = write(directory.resolve("legacy").resolve("database.db-wal"), "old-wal");
        Files.setLastModifiedTime(legacy, FileTime.fromMillis(9000L));
        Path target = write(directory.resolve("storage").resolve("database.db"), "live");

        StorageRelocation.relocateDatabase(legacy, target, StorageFiles.SQLITE_SIDECARS, LOG);

        assertEquals("live", read(target));
        assertFalse(Files.exists(legacy));
        assertFalse(Files.exists(legacyWal));
        assertEquals("old", read(legacy.resolveSibling("database.db.legacy-9000")));
        assertEquals("old-wal", read(legacy.resolveSibling("database.db-wal.legacy-9000")));
    }

    @Test
    void refusesToRunWhenAnEarlierMigrationWasInterrupted(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "rows");
        Path target = directory.resolve("storage").resolve("database.db");
        write(target.resolveSibling("database.db.migration-incomplete"), "");

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> StorageRelocation.relocateDatabase(legacy, target, StorageFiles.SQLITE_SIDECARS, LOG));

        assertTrue(failure.getMessage().contains("did not finish"));
        assertEquals("rows", read(legacy));
    }

    @Test
    void movesEveryFileMatchingAPattern(@TempDir Path directory) throws IOException {
        Path legacy = directory.resolve("legacy");
        write(legacy.resolve("database.db.v1-20240101-000000"), "first");
        write(legacy.resolve("database.db.v1-20240101-000000-wal"), "first-wal");
        write(legacy.resolve("database.db"), "live");
        Path storage = directory.resolve("storage");

        StorageRelocation.relocateMatching(legacy, "*.v1-*", storage, LOG);

        assertEquals("first", read(storage.resolve("database.db.v1-20240101-000000")));
        assertEquals("first-wal", read(storage.resolve("database.db.v1-20240101-000000-wal")));
        assertEquals("live", read(legacy.resolve("database.db")));
    }

    @Test
    void leavesShelvedFilesWhereAnOperatorWasToldToLookForThem(@TempDir Path directory) throws IOException {
        Path legacy = directory.resolve("legacy");
        write(legacy.resolve("scratch"), "kept");
        write(legacy.resolve("database.db.legacy-1234000"), "shelved");

        StorageRelocation.mergeDirectory(legacy, directory.resolve("storage"), LOG);

        assertEquals("kept", read(directory.resolve("storage").resolve("scratch")));
        assertEquals("shelved", read(legacy.resolve("database.db.legacy-1234000")));
    }

    @Test
    void mergesNestedDirectoriesAndRemovesTheEmptiedTree(@TempDir Path directory) throws IOException {
        Path legacy = directory.resolve("legacy");
        write(legacy.resolve("inner").resolve("spill"), "scratch");

        StorageRelocation.mergeDirectory(legacy, directory.resolve("storage"), LOG);

        assertEquals("scratch", read(directory.resolve("storage").resolve("inner").resolve("spill")));
        assertFalse(Files.exists(legacy));
    }

    @Test
    void runsToTheSameResultASecondTime(@TempDir Path directory) throws IOException {
        Path legacy = write(directory.resolve("legacy").resolve("database.db"), "rows");
        write(directory.resolve("legacy").resolve("database.db-wal"), "committed");
        Path target = directory.resolve("storage").resolve("database.db");

        StorageRelocation.relocateDatabase(legacy, target, StorageFiles.SQLITE_SIDECARS, LOG);
        StorageRelocation.relocateDatabase(legacy, target, StorageFiles.SQLITE_SIDECARS, LOG);

        assertEquals("rows", read(target));
        assertEquals("committed", read(target.resolveSibling("database.db-wal")));
        assertEquals(List.of("database.db", "database.db-wal"), names(target.getParent()));
    }

    private static List<String> names(Path directory) throws IOException {
        try (java.util.stream.Stream<Path> entries = Files.list(directory)) {
            return entries.map(entry -> entry.getFileName().toString()).sorted()
                    .collect(java.util.stream.Collectors.toList());
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
