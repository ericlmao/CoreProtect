package net.coreprotect.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoragePathsTest {

    @Test
    void resolvesStorageBesideThePluginsDirectory(@TempDir Path server) {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");

        assertEquals(server.resolve("storage").resolve("CoreProtect").toAbsolutePath().normalize(),
                StoragePaths.root(dataDirectory));
    }

    @Test
    void namesStorageAfterTheDataFolderRatherThanThePlugin(@TempDir Path server) {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect-Test");

        assertEquals(server.resolve("storage").resolve("CoreProtect-Test").toAbsolutePath().normalize(),
                StoragePaths.root(dataDirectory));
    }

    @Test
    void findsTheServerDirectoryTheDataFolderSitsIn(@TempDir Path server) {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");

        assertEquals(server.toAbsolutePath().normalize(), StoragePaths.serverRoot(dataDirectory));
    }

    @Test
    void resolvingDoesNotCreateAnything(@TempDir Path server) {
        StoragePaths.root(server.resolve("plugins").resolve("CoreProtect"));

        assertFalse(Files.exists(server.resolve("storage")));
    }

    @Test
    void creatingMakesTheWholeTree(@TempDir Path server) {
        Path root = StoragePaths.createRoot(server.resolve("plugins").resolve("CoreProtect"));

        assertTrue(Files.isDirectory(root));
        assertEquals(server.resolve("storage").resolve("CoreProtect").toAbsolutePath().normalize(), root);
    }

    @Test
    void creatingTwiceIsHarmless(@TempDir Path server) {
        Path dataDirectory = server.resolve("plugins").resolve("CoreProtect");

        assertEquals(StoragePaths.createRoot(dataDirectory), StoragePaths.createRoot(dataDirectory));
    }

    @Test
    void refusesADataDirectoryWithNoServerAboveIt() {
        assertThrows(IllegalStateException.class, () -> StoragePaths.root(Path.of("/")));
    }
}
