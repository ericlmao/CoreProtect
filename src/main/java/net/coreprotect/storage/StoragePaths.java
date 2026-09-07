package net.coreprotect.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Resolves the server-root storage directory that owns every persistent CoreProtect file.
 *
 * <p>
 * The plugin data folder under {@code plugins/} holds configuration only. Databases, their
 * write-ahead sidecars, import sources, caches, and the credentials used to reach an external
 * database live in {@code <server root>/storage/<plugin folder name>/}. The name is derived from
 * the data folder rather than hard coded, so a renamed plugin folder keeps its own storage, and no
 * configuration key can point the storage directory somewhere else.
 * </p>
 */
public final class StoragePaths {

    private static final String STORAGE_DIRECTORY = "storage";

    private StoragePaths() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Resolves the storage directory for the plugin owning the given data directory, without
     * creating it.
     *
     * @param dataDirectory
     *            the plugin data directory, normally {@code plugins/CoreProtect}
     * @return {@code <server root>/storage/<plugin folder name>}
     */
    public static Path root(Path dataDirectory) {
        Path normalized = Objects.requireNonNull(dataDirectory, "dataDirectory").toAbsolutePath().normalize();
        Path folderName = normalized.getFileName();
        Path pluginsDirectory = normalized.getParent();
        if (folderName == null || pluginsDirectory == null) {
            throw new IllegalStateException("Cannot resolve the CoreProtect storage directory: data directory '"
                    + normalized + "' has no parent plugins directory.");
        }

        Path serverRoot = pluginsDirectory.getParent();
        if (serverRoot == null) {
            throw new IllegalStateException("Cannot resolve the CoreProtect storage directory: plugins directory '"
                    + pluginsDirectory + "' has no parent server directory.");
        }

        return serverRoot.resolve(STORAGE_DIRECTORY).resolve(folderName);
    }

    /**
     * Resolves the server directory the plugin is running in.
     *
     * @param dataDirectory
     *            the plugin data directory, normally {@code plugins/CoreProtect}
     * @return the directory holding {@code plugins/} and {@code storage/}
     */
    public static Path serverRoot(Path dataDirectory) {
        return root(dataDirectory).getParent().getParent();
    }

    /**
     * Resolves the storage directory and creates it when it does not exist yet.
     *
     * @param dataDirectory
     *            the plugin data directory, normally {@code plugins/CoreProtect}
     * @return the existing storage directory
     */
    public static Path createRoot(Path dataDirectory) {
        Path root = root(dataDirectory);
        try {
            Files.createDirectories(root);
        }
        catch (IOException exception) {
            throw new IllegalStateException("Failed to create the CoreProtect storage directory " + root, exception);
        }
        return root;
    }
}
