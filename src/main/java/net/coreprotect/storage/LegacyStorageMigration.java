package net.coreprotect.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Moves CoreProtect files that older versions kept inside {@code plugins/CoreProtect} into the
 * server-root storage directory.
 *
 * <p>
 * This runs as the first statement of {@code onLoad()}, before anything reads or writes the
 * configuration. The database settings are the reason for the ordering: loading the configuration
 * rewrites {@code config.yml} with every key it expects to find, so a migration that ran later
 * would be racing against the file it is trying to split.
 * </p>
 *
 * <p>
 * Every step is idempotent, so this can run on each boot, and no step ever deletes a file. A file
 * that exists only in the plugin folder is moved; when storage already owns the same file, the
 * plugin folder's copy is renamed aside with a timestamp and the operator is told about it.
 * </p>
 */
public final class LegacyStorageMigration {

    /** The scratch file a purge attaches while it rebuilds the SQLite tables. */
    private static final String SQLITE_PURGE_SCRATCH = ".tmp";

    /** Databases set aside by the upgrade to this fork's storage layout. */
    private static final String SQLITE_ARCHIVE_GLOB = "*.v1-*";

    /** The partial copy left behind if a shutdown rebuild was interrupted. */
    private static final String SQLITE_REBUILD_GLOB = "*.rebuilding";

    /** Import sources CoreProtect renamed after reading them. */
    private static final String IMPORT_ARCHIVE_GLOB = "*.imported-*";

    /** DuckDB's spill directory, which it recreates as needed. */
    private static final String DUCKDB_SCRATCH = ".tmp";

    /** The plugin configuration the database settings are split out of. */
    private static final String CONFIGURATION = "config.yml";

    private LegacyStorageMigration() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Relocates every CoreProtect file that still sits in the plugin data folder.
     *
     * @param dataDirectory
     *            the plugin data directory, normally {@code plugins/CoreProtect}
     * @param log
     *            the plugin logger
     */
    public static void run(Path dataDirectory, Logger log) {
        Objects.requireNonNull(dataDirectory, "dataDirectory");
        Objects.requireNonNull(log, "log");
        if (!Files.isDirectory(dataDirectory)) {
            return;
        }

        Path storageRoot = StoragePaths.root(dataDirectory);

        // The live databases move first, with their write-ahead sidecars, so that an interruption
        // anywhere later in this method still leaves a complete database in one directory.
        StorageRelocation.relocateDatabase(dataDirectory.resolve(StorageFiles.SQLITE),
                storageRoot.resolve(StorageFiles.SQLITE), StorageFiles.SQLITE_SIDECARS, log);
        StorageRelocation.relocateDatabase(dataDirectory.resolve(StorageFiles.DUCKDB),
                storageRoot.resolve(StorageFiles.DUCKDB), StorageFiles.DUCKDB_SIDECARS, log);
        StorageRelocation.relocateDatabase(dataDirectory.resolve(StorageFiles.LEGACY_IMPORT),
                storageRoot.resolve(StorageFiles.LEGACY_IMPORT), StorageFiles.SQLITE_SIDECARS, log);

        // Then the copies CoreProtect set aside itself. These are whole databases an operator may
        // still want, so they follow the live file rather than being left behind to be forgotten.
        StorageRelocation.relocateFile(dataDirectory.resolve(StorageFiles.SQLITE + SQLITE_PURGE_SCRATCH),
                storageRoot.resolve(StorageFiles.SQLITE + SQLITE_PURGE_SCRATCH), log);
        StorageRelocation.relocateMatching(dataDirectory, SQLITE_ARCHIVE_GLOB, storageRoot, log);
        StorageRelocation.relocateMatching(dataDirectory, SQLITE_REBUILD_GLOB, storageRoot, log);
        StorageRelocation.relocateMatching(dataDirectory, IMPORT_ARCHIVE_GLOB, storageRoot, log);
        StorageRelocation.mergeDirectory(dataDirectory.resolve(StorageFiles.DUCKDB + DUCKDB_SCRATCH),
                storageRoot.resolve(StorageFiles.DUCKDB + DUCKDB_SCRATCH), log);

        // Files CoreProtect writes for itself rather than for an operator to edit.
        StorageRelocation.relocateFile(dataDirectory.resolve(StorageFiles.CLICKHOUSE_WRITER),
                storageRoot.resolve(StorageFiles.CLICKHOUSE_WRITER), log);
        StorageRelocation.relocateFile(dataDirectory.resolve(StorageFiles.LANGUAGE_CACHE),
                storageRoot.resolve(StorageFiles.LANGUAGE_CACHE), log);

        // Last, because it rewrites a file the rest of startup is about to read.
        LegacyDatabaseConfiguration.split(dataDirectory.resolve(CONFIGURATION),
                storageRoot.resolve(StorageFiles.DATABASE_CONFIGURATION), log);
    }
}
