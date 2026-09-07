package net.coreprotect.storage;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Canonical names of the files CoreProtect owns inside its storage directory.
 *
 * <p>
 * These names are fixed. Nothing in the configuration may point at a different file, because a
 * misconfigured path is a silently empty history rather than an error.
 * </p>
 */
public final class StorageFiles {

    /** Database engine selection and the credentials used to reach an external database. */
    public static final String DATABASE_CONFIGURATION = "database.yml";

    /** The embedded SQLite database holding all logged activity. */
    public static final String SQLITE = "database.db";

    /** The embedded DuckDB database holding all logged activity. */
    public static final String DUCKDB = "database.duckdb";

    /** An upstream CoreProtect database placed here by an operator to be imported. */
    public static final String LEGACY_IMPORT = "old.db";

    /** Records which server is writing to a shared ClickHouse database. */
    public static final String CLICKHOUSE_WRITER = ".clickhouse-writer";

    /** Translated phrases, written by CoreProtect rather than by an operator. */
    public static final String LANGUAGE_CACHE = ".language";

    /** Scratch space used when the system temporary directory cannot hold executable files. */
    public static final String TEMPORARY_DIRECTORY = "cache";

    /**
     * SQLite writes committed transactions into the write-ahead log before checkpointing them into
     * the database file, so a {@code -wal} left behind by an unclean shutdown carries durable rows
     * and must travel with its database.
     */
    public static final List<String> SQLITE_SIDECARS = Collections
            .unmodifiableList(Arrays.asList("-wal", "-shm", "-journal"));

    /** DuckDB keeps its own write-ahead log beside the database file. */
    public static final List<String> DUCKDB_SIDECARS = Collections.unmodifiableList(Arrays.asList(".wal"));

    /**
     * The configuration keys that select a database engine or say how to reach an external one.
     *
     * <p>
     * These live in {@link #DATABASE_CONFIGURATION} rather than in {@code config.yml}, because a
     * plugin data folder is routinely copied between servers and shared with support, and these
     * keys carry passwords. The embedded engines are configured here too, so that one file answers
     * the question of which database a server is writing to.
     * </p>
     */
    public static final Set<String> DATABASE_KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "database-type",
            "use-mysql",
            "table-prefix",
            "maximum-pool-size",
            "enable-ssl",
            "mysql-host",
            "mysql-port",
            "mysql-database",
            "mysql-username",
            "mysql-password",
            "clickhouse-host",
            "clickhouse-port",
            "clickhouse-database",
            "clickhouse-username",
            "clickhouse-password",
            "clickhouse-tls",
            "clickhouse-consumer-delay")));

    private StorageFiles() {
        throw new IllegalStateException("Utility class");
    }
}
