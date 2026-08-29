package net.coreprotect.database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import net.coreprotect.config.ConfigHandler;

/**
 * Owns the layout of the version 2 SQLite database.
 *
 * <p>
 * Version 2 stores recent activity in the regular indexed tables and older activity in compressed
 * column segments, which is a different physical layout from the version 1 databases upstream
 * CoreProtect writes. The two cannot be mixed, so a version 1 file is set aside on first start and
 * a fresh version 2 file takes its place; no data is converted and nothing is deleted.
 * </p>
 */
public final class SQLiteSchema {

    /** The layout version this build reads and writes. */
    public static final int SCHEMA_VERSION = 2;

    /** Page size for new databases; larger pages compress and pack segment blobs better. */
    private static final int PAGE_SIZE = 8192;

    private static final DateTimeFormatter ARCHIVE_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ROOT);

    private SQLiteSchema() {
        throw new IllegalStateException("Schema class");
    }

    /**
     * @return the path of the configured SQLite database file
     */
    public static Path databaseFile() {
        return Paths.get(ConfigHandler.path + ConfigHandler.sqlite);
    }

    /**
     * Prepares the database file before any connection pool is opened.
     *
     * <p>
     * An existing version 2 file is left alone. A version 1 file is renamed out of the way, which
     * moves the file rather than copying it, so this never needs free disk space and never leaves a
     * second copy of the data behind. A file written by a newer layout is refused rather than
     * risking damage to it.
     * </p>
     *
     * @return the path the previous database was renamed to, or null if nothing was renamed
     * @throws SQLException
     *             if the file cannot be inspected, or was written by a newer layout
     */
    public static Path prepareDatabaseFile() throws SQLException {
        Path database = databaseFile();
        if (!Files.exists(database)) {
            createDatabaseFile(database);
            return null;
        }

        int version = readSchemaVersion(database);
        if (version == SCHEMA_VERSION) {
            return null;
        }
        if (version > SCHEMA_VERSION) {
            throw new SQLException("The CoreProtect database uses storage layout " + version + ", which this version cannot read");
        }

        Path archived = archive(database);
        createDatabaseFile(database);
        return archived;
    }

    /**
     * Creates an empty database file with the settings that can only be chosen while it is empty.
     *
     * <p>
     * The page size and the incremental vacuum setting are properties of the file itself. SQLite
     * only accepts them before anything has been written, which includes choosing a journal mode,
     * so they have to be applied on a connection of their own before the pool opens the database.
     * </p>
     *
     * @param database
     *            the database file to create
     * @throws SQLException
     *             if the file cannot be created
     */
    private static void createDatabaseFile(Path database) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database); Statement statement = connection.createStatement()) {
            applyFileSettings(statement);
            // Auto vacuum only takes effect once the setting has been written to the file, which
            // needs one page of content.
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS coreprotect_init (id INTEGER)");
            statement.executeUpdate("DROP TABLE IF EXISTS coreprotect_init");
        }
    }

    /**
     * Applies the settings a new database file needs before its tables are created. Both of these
     * are properties of the file itself and cannot be changed later without rewriting it.
     *
     * @param statement
     *            a statement on a connection to the database
     * @throws SQLException
     *             if the settings cannot be applied
     */
    public static void applyFileSettings(Statement statement) throws SQLException {
        statement.executeUpdate("PRAGMA page_size=" + PAGE_SIZE);
        // Incremental vacuuming lets roll-up and purge hand pages back without ever rewriting the
        // database into a second file.
        statement.executeUpdate("PRAGMA auto_vacuum=INCREMENTAL");
    }

    /**
     * Creates the version 2 tables and records the layout version.
     *
     * @param prefix
     *            the configured table prefix
     * @param statement
     *            a statement on a connection to the database
     * @throws SQLException
     *             if the tables cannot be created
     */
    public static void createTables(String prefix, Statement statement) throws SQLException {
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix + "schema (name TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;");

        statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix + "segment ("
                + "id INTEGER PRIMARY KEY, table_id INTEGER NOT NULL, start_rowid INTEGER NOT NULL, end_rowid INTEGER NOT NULL, "
                + "row_count INTEGER NOT NULL, min_time INTEGER NOT NULL, max_time INTEGER NOT NULL, day INTEGER NOT NULL, "
                + "wid_set BLOB, chunk_filter BLOB, user_filter BLOB, type_filter BLOB, action_bits INTEGER NOT NULL, "
                + "dict_id INTEGER NOT NULL, codec_version INTEGER NOT NULL, scalars BLOB NOT NULL, scalars_size INTEGER NOT NULL, "
                + "payload BLOB, payload_size INTEGER NOT NULL, user_stats BLOB, type_stats BLOB, action_stats BLOB, spawn_filter BLOB);");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + prefix + "segment_rowid_index ON " + prefix + "segment(table_id,end_rowid);");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + prefix + "segment_time_index ON " + prefix + "segment(table_id,max_time);");

        statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix + "segment_dict ("
                + "dict_id INTEGER PRIMARY KEY, table_id INTEGER NOT NULL, time INTEGER NOT NULL, sample_bytes INTEGER NOT NULL, "
                + "data BLOB NOT NULL);");

        // Blobs of consecutive rows, compressed together. Keyed by the row id the group begins at,
        // which is worked out from a row id rather than looked up.
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix + "blob_group ("
                + "table_id INTEGER NOT NULL, first_rowid INTEGER NOT NULL, dict_id INTEGER NOT NULL, "
                + "raw_size INTEGER NOT NULL, sizes BLOB, data BLOB NOT NULL, "
                + "PRIMARY KEY (table_id, first_rowid)) WITHOUT ROWID;");

        statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix + "cold_flag ("
                + "table_id INTEGER NOT NULL, rowid_ref INTEGER NOT NULL, rolled_back INTEGER NOT NULL, "
                + "PRIMARY KEY (table_id, rowid_ref)) WITHOUT ROWID;");

        // Databases created before segment statistics existed gain the columns here; the roll-up
        // fills them in as segments are written, and lookups fall back where they are missing.
        addSegmentColumn(prefix, statement, "user_stats");
        addSegmentColumn(prefix, statement, "type_stats");
        addSegmentColumn(prefix, statement, "action_stats");
        // Which entities a segment holds rows for, so the inspector can look one up without opening
        // segments that hold nothing of it.
        addSegmentColumn(prefix, statement, "spawn_filter");

        recordSchemaVersion(prefix, statement);
    }

    private static void addSegmentColumn(String prefix, Statement statement, String column) throws SQLException {
        try (ResultSet results = statement.executeQuery("PRAGMA table_info(" + prefix + "segment)")) {
            while (results.next()) {
                if (column.equalsIgnoreCase(results.getString("name"))) {
                    return;
                }
            }
        }
        statement.executeUpdate("ALTER TABLE " + prefix + "segment ADD COLUMN " + column + " BLOB");
    }

    private static void recordSchemaVersion(String prefix, Statement statement) throws SQLException {
        statement.executeUpdate("INSERT OR REPLACE INTO " + prefix + "schema (name, value) VALUES ('schema_version', '" + SCHEMA_VERSION + "');");
    }

    /**
     * Reads the layout version of an existing database file.
     *
     * @param database
     *            the database file
     * @return the recorded version, or 1 when the file predates version markers
     * @throws SQLException
     *             if the file cannot be read
     */
    private static int readSchemaVersion(Path database) throws SQLException {
        String prefix = ConfigHandler.prefix;
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database); Statement statement = connection.createStatement()) {
            boolean hasSchemaTable;
            try (ResultSet results = statement.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='" + prefix + "schema' LIMIT 1")) {
                hasSchemaTable = results.next();
            }
            if (!hasSchemaTable) {
                return 1;
            }

            try (ResultSet results = statement.executeQuery("SELECT value FROM " + prefix + "schema WHERE name='schema_version' LIMIT 1")) {
                if (!results.next()) {
                    return 1;
                }
                try {
                    return Integer.parseInt(results.getString(1).trim());
                }
                catch (NumberFormatException exception) {
                    return 1;
                }
            }
        }
    }

    /**
     * Renames a database file and its write-ahead log siblings out of the way.
     *
     * @param database
     *            the database file to set aside
     * @return the path the database was renamed to
     * @throws SQLException
     *             if the file cannot be renamed
     */
    private static Path archive(Path database) throws SQLException {
        String stamp = ARCHIVE_SUFFIX.format(LocalDateTime.now());
        Path archived = database.resolveSibling(database.getFileName() + ".v1-" + stamp);
        try {
            Files.move(database, archived, StandardCopyOption.ATOMIC_MOVE);
            for (String suffix : new String[] { "-wal", "-shm", "-journal" }) {
                Path sibling = database.resolveSibling(database.getFileName() + suffix);
                if (Files.exists(sibling)) {
                    Files.move(sibling, archived.resolveSibling(archived.getFileName() + suffix), StandardCopyOption.ATOMIC_MOVE);
                }
            }
        }
        catch (IOException exception) {
            throw new SQLException("Unable to set aside the previous CoreProtect database at " + database, exception);
        }

        return archived;
    }
}
