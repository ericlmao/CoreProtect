package net.coreprotect.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.Chat;

/**
 * Rebuilds the database file so it is the size of the data it holds.
 *
 * <p>
 * Compacting turns tens of gigabytes of rows into a fraction of that in compressed storage, and
 * everything the rows used to occupy becomes free space inside the file. Handing that space back a
 * page at a time is what SQLite offers for it, and on a file that is almost all free space it is
 * ruinous: each page still holding data has to be moved out of the end of the file before the end
 * can be dropped, one page and one parent pointer at a time. Measured on a server whose file was
 * seventy five gigabytes with seventy three free, that came to two and a half days of work.
 * </p>
 *
 * <p>
 * Writing the data out to a new file instead costs one pass over what is actually there, so the same
 * job takes as long as writing the compressed data once. The new file replaces the old one when the
 * server stops, which is the moment nothing is logging and no connection is open, so nothing written
 * since can be lost by the swap.
 * </p>
 *
 * <p>
 * The copy is made with <code>VACUUM INTO</code>, which keeps every row id exactly as it was.
 * <em>Plain <code>VACUUM</code> must never be used here.</em> It renumbers the row ids of any table
 * without an explicit integer primary key, which is all of the activity tables, and this fork's
 * compressed storage rests on live row ids sitting above the sealed ones and on rows in other tables
 * pointing at them by number. Renumbering would quietly break both. The copy is checked against the
 * original before anything is replaced.
 * </p>
 */
public final class DatabaseRebuild {

    /** Where the copy is written while it is being made. */
    private static final String WORKING_SUFFIX = ".rebuilding";

    /** The share of the file that has to be free space before a rebuild is worth its cost. */
    private static final double FREE_SHARE = 0.20;

    /** And the least free space worth rebuilding for, so small files are left alone. */
    private static final long FREE_BYTES = 256L * 1024 * 1024;

    /** How much more room than the copy needs has to be free on the disk before one is attempted. */
    private static final double HEADROOM = 1.25;

    private DatabaseRebuild() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Whether the file holds enough free space to be worth rebuilding.
     *
     * @param connection
     *            an open connection
     * @return true when a rebuild would shrink the file appreciably
     * @throws SQLException
     *             if the file cannot be measured
     */
    public static boolean worthwhile(Connection connection) throws SQLException {
        return worthwhile(connection, FREE_BYTES);
    }

    /**
     * Whether the file holds enough free space to be worth rebuilding.
     *
     * @param connection
     *            an open connection
     * @param minimumFreeBytes
     *            the least free space worth rebuilding for
     * @return true when a rebuild would shrink the file appreciably
     * @throws SQLException
     *             if the file cannot be measured
     */
    static boolean worthwhile(Connection connection, long minimumFreeBytes) throws SQLException {
        if (!ConfigHandler.databaseType.isSQLite() || !Config.getGlobal().COMPACT_REBUILD) {
            return false;
        }

        try (Statement statement = connection.createStatement()) {
            long pageSize = value(statement, "PRAGMA page_size");
            long pages = value(statement, "PRAGMA page_count");
            long free = value(statement, "PRAGMA freelist_count");
            if (pages <= 0) {
                return false;
            }
            return free * pageSize >= minimumFreeBytes && free >= pages * FREE_SHARE;
        }
    }

    /**
     * Rebuilds the file if it is worth it. Called when the server stops, after every connection has
     * been closed, so that nothing can be written while the file is replaced.
     */
    public static void runAfterClose() {
        if (!ConfigHandler.databaseType.isSQLite() || !Config.getGlobal().COMPACT_REBUILD) {
            return;
        }

        Path database = Paths.get(ConfigHandler.path, ConfigHandler.sqlite);
        if (!Files.isRegularFile(database)) {
            return;
        }

        try {
            rebuild(database);
        }
        catch (Exception failure) {
            // The old file is untouched unless the copy was made and checked, so a failure here
            // costs the space it would have saved and nothing else.
            Chat.console("Could not rebuild the database file: " + failure.getMessage());
        }
    }

    /**
     * Writes the data out to a new file and puts it in the old one's place.
     *
     * @param database
     *            the database file
     * @return the number of bytes the file shrank by, or 0 if it was left alone
     * @throws Exception
     *             if the copy cannot be made or does not match the original
     */
    static long rebuild(Path database) throws Exception {
        return rebuild(database, FREE_BYTES);
    }

    /**
     * Writes the data out to a new file and puts it in the old one's place.
     *
     * @param database
     *            the database file
     * @param minimumFreeBytes
     *            the least free space worth rebuilding for
     * @return the number of bytes the file shrank by, or 0 if it was left alone
     * @throws Exception
     *             if the copy cannot be made or does not match the original
     */
    static long rebuild(Path database, long minimumFreeBytes) throws Exception {
        Path working = Paths.get(database.toString() + WORKING_SUFFIX);
        deleteFile(working);

        long before = Files.size(database);
        long saved = 0;

        try (Connection connection = open(database)) {
            try (Statement statement = connection.createStatement()) {
                // Anything still in the write ahead log belongs in the copy.
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }

            if (!worthwhile(connection, minimumFreeBytes)) {
                return 0;
            }

            long needed = liveBytes(connection);
            long available = new File(database.getParent().toString()).getUsableSpace();
            if (available < needed * HEADROOM) {
                Chat.console("Skipping the database rebuild: it needs " + ColdStorageStats.format((long) (needed * HEADROOM))
                        + " free on the disk and there is " + ColdStorageStats.format(available) + ".");
                return 0;
            }

            Chat.console("Rebuilding the database file, which is " + ColdStorageStats.format(before) + " holding "
                    + ColdStorageStats.format(needed) + " of data. This is done once and takes a few minutes.");

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("VACUUM INTO '" + working.toString().replace("'", "''") + "'");
            }

            check(connection, working);
        }

        // Only now, with the copy made and checked and every connection closed.
        replace(working, database);
        saved = before - Files.size(database);
        Chat.console("Rebuilt the database file: " + ColdStorageStats.format(before) + " -> "
                + ColdStorageStats.format(Files.size(database)) + ", freeing " + ColdStorageStats.format(saved) + ".");
        return saved;
    }

    /**
     * Checks the copy against the original before anything is replaced.
     *
     * <p>
     * What is checked is what this fork cannot survive being wrong: that every table is there, holds
     * the same number of rows, and holds them under exactly the same row ids. Compressed storage is
     * addressed by row id from end to end, and rows in one table point at rows in another by number,
     * so a copy that renumbered anything would be worse than no copy at all.
     * </p>
     */
    static void check(Connection connection, Path working) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("ATTACH DATABASE '" + working.toString().replace("'", "''") + "' AS rebuilt");
            try {
                try (ResultSet results = statement.executeQuery("PRAGMA rebuilt.quick_check(1)")) {
                    String answer = results.next() ? results.getString(1) : "no answer";
                    if (!"ok".equalsIgnoreCase(answer)) {
                        throw new SQLException("the rebuilt file did not check out: " + answer);
                    }
                }

                try (ResultSet results = statement.executeQuery(
                        "SELECT name, sql FROM main.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")) {
                    List<String[]> tables = new ArrayList<>();
                    while (results.next()) {
                        tables.add(new String[] { results.getString(1), results.getString(2) });
                    }
                    for (String[] table : tables) {
                        compare(statement, table[0], table[1]);
                    }
                }
            }
            finally {
                statement.executeUpdate("DETACH DATABASE rebuilt");
            }
        }
    }

    /**
     * Checks one table in the copy against the same table in the original.
     *
     * <p>
     * A table with row ids is checked on them, since they are what everything else here is addressed
     * by. A table declared without row ids has none to check, so it is checked on the whole of its
     * key instead, which for these is a pair of numbers naming what the row holds.
     * </p>
     */
    private static void compare(Statement statement, String table, String definition) throws SQLException {
        String keys = definition != null && definition.toLowerCase(java.util.Locale.ROOT).contains("without rowid")
                ? numericKeys(statement, table)
                : "rowid";

        StringBuilder query = new StringBuilder("SELECT COUNT(*)");
        for (String key : keys.isEmpty() ? new String[0] : keys.split(",")) {
            query.append(", COALESCE(MIN(").append(key).append("),0), COALESCE(MAX(").append(key)
                    .append("),0), COALESCE(SUM(").append(key).append("),0)");
        }
        query.append(" FROM ");

        String original = row(statement, query + "main." + table);
        String copy = row(statement, query + "rebuilt." + table);
        if (!original.equals(copy)) {
            throw new SQLException("the rebuilt file holds " + table + " differently: " + original + " became " + copy);
        }
    }

    /** The numeric parts of a table's primary key, for tables that have no row ids to check. */
    private static String numericKeys(Statement statement, String table) throws SQLException {
        StringBuilder keys = new StringBuilder();
        try (ResultSet results = statement.executeQuery("PRAGMA main.table_info(" + table + ")")) {
            while (results.next()) {
                String type = results.getString("type");
                if (results.getInt("pk") > 0 && type != null && type.toUpperCase(java.util.Locale.ROOT).contains("INT")) {
                    keys.append(keys.length() > 0 ? "," : "").append(results.getString("name"));
                }
            }
        }
        return keys.toString();
    }

    /** Puts the copy in the original's place, and clears away what belonged to the original. */
    private static void replace(Path working, Path database) throws IOException {
        try {
            Files.move(working, database, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(working, database, StandardCopyOption.REPLACE_EXISTING);
        }
        // The old file's write ahead log describes pages the new file does not have.
        deleteFile(Paths.get(database.toString() + "-wal"));
        deleteFile(Paths.get(database.toString() + "-shm"));
        deleteFile(Paths.get(working.toString() + "-wal"));
        deleteFile(Paths.get(working.toString() + "-shm"));
    }

    /** The bytes the data actually occupies, which is what the copy will be. */
    private static long liveBytes(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            long pageSize = value(statement, "PRAGMA page_size");
            return (value(statement, "PRAGMA page_count") - value(statement, "PRAGMA freelist_count")) * pageSize;
        }
    }

    private static String row(Statement statement, String query) throws SQLException {
        StringBuilder answer = new StringBuilder();
        try (ResultSet results = statement.executeQuery(query)) {
            if (!results.next()) {
                return "";
            }
            int columns = results.getMetaData().getColumnCount();
            for (int column = 1; column <= columns; column++) {
                answer.append(column > 1 ? "/" : "").append(results.getLong(column));
            }
        }
        return answer.toString();
    }

    private static long value(Statement statement, String pragma) throws SQLException {
        try (ResultSet results = statement.executeQuery(pragma)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private static void deleteFile(Path path) throws IOException {
        Files.deleteIfExists(path);
    }

    private static Connection open(Path database) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=30000");
            statement.executeUpdate("PRAGMA temp_store=FILE");
        }
        return connection;
    }
}
