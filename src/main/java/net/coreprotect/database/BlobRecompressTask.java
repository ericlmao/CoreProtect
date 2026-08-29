package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobCompression;
import net.coreprotect.utility.serialize.BlobDictionary;

/**
 * Compresses the blobs held in the live tables against a shared dictionary.
 *
 * <p>
 * Most of CoreProtect's data is rolled into compressed segments once it is old enough, where many
 * rows are compressed together and the repetition between them is what makes the saving. Entity data
 * cannot be treated that way: it is read one row at a time, by row id, from anywhere in the table,
 * so it has to stay where a single row can be fetched without unpacking anything else.
 * </p>
 *
 * <p>
 * That leaves each blob to be compressed on its own, and on a couple of kilobytes of entity data
 * that is worth about a third of the size. A dictionary trained on a sample of the same data
 * supplies the repetition that a single blob does not contain, and takes the same data to a
 * fortieth of its size while leaving every blob independently readable. This walks the tables and
 * rewrites their blobs that way.
 * </p>
 *
 * <p>
 * Rows are rewritten in place, in batches, so the database is never copied and the pages the shorter
 * blobs no longer need are returned by the incremental vacuum that follows. How far each table has
 * been rewritten is recorded as it goes, so an interrupted run carries on rather than starting over.
 * </p>
 */
public final class BlobRecompressTask {

    /**
     * The dictionary table id used for blobs in the live tables. Segment dictionaries are numbered by
     * the table they belong to, so this sits above them where it cannot collide.
     */
    static final int DICTIONARY_TABLE_ID = 64;

    /** Rows rewritten per transaction. */
    private static final int BATCH_ROWS = 2000;

    /** Blobs read to train a dictionary. At a couple of kilobytes each this is ample material. */
    private static final int SAMPLE_ROWS = 4096;

    /** Tables holding a blob per row that is read by row id rather than scanned. */
    private static final List<String> TABLES = Arrays.asList("entity", "entity_spawn");

    private BlobRecompressTask() {
        throw new IllegalStateException("Task class");
    }

    /**
     * Makes the stored dictionaries available for reading and writing blobs. Called once while the
     * plugin starts, before anything is logged or read.
     *
     * @param connection
     *            an open connection
     * @throws SQLException
     *             if the dictionaries cannot be read
     */
    public static void loadDictionaries(Connection connection) throws SQLException {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return;
        }

        String query = "SELECT data FROM " + ConfigHandler.prefix + "segment_dict WHERE table_id = ? ORDER BY dict_id";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, DICTIONARY_TABLE_ID);
            try (ResultSet results = statement.executeQuery()) {
                List<byte[]> dictionaries = new ArrayList<>();
                while (results.next()) {
                    dictionaries.add(results.getBytes(1));
                }
                for (int index = 0; index < dictionaries.size(); index++) {
                    // Everything is registered so that older blobs stay readable; the newest is the
                    // one anything written from now on is compressed against.
                    BlobDictionary.register(dictionaries.get(index), index == dictionaries.size() - 1);
                }
            }
        }
    }

    /**
     * Rewrites every blob that is not already compressed against the current dictionary.
     *
     * @param connection
     *            an open connection
     * @param callback
     *            invoked between batches; throwing stops the run
     * @return the number of bytes the tables shrank by
     * @throws Exception
     *             if the rewrite fails or is stopped
     */
    public static long run(Connection connection, ColdRollupTask.Callback callback) throws Exception {
        if (!ConfigHandler.databaseType.isSQLite() || !BlobCompression.isAvailable()) {
            return 0;
        }

        if (!BlobDictionary.hasDictionary()) {
            trainDictionary(connection);
        }
        if (!BlobDictionary.hasDictionary()) {
            // Without a dictionary this would compress each blob by itself, which on data of this
            // shape is not worth rewriting the table for.
            return 0;
        }

        long saved = 0;
        for (String table : TABLES) {
            saved = saved + recompressTable(connection, table, callback);
        }

        if (saved > 0) {
            Database.reclaimFreePages(connection);
        }
        return saved;
    }

    /**
     * Trains a dictionary from a sample of the blobs that are about to be rewritten.
     */
    private static void trainDictionary(Connection connection) throws SQLException {
        List<byte[]> samples = new ArrayList<>();
        for (String table : TABLES) {
            if (!hasColumn(connection, table)) {
                continue;
            }
            String query = "SELECT data FROM " + ConfigHandler.prefix + table
                    + " WHERE data IS NOT NULL ORDER BY rowid DESC LIMIT " + SAMPLE_ROWS;
            try (PreparedStatement statement = connection.prepareStatement(query); ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    byte[] stored = results.getBytes(1);
                    // Trained on what will actually be compressed, which is the blob as the rest of
                    // the plugin sees it rather than however it happens to be stored today.
                    byte[] plain = readable(stored);
                    if (plain != null && plain.length > 0) {
                        samples.add(plain);
                    }
                }
            }
            if (!samples.isEmpty()) {
                break;
            }
        }

        int dictionaryId = SegmentDictionary.train(connection, DICTIONARY_TABLE_ID, samples);
        if (dictionaryId > 0) {
            loadDictionaries(connection);
            // A new dictionary means every blob written against the previous one is worth rewriting,
            // so the record of how far each table has been taken starts again.
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DELETE FROM " + ConfigHandler.prefix + "schema WHERE name LIKE 'blobs\\_%' ESCAPE '\\'");
            }
        }
    }

    /**
     * Rewrites one table's blobs.
     *
     * @return the number of bytes the table shrank by
     */
    private static long recompressTable(Connection connection, String table, ColdRollupTask.Callback callback) throws Exception {
        Columns columns = columnsOf(connection, table);
        if (columns == null) {
            return 0;
        }

        String marker = "blobs_" + table + "_rowid";
        String recorded = readMarker(connection, marker);
        long frontier = recorded == null ? 0 : Long.parseLong(recorded);
        long highest = maximumRowId(connection, table);
        long saved = 0;

        while (frontier < highest) {
            callback.beforeSegment();

            long batchEnd = Math.min(frontier + BATCH_ROWS, highest);
            saved = saved + recompressRange(connection, table, columns, frontier, batchEnd);
            frontier = batchEnd;
            writeMarker(connection, marker, Long.toString(frontier));
        }

        return saved;
    }

    /**
     * Rewrites a range of rows with their blobs compressed.
     *
     * <p>
     * The rows are deleted and written back rather than updated in place. A shorter value written
     * over a longer one leaves the row where it was, on a page that keeps every page it already had
     * and simply carries more free space; SQLite never repacks a page that still holds rows, so the
     * file would not shrink by a single byte however well the blobs compressed. Removing a
     * contiguous range of row ids empties whole pages, and writing the much smaller rows back fills
     * far fewer of them, which is what leaves pages for the vacuum to hand back.
     * </p>
     *
     * @return the number of bytes this range shrank by
     */
    private static long recompressRange(Connection connection, String table, Columns columns, long after, long through) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        long before = 0;
        long now = 0;
        boolean anyChanged = false;

        String select = "SELECT " + columns.list + " FROM " + ConfigHandler.prefix + table + " WHERE rowid > ? AND rowid <= ? ORDER BY rowid";
        try (PreparedStatement statement = connection.prepareStatement(select)) {
            statement.setLong(1, after);
            statement.setLong(2, through);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    Object[] row = new Object[columns.names.size()];
                    for (int index = 0; index < row.length; index++) {
                        row[index] = results.getObject(index + 1);
                    }

                    byte[] stored = results.getBytes(columns.dataColumn + 1);
                    before = before + (stored == null ? 0 : stored.length);
                    byte[] replacement = compressed(stored);
                    if (replacement != null) {
                        row[columns.dataColumn] = replacement;
                        anyChanged = true;
                        now = now + replacement.length;
                    }
                    else {
                        now = now + (stored == null ? 0 : stored.length);
                    }

                    rows.add(row);
                }
            }
        }

        if (!anyChanged || rows.isEmpty()) {
            return 0;
        }

        StringBuilder placeholders = new StringBuilder();
        for (int index = 0; index < columns.names.size(); index++) {
            placeholders.append(index > 0 ? ",?" : "?");
        }
        String insert = "INSERT INTO " + ConfigHandler.prefix + table + " (" + columns.list + ") VALUES (" + placeholders + ")";

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM " + ConfigHandler.prefix + table + " WHERE rowid > ? AND rowid <= ?")) {
                statement.setLong(1, after);
                statement.setLong(2, through);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(insert)) {
                for (Object[] row : rows) {
                    for (int index = 0; index < row.length; index++) {
                        statement.setObject(index + 1, row[index]);
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            connection.commit();
        }
        catch (SQLException exception) {
            connection.rollback();
            throw exception;
        }
        finally {
            connection.setAutoCommit(autoCommit);
        }

        return before - now;
    }

    /**
     * @param stored
     *            the blob as it is stored
     * @return a smaller replacement, or null when the blob is already as small as this can make it
     */
    private static byte[] compressed(byte[] stored) {
        if (stored == null || stored.length == 0 || BlobDictionary.isCurrent(stored)) {
            return null;
        }

        byte[] plain = readable(stored);
        if (plain == null) {
            return null;
        }
        byte[] compressed = BlobCompression.recompress(plain);
        return compressed.length >= stored.length ? null : compressed;
    }

    /** The columns of a table, in order, and where its blob sits among them. */
    private static final class Columns {
        private final List<String> names;
        private final String list;
        private final int dataColumn;

        private Columns(List<String> names, int dataColumn) {
            this.names = names;
            this.list = String.join(",", names);
            this.dataColumn = dataColumn;
        }
    }

    /**
     * Works out which columns a table has, so that rows can be written back exactly as they were
     * apart from the blob.
     *
     * @return the columns, or null when the table has no blob to compress
     */
    private static Columns columnsOf(Connection connection, String table) throws SQLException {
        List<String> names = new ArrayList<>();
        int dataColumn = -1;
        boolean hasKey = false;

        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("PRAGMA table_info(" + ConfigHandler.prefix + table + ")")) {
            while (results.next()) {
                String name = results.getString("name");
                String type = results.getString("type");
                if (results.getInt("pk") == 1 && type != null && type.toUpperCase(Locale.ROOT).contains("INT")) {
                    hasKey = true;
                }
                if ("data".equalsIgnoreCase(name)) {
                    dataColumn = names.size();
                }
                names.add(name);
            }
        }

        if (names.isEmpty() || dataColumn < 0) {
            return null;
        }
        if (!hasKey) {
            // Without an integer primary key the row id is not one of the columns, so it is named
            // explicitly; rows have to keep the row ids the rest of the data refers to them by.
            names.add(0, "rowid");
            dataColumn = dataColumn + 1;
        }
        return new Columns(names, dataColumn);
    }

    /**
     * @return the blob as the rest of the plugin reads it, or null when it cannot be read at all
     */
    private static byte[] readable(byte[] stored) {
        try {
            return BlobCompression.decompress(stored);
        }
        catch (RuntimeException failure) {
            // A blob that cannot be decompressed is left exactly as it is, rather than replaced by a
            // guess at what it held.
            return null;
        }
    }

    private static boolean hasColumn(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("PRAGMA table_info(" + ConfigHandler.prefix + table + ")")) {
            while (results.next()) {
                if ("data".equalsIgnoreCase(results.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static long maximumRowId(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(MAX(rowid),0) FROM " + ConfigHandler.prefix + table)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private static String readMarker(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT value FROM " + ConfigHandler.prefix + "schema WHERE name = ?")) {
            statement.setString(1, name);
            try (ResultSet results = statement.executeQuery()) {
                return results.next() ? results.getString(1) : null;
            }
        }
    }

    private static void writeMarker(Connection connection, String name, String value) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT OR REPLACE INTO " + ConfigHandler.prefix + "schema (name, value) VALUES (?, ?)")) {
            statement.setString(1, name);
            statement.setString(2, value);
            statement.executeUpdate();
        }
    }
}
