package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

    /** Groups packed per transaction. */
    private static final int GROUPS_PER_BATCH = 32;

    /** Blobs read to train a dictionary. At a couple of kilobytes each this is ample material. */
    private static final int SAMPLE_ROWS = 4096;

    /** Where the running total of bytes saved by compressing blobs is kept. */
    private static final String SAVED_MARKER = "blobs_saved_bytes";

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
            // Grouping is worth about three times what compressing a blob on its own is, so it is
            // done first and everything it cannot reach is compressed singly afterwards.
            saved = saved + packTable(connection, table, callback);
            saved = saved + recompressTable(connection, table, callback);
        }

        for (String table : TABLES) {
            dropOrphanedGroups(connection, table);
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
                statement.executeUpdate("DELETE FROM " + ConfigHandler.prefix + "schema WHERE name LIKE 'blobs\\_%\\_rowid' ESCAPE '\\'");
            }
        }
    }

    /**
     * Removes groups whose rows have all been purged.
     *
     * <p>
     * Retention removes the oldest rows, which are the lowest numbered, so any group lying entirely
     * below the oldest row still present holds nothing anyone can ask for. Left behind they would
     * keep that data on disk for good.
     * </p>
     */
    private static void dropOrphanedGroups(Connection connection, String table) throws SQLException {
        Integer tableId = ColdBlobStore.tableId(table);
        if (tableId == null) {
            return;
        }

        long oldest = minimumRowId(connection, table);
        String delete = "DELETE FROM " + ConfigHandler.prefix + "blob_group WHERE table_id = ? AND first_rowid + "
                + ColdBlobStore.GROUP_ROWS + " <= ?";
        try (PreparedStatement statement = connection.prepareStatement(delete)) {
            statement.setInt(1, tableId);
            statement.setLong(2, oldest);
            if (statement.executeUpdate() > 0) {
                ColdBlobStore.clearCache();
            }
        }
    }

    /**
     * Packs a table's blobs into groups of consecutive rows.
     *
     * <p>
     * The newest rows are left alone. They are still being written and a group is only worth making
     * once it is complete, so packing stops a group short of the end and the tail is compressed one
     * blob at a time until it too has been overtaken.
     * </p>
     *
     * @return the number of bytes the table shrank by
     */
    private static long packTable(Connection connection, String table, ColdRollupTask.Callback callback) throws Exception {
        Integer tableId = ColdBlobStore.tableId(table);
        Columns columns = tableId == null ? null : columnsOf(connection, table);
        if (columns == null) {
            return 0;
        }

        int dictionaryId = SegmentDictionary.currentDictionary(connection, DICTIONARY_TABLE_ID);
        if (dictionaryId <= 0) {
            return 0;
        }

        String marker = "blobgroup_" + table + "_through";
        String recorded = readMarker(connection, marker);
        long frontier = recorded == null ? 0 : Long.parseLong(recorded);
        long highest = maximumRowId(connection, table);
        // A whole group short of the end, so a group is never made from rows that are still arriving.
        long limit = ColdBlobStore.groupOf(Math.max(0, highest - ColdBlobStore.GROUP_ROWS));
        long saved = 0;

        while (frontier < limit) {
            callback.beforeSegment();

            long batchEnd = Math.min(frontier + (ColdBlobStore.GROUP_ROWS * GROUPS_PER_BATCH), limit);
            long batchSaved = packRange(connection, table, tableId, columns, dictionaryId, frontier, batchEnd);
            saved = saved + batchSaved;
            frontier = batchEnd;
            writeMarker(connection, marker, Long.toString(frontier));
            if (batchSaved > 0) {
                recordSaving(connection, batchSaved);
            }
        }

        return saved;
    }

    /**
     * Packs the groups covering a range of row ids and writes the rows back without their blobs.
     *
     * @return the number of bytes this range shrank by
     */
    private static long packRange(Connection connection, String table, int tableId, Columns columns, int dictionaryId, long from, long to) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        Map<Long, byte[]> blobs = new HashMap<>();
        long before = 0;

        String select = "SELECT " + columns.list + " FROM " + ConfigHandler.prefix + table + " WHERE rowid >= ? AND rowid < ? ORDER BY rowid";
        try (PreparedStatement statement = connection.prepareStatement(select)) {
            statement.setLong(1, from);
            statement.setLong(2, to);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    Object[] row = new Object[columns.names.size()];
                    for (int index = 0; index < row.length; index++) {
                        row[index] = results.getObject(index + 1);
                    }

                    byte[] stored = results.getBytes(columns.dataColumn + 1);
                    if (stored != null && stored.length > 0) {
                        byte[] plain = readable(stored);
                        if (plain == null) {
                            // A blob that cannot be read is left where it is rather than packed away
                            // as something it is not.
                            rows.add(row);
                            continue;
                        }
                        blobs.put(results.getLong(columns.rowIdColumn + 1), plain);
                        before = before + stored.length;
                        row[columns.dataColumn] = null;
                    }
                    rows.add(row);
                }
            }
        }

        if (blobs.isEmpty()) {
            return 0;
        }

        long stored = 0;
        List<ColdBlobStore.Group> groups = new ArrayList<>();
        for (long first = from; first < to; first = first + ColdBlobStore.GROUP_ROWS) {
            ColdBlobStore.Group group = ColdBlobStore.pack(blobs, first);
            if (group != null) {
                groups.add(group);
            }
        }

        String insertGroup = "INSERT OR REPLACE INTO " + ConfigHandler.prefix
                + "blob_group (table_id, first_rowid, dict_id, raw_size, sizes, data) VALUES (?,?,?,?,?,?)";
        StringBuilder placeholders = new StringBuilder();
        for (int index = 0; index < columns.names.size(); index++) {
            placeholders.append(index > 0 ? ",?" : "?");
        }
        String insertRow = "INSERT INTO " + ConfigHandler.prefix + table + " (" + columns.list + ") VALUES (" + placeholders + ")";

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            try (PreparedStatement statement = connection.prepareStatement(insertGroup)) {
                for (ColdBlobStore.Group group : groups) {
                    byte[] compressed = SegmentDictionary.compress(group.payload, dictionaryId, connection);
                    statement.setInt(1, tableId);
                    statement.setLong(2, group.firstRowId);
                    statement.setInt(3, dictionaryId);
                    statement.setInt(4, group.payload.length);
                    statement.setBytes(5, group.sizes);
                    statement.setBytes(6, compressed);
                    statement.addBatch();
                    stored = stored + compressed.length + group.sizes.length;
                }
                statement.executeBatch();
            }

            // The rows go back without their blobs. Written over in place they would stay on the pages
            // they already occupied, so they are removed and written again to free those pages.
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM " + ConfigHandler.prefix + table + " WHERE rowid >= ? AND rowid < ?")) {
                statement.setLong(1, from);
                statement.setLong(2, to);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(insertRow)) {
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

        return before - stored;
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
            long batchSaved = recompressRange(connection, table, columns, frontier, batchEnd);
            saved = saved + batchSaved;
            frontier = batchEnd;
            writeMarker(connection, marker, Long.toString(frontier));
            // Recorded as it is earned rather than at the end, so a run that is stopped part way
            // still accounts for the rows it did compress.
            if (batchSaved > 0) {
                recordSaving(connection, batchSaved);
            }
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
        private final int rowIdColumn;

        private Columns(List<String> names, int dataColumn, int rowIdColumn) {
            this.names = names;
            this.list = String.join(",", names);
            this.dataColumn = dataColumn;
            this.rowIdColumn = rowIdColumn;
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
        int rowIdColumn = -1;

        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("PRAGMA table_info(" + ConfigHandler.prefix + table + ")")) {
            while (results.next()) {
                String name = results.getString("name");
                String type = results.getString("type");
                if (results.getInt("pk") == 1 && type != null && type.toUpperCase(Locale.ROOT).contains("INT")) {
                    // An integer primary key is the row id under another name.
                    rowIdColumn = names.size();
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
        if (rowIdColumn < 0) {
            // Without one the row id is not among the columns, so it is named explicitly; rows have
            // to keep the row ids the rest of the data refers to them by.
            names.add(0, "rowid");
            dataColumn = dataColumn + 1;
            rowIdColumn = 0;
        }
        return new Columns(names, dataColumn, rowIdColumn);
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

    private static long minimumRowId(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(MIN(rowid),0) FROM " + ConfigHandler.prefix + table)) {
            // With no rows left every group is orphaned, so the boundary is above all of them.
            long minimum = results.next() ? results.getLong(1) : 0;
            return minimum == 0 ? Long.MAX_VALUE : minimum;
        }
    }

    private static long maximumRowId(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COALESCE(MAX(rowid),0) FROM " + ConfigHandler.prefix + table)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    /**
     * Adds to the running total of what compressing these blobs has saved.
     *
     * <p>
     * The live tables cannot be measured cheaply enough to do it on demand, and a lookup asking how
     * much space entity data takes would have to read every row of it. The total is kept as it is
     * earned instead, so {@code /co status} can say that the live side shrank because the data was
     * compressed rather than because any of it went missing.
     * </p>
     */
    private static void recordSaving(Connection connection, long saved) throws SQLException {
        String recorded = readMarker(connection, SAVED_MARKER);
        long total = saved;
        if (recorded != null) {
            try {
                total = total + Long.parseLong(recorded);
            }
            catch (NumberFormatException exception) {
                // A total that cannot be read starts again from this run.
            }
        }
        writeMarker(connection, SAVED_MARKER, Long.toString(total));
    }

    /**
     * @param connection
     *            an open connection
     * @return the bytes compressing the live tables' blobs has saved so far
     * @throws SQLException
     *             if the total cannot be read
     */
    public static long savedBytes(Connection connection) throws SQLException {
        String recorded = readMarker(connection, SAVED_MARKER);
        if (recorded == null) {
            return 0;
        }
        try {
            return Long.parseLong(recorded);
        }
        catch (NumberFormatException exception) {
            return 0;
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
