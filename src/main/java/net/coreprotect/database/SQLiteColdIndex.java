package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.serialize.BlobCompression;

/**
 * Keeps track of the cold segments in a version 2 SQLite database and makes them readable by the
 * ordinary lookup queries.
 *
 * <p>
 * Segment metadata is small and is held in memory, so deciding which segments a lookup has to touch
 * costs no database work. Rows from the segments that survive that check are decoded into a
 * temporary table which is unioned with the live table, so every existing predicate, ordering rule
 * and paging behaviour keeps working unchanged.
 * </p>
 */
public final class SQLiteColdIndex {

    /** Tables whose old rows are rolled into segments, with the id stored in the segment table. */
    private static final Map<String, Integer> SEGMENTED_TABLES = segmentedTables();

    private static final Map<String, TableLayout> LAYOUTS = new ConcurrentHashMap<>();

    private static volatile Map<Integer, List<ColdSegment>> segments = Collections.emptyMap();
    private static volatile boolean loaded;


    private SQLiteColdIndex() {
        throw new IllegalStateException("Index class");
    }

    private static Map<String, Integer> segmentedTables() {
        Map<String, Integer> tables = new LinkedHashMap<>();
        tables.put("block", 0);
        tables.put("container", 1);
        tables.put("entity_container", 2);
        tables.put("entity_interaction", 3);
        tables.put("item", 4);
        tables.put("sign", 5);
        tables.put("chat", 6);
        tables.put("command", 7);
        tables.put("session", 8);
        return Collections.unmodifiableMap(tables);
    }

    /**
     * @return the tables that are rolled into cold segments
     */
    public static List<String> getSegmentedTables() {
        return new ArrayList<>(SEGMENTED_TABLES.keySet());
    }

    /**
     * @return the segment table ids of the tables whose rows belong to an entity rather than a place,
     *         as a comma separated list for use in a query
     */
    public static String entityKeyedTableIds() {
        StringBuilder ids = new StringBuilder();
        for (String table : new String[] { "entity_container", "entity_interaction" }) {
            Integer id = SEGMENTED_TABLES.get(table);
            if (id != null) {
                ids.append(ids.length() > 0 ? "," : "").append(id);
            }
        }
        return ids.length() > 0 ? ids.toString() : "-1";
    }

    /**
     * @param table
     *            an unprefixed table name
     * @return the segment table id, or null when the table is never segmented
     */
    public static Integer tableId(String table) {
        return SEGMENTED_TABLES.get(table);
    }

    /** The metadata of one segment, without its compressed contents. */
    public static final class ColdSegment {
        final long id;
        final int tableId;
        final long startRowId;
        final long endRowId;
        final int rowCount;
        final long minTime;
        final long maxTime;
        final int[] worldIds;
        final SegmentMembership chunkFilter;
        final SegmentMembership userFilter;
        final SegmentMembership typeFilter;
        final SegmentMembership spawnFilter;
        final SegmentStatistics userStats;
        final SegmentStatistics typeStats;
        final SegmentStatistics actionStats;
        /** The exact rows per chunk, or null for a segment written before they were recorded. */
        final SegmentChunkCounts chunkCounts;

        ColdSegment(long id, int tableId, long startRowId, long endRowId, int rowCount, long minTime, long maxTime, int[] worldIds, SegmentMembership chunkFilter, SegmentMembership userFilter, SegmentMembership typeFilter, SegmentMembership spawnFilter, SegmentStatistics userStats, SegmentStatistics typeStats, SegmentStatistics actionStats, SegmentChunkCounts chunkCounts) {
            this.id = id;
            this.tableId = tableId;
            this.startRowId = startRowId;
            this.endRowId = endRowId;
            this.rowCount = rowCount;
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.worldIds = worldIds;
            // The exact counts answer everything the chunk filter does, and without its false
            // positives, so a segment that has them does not keep the filter's four kilobytes too.
            this.chunkFilter = chunkCounts == null ? chunkFilter : null;
            this.chunkCounts = chunkCounts;
            this.userFilter = userFilter;
            this.typeFilter = typeFilter;
            this.spawnFilter = spawnFilter;
            this.userStats = userStats;
            this.typeStats = typeStats;
            this.actionStats = actionStats;
        }

        /**
         * @param users
         *            the user ids a lookup wants, or null
         * @param types
         *            the block type ids a lookup wants, or null
         * @return how many rows this segment holds for them, or the whole segment when the counts
         *         are unknown
         */
        int estimatedRows(long[] users, long[] types) {
            if (users != null && userStats != null) {
                return userStats.count(users);
            }
            if (types != null && typeStats != null) {
                return typeStats.count(types);
            }
            return rowCount;
        }

        public long getId() {
            return id;
        }

        public long getStartRowId() {
            return startRowId;
        }

        public long getEndRowId() {
            return endRowId;
        }

        public int getRowCount() {
            return rowCount;
        }

        /**
         * @return the exact rows per chunk, or null when the segment has not recorded them yet
         */
        public SegmentChunkCounts getChunkCounts() {
            return chunkCounts;
        }

        /**
         * @return the segment table this segment belongs to
         */
        public int getTableId() {
            return tableId;
        }
    }

    /** The column names and kinds of one segmented table. */
    static final class TableLayout {
        final String[] columns;
        final int[] types;
        final String[] declaredTypes;
        final int timeColumn;
        final int worldColumn;
        final int xColumn;
        final int zColumn;
        final int rolledBackColumn;
        final int userColumn;
        final int typeColumn;
        final int actionColumn;
        final int spawnColumn;
        /**
         * The columns every segment of the table is read as, so a segment packed before a column was
         * added shows it holding the column's default.
         */
        final ColdSegmentCodec.Columns decodeColumns;

        TableLayout(String[] columns, int[] types, String[] declaredTypes) {
            this(columns, types, declaredTypes, new Object[columns.length]);
        }

        /**
         * @param defaults
         *            each column's default, as {@link ColdSegmentCodec.Columns} takes them
         */
        TableLayout(String[] columns, int[] types, String[] declaredTypes, Object[] defaults) {
            this.columns = columns;
            this.types = types;
            this.declaredTypes = declaredTypes;
            this.decodeColumns = new ColdSegmentCodec.Columns(types, defaults);
            this.timeColumn = indexOf(columns, "time");
            this.worldColumn = indexOf(columns, "wid");
            this.xColumn = indexOf(columns, "x");
            this.zColumn = indexOf(columns, "z");
            this.rolledBackColumn = indexOf(columns, "rolled_back");
            this.userColumn = indexOf(columns, "user");
            this.typeColumn = indexOf(columns, "type");
            this.actionColumn = indexOf(columns, "action");
            this.spawnColumn = indexOf(columns, "entity_spawn_rowid");
        }

        /**
         * @return the indexes of the world, x and z columns, which place a row in a chunk, or null
         *         when the table lacks any of them or they do not hold integers, which is the only
         *         kind of value a filter is shown as a plain number
         */
        int[] chunkColumns() {
            int[] location = { worldColumn, xColumn, zColumn };
            for (int column : location) {
                if (column < 0 || types[column] != ColdSegmentCodec.TYPE_INTEGER) {
                    return null;
                }
            }
            return location;
        }

        private static int indexOf(String[] columns, String name) {
            for (int index = 0; index < columns.length; index++) {
                if (columns[index].equalsIgnoreCase(name)) {
                    return index;
                }
            }
            return -1;
        }
    }

    /**
     * Reads the column layout of a table, which the segment encoder and the temporary table both
     * need. Layouts never change while the server is running, so they are read once.
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @return the layout of that table, or null when the table does not exist
     * @throws SQLException
     *             if the layout cannot be read
     */
    static TableLayout layout(Connection connection, String table) throws SQLException {
        TableLayout cached = LAYOUTS.get(table);
        if (cached != null) {
            return cached;
        }

        List<String> columns = new ArrayList<>();
        List<Integer> types = new ArrayList<>();
        List<String> declared = new ArrayList<>();
        List<Object> defaults = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("PRAGMA table_info(" + ConfigHandler.prefix + table + ")")) {
            while (results.next()) {
                String name = results.getString("name");
                String declaredType = results.getString("type");
                int type = columnType(declaredType);
                columns.add(name);
                declared.add(declaredType == null || declaredType.isEmpty() ? "INTEGER" : declaredType);
                types.add(type);
                defaults.add(defaultValue(results.getString("dflt_value"), type));
            }
        }

        if (columns.isEmpty()) {
            // The table does not exist in this database, which is normal for a backend that only
            // creates part of the schema, and for tests.
            return null;
        }

        int[] typeArray = new int[types.size()];
        for (int index = 0; index < typeArray.length; index++) {
            typeArray[index] = types.get(index);
        }

        TableLayout layout = new TableLayout(columns.toArray(new String[0]), typeArray, declared.toArray(new String[0]), defaults.toArray());
        LAYOUTS.put(table, layout);
        return layout;
    }

    /**
     * Forgets the layout read for a table, after a column has been added to it, so the next read
     * sees the column.
     *
     * @param table
     *            an unprefixed table name
     */
    static void forgetLayout(String table) {
        LAYOUTS.remove(table);
    }

    /**
     * The value a row that predates a column holds in it, as {@code PRAGMA table_info} reports the
     * column's default.
     *
     * @param declaredDefault
     *            the default as SQLite reports it, or null for none
     * @param type
     *            the column's kind
     * @return null, a Long for an integer column, a Double for a real one, or
     *         {@link ColdSegmentCodec.Columns#UNSUPPORTED_DEFAULT}
     */
    static Object defaultValue(String declaredDefault, int type) {
        if (declaredDefault == null) {
            return null;
        }
        String value = declaredDefault.trim();
        while (value.length() >= 2 && value.startsWith("(") && value.endsWith(")")) {
            value = value.substring(1, value.length() - 1).trim();
        }
        if (value.equalsIgnoreCase("NULL")) {
            return null;
        }
        if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
            // A quoted number in a numeric column is stored as that number.
            value = value.substring(1, value.length() - 1).trim();
        }
        try {
            if (type == ColdSegmentCodec.TYPE_INTEGER) {
                return Long.valueOf(value);
            }
            if (type == ColdSegmentCodec.TYPE_REAL) {
                return Double.valueOf(value);
            }
        }
        catch (NumberFormatException unsupported) {
            // Falls through: the default is not a plain number.
        }
        return ColdSegmentCodec.Columns.UNSUPPORTED_DEFAULT;
    }

    /**
     * @return the columns a segment's rows are read as, or null when its table is not in this
     *         database, in which case the segment is read with the columns it was packed with
     */
    private static ColdSegmentCodec.Columns decodeColumns(Connection connection, ColdSegment segment) throws SQLException {
        for (Map.Entry<String, Integer> entry : SEGMENTED_TABLES.entrySet()) {
            if (entry.getValue() == segment.tableId) {
                TableLayout layout = layout(connection, entry.getKey());
                return layout == null ? null : layout.decodeColumns;
            }
        }
        return null;
    }

    private static int columnType(String declaredType) {
        String type = declaredType == null ? "" : declaredType.toUpperCase(java.util.Locale.ROOT);
        if (type.contains("BLOB")) {
            return ColdSegmentCodec.TYPE_BLOB;
        }
        if (type.contains("CHAR") || type.contains("TEXT") || type.contains("CLOB")) {
            return ColdSegmentCodec.TYPE_TEXT;
        }
        if (type.contains("REAL") || type.contains("FLOA") || type.contains("DOUB")) {
            return ColdSegmentCodec.TYPE_REAL;
        }
        return ColdSegmentCodec.TYPE_INTEGER;
    }

    /**
     * Loads the segment metadata into memory. Called when the database is opened and again after
     * segments are added or removed.
     *
     * @param connection
     *            an open connection
     * @throws SQLException
     *             if the metadata cannot be read
     */
    public static synchronized void reload(Connection connection) throws SQLException {
        Map<Integer, List<ColdSegment>> loadedSegments = new HashMap<>();
        String query = "SELECT " + segmentColumns(connection) + " FROM " + ConfigHandler.prefix + "segment WHERE " + visibleSegments(connection) + " ORDER BY table_id,start_rowid,id";
        List<ColdSegment> all = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                ColdSegment segment = readSegment(results);
                loadedSegments.computeIfAbsent(segment.tableId, key -> new ArrayList<>()).add(segment);
                all.add(segment);
            }
        }

        segments = loadedSegments;
        loaded = true;
        SegmentFrameCache.retain(all);
    }

    private static final String SEGMENT_COLUMNS = "id,table_id,start_rowid,end_rowid,row_count,min_time,max_time,wid_set,chunk_filter,user_filter,type_filter,spawn_filter,user_stats,type_stats,action_stats";

    /**
     * The segment metadata to read. A database is given the chunk counts column when it is opened,
     * but a copy opened read only, as the benchmarks open one, may predate it, and reads as if no
     * segment had counts yet.
     */
    private static String segmentColumns(Connection connection) throws SQLException {
        return hasSegmentColumn(connection, "chunk_counts") ? SEGMENT_COLUMNS + ",chunk_counts" : SEGMENT_COLUMNS + ",NULL AS chunk_counts";
    }

    /**
     * The condition that leaves out segments compaction is still writing. Until compaction commits
     * them in place of the segments they were merged from they hold rows that are also still in
     * those, so no lookup may ever see them. A copy opened read only that predates compaction has
     * no such segments.
     */
    static String visibleSegments(Connection connection) throws SQLException {
        return hasSegmentColumn(connection, "compacted") ? "compacted >= 0" : "1";
    }

    private static boolean hasSegmentColumn(Connection connection, String column) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("PRAGMA table_info(" + ConfigHandler.prefix + "segment)")) {
            while (results.next()) {
                if (column.equalsIgnoreCase(results.getString("name"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static ColdSegment readSegment(ResultSet results) throws SQLException {
        return new ColdSegment(
                results.getLong("id"),
                results.getInt("table_id"),
                results.getLong("start_rowid"),
                results.getLong("end_rowid"),
                results.getInt("row_count"),
                results.getLong("min_time"),
                results.getLong("max_time"),
                readWorldIds(results.getBytes("wid_set")),
                SegmentMembership.decode(results.getBytes("chunk_filter")),
                SegmentMembership.decode(results.getBytes("user_filter")),
                SegmentMembership.decode(results.getBytes("type_filter")),
                SegmentMembership.decode(results.getBytes("spawn_filter")),
                SegmentStatistics.decode(results.getBytes("user_stats")),
                SegmentStatistics.decode(results.getBytes("type_stats")),
                SegmentStatistics.decode(results.getBytes("action_stats")),
                SegmentChunkCounts.decode(results.getBytes("chunk_counts")));
    }

    /**
     * Reads the metadata of some segments again after they were rewritten, and publishes it in
     * place of what was loaded, leaving every other segment as it was.
     *
     * @param connection
     *            an open connection
     * @param ids
     *            the segments to read again
     * @throws SQLException
     *             if the metadata cannot be read
     */
    static void refreshSegments(Connection connection, java.util.Collection<Long> ids) throws SQLException {
        if (ids.isEmpty() || !loaded) {
            return;
        }
        synchronized (SQLiteColdIndex.class) {
            Map<Long, ColdSegment> fresh = new HashMap<>();
            String query = "SELECT " + segmentColumns(connection) + " FROM " + ConfigHandler.prefix + "segment WHERE id = ? AND " + visibleSegments(connection);
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                for (Long id : ids) {
                    statement.setLong(1, id);
                    try (ResultSet results = statement.executeQuery()) {
                        if (results.next()) {
                            fresh.put(id, readSegment(results));
                        }
                    }
                }
            }
            if (fresh.isEmpty()) {
                return;
            }

            // Published as a new map, so a lookup already walking the old lists never sees them
            // change underneath it.
            Map<Integer, List<ColdSegment>> published = new HashMap<>();
            for (Map.Entry<Integer, List<ColdSegment>> entry : segments.entrySet()) {
                List<ColdSegment> replaced = new ArrayList<>(entry.getValue().size());
                for (ColdSegment segment : entry.getValue()) {
                    replaced.add(fresh.getOrDefault(segment.id, segment));
                }
                published.put(entry.getKey(), replaced);
            }
            segments = published;
        }
    }

    /**
     * Adds the segments written for one table since its metadata was last read, without reading
     * the rest again.
     *
     * <p>
     * Rows are packed as soon as a segment's worth of them has been logged, so segments are added
     * all day long, and reading the metadata of every segment back after each one would cost more
     * the longer a server has been running. A new segment always starts above every row id already
     * packed, so the new ones are exactly those ending above the highest packed row id known.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @throws SQLException
     *             if the metadata cannot be read
     */
    static void loadNewSegments(Connection connection, String table) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null) {
            return;
        }
        if (!loaded) {
            reload(connection);
            return;
        }

        synchronized (SQLiteColdIndex.class) {
            List<ColdSegment> known = segments.getOrDefault(tableId, Collections.emptyList());
            long highWater = 0;
            for (ColdSegment segment : known) {
                highWater = Math.max(highWater, segment.endRowId);
            }

            List<ColdSegment> added = new ArrayList<>();
            String query = "SELECT " + segmentColumns(connection) + " FROM " + ConfigHandler.prefix + "segment WHERE table_id = ? AND end_rowid > ? AND " + visibleSegments(connection)
                    + " ORDER BY start_rowid,id";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setInt(1, tableId);
                statement.setLong(2, highWater);
                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        added.add(readSegment(results));
                    }
                }
            }
            if (added.isEmpty()) {
                return;
            }

            // Published as a new map, so a lookup already walking the old lists never sees them
            // change underneath it.
            List<ColdSegment> combined = new ArrayList<>(known.size() + added.size());
            combined.addAll(known);
            combined.addAll(added);
            Map<Integer, List<ColdSegment>> published = new HashMap<>(segments);
            published.put(tableId, combined);
            segments = published;
        }
    }

    /**
     * Publishes the segments compaction wrote in place of the ones they were merged from, leaving
     * every other segment as it was.
     *
     * <p>
     * The new segments hold exactly the rows of the old ones under the same row ids, so a lookup
     * finds the same rows either way, but only one of the two may be in memory at a time: called
     * while lookups are held off, just after the swap committed, so no lookup ever sees both or
     * neither. What was held in memory of the old segments' frames is let go of too.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param removed
     *            the ids of the segments that were merged
     * @param added
     *            the ids of the segments that replace them
     * @throws SQLException
     *             if the new segments' metadata cannot be read
     */
    static void replaceSegments(Connection connection, String table, java.util.Collection<Long> removed, java.util.Collection<Long> added) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null) {
            return;
        }
        if (!loaded) {
            reload(connection);
            return;
        }

        synchronized (SQLiteColdIndex.class) {
            List<ColdSegment> fresh = new ArrayList<>(added.size());
            String query = "SELECT " + segmentColumns(connection) + " FROM " + ConfigHandler.prefix + "segment WHERE id = ? AND " + visibleSegments(connection);
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                for (Long id : added) {
                    statement.setLong(1, id);
                    try (ResultSet results = statement.executeQuery()) {
                        if (!results.next()) {
                            throw new SQLException("Compacted segment " + id + " is missing");
                        }
                        fresh.add(readSegment(results));
                    }
                }
            }

            java.util.Set<Long> gone = new java.util.HashSet<>(removed);
            List<ColdSegment> known = segments.getOrDefault(tableId, Collections.emptyList());
            List<ColdSegment> combined = new ArrayList<>(known.size() - gone.size() + fresh.size());
            for (ColdSegment segment : known) {
                if (gone.contains(segment.id)) {
                    SegmentFrameCache.forget(segment);
                }
                else {
                    combined.add(segment);
                }
            }
            combined.addAll(fresh);
            // Kept in the order the database lists them in, by first row id and then id, which is
            // the order the segments of one compacted run were written in.
            combined.sort(Comparator.comparingLong((ColdSegment segment) -> segment.startRowId).thenComparingLong(segment -> segment.id));

            // Published as a new map, so a lookup already walking the old lists never sees them
            // change underneath it.
            Map<Integer, List<ColdSegment>> published = new HashMap<>(segments);
            published.put(tableId, combined);
            segments = published;
        }
    }

    /**
     * Reads the highest packed row id of a table from the database itself rather than from memory.
     *
     * <p>
     * The metadata in memory is brought up to date just after a segment is committed, so for a
     * moment it can be behind. A write that has to decide whether a row it could not find in the
     * live table was packed a moment ago asks here, inside its own transaction, where the answer
     * is always current.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @return the highest packed row id, or 0 when nothing of that table is packed
     * @throws SQLException
     *             if the database cannot be read
     */
    public static long storedHighWaterMark(Connection connection, String table) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null || !ConfigHandler.databaseType.isSQLite()) {
            return 0;
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT COALESCE(MAX(end_rowid),0) FROM " + ConfigHandler.prefix + "segment WHERE table_id = ?")) {
            statement.setInt(1, tableId);
            try (ResultSet results = statement.executeQuery()) {
                return results.next() ? results.getLong(1) : 0;
            }
        }
    }

    /**
     * Forgets the loaded metadata, so the next lookup reloads it.
     */
    public static void invalidate() {
        segments = Collections.emptyMap();
        // The column layouts describe the database that is being let go of, so they cannot be
        // carried over to the next one.
        LAYOUTS.clear();
        loaded = false;
        SegmentFrameCache.clear();
        SegmentQuarantine.clear();
    }

    /**
     * @return true if any cold segment exists
     */
    public static boolean hasSegments() {
        for (List<ColdSegment> tableSegments : segments.values()) {
            if (!tableSegments.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    static void ensureLoaded(Connection connection) throws SQLException {
        if (!loaded) {
            synchronized (SQLiteColdIndex.class) {
                if (!loaded) {
                    reload(connection);
                }
            }
        }
    }

    /** What one lookup needs to know about while it builds and runs its query. */
    /**
     * The values a lookup asks to leave out.
     *
     * <p>
     * A lookup can name users, block types or actions it does not want. The query applies those as
     * NOT IN tests, and the compressed reader has to apply exactly the same ones or its counts
     * would describe more rows than the query would return.
     * </p>
     */
    static final class Exclusions {

        static final Exclusions NONE = new Exclusions(null, null, null);

        final long[] users;
        final long[] types;
        final long[] actions;

        Exclusions(long[] users, long[] types, long[] actions) {
            this.users = users;
            this.types = types;
            this.actions = actions;
        }

        boolean isEmpty() {
            return users == null && types == null && actions == null;
        }
    }

    private static final class LookupContext {
        private final long startTime;
        private final long endTime;
        private final List<String> materialized = new ArrayList<>();

        private LookupContext(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        private long[] users;
        private long[] types;
        private long[] actions;
        private long[] spawns;
        private long rowBudget;
        private boolean truncated;
        private boolean readFailed;
        /** The part of each table the lookup is confined to, by unprefixed table name. */
        private final Map<String, KeyWindow> windows = new HashMap<>();
        /** Rows written to this lookup's temporary tables and not yet dropped. */
        private long heldRows;
        /** Whether a read over the limit is handed back to the caller to divide, rather than failed. */
        private boolean divisible;
        private boolean counting;
        private final Map<String, Long> counts = new HashMap<>();
        private long[] excludedUsers;
        private long[] excludedTypes;
        private long[] excludedActions;
        private boolean predicateUnmodelled;
        private boolean predicateGiven;
        private ColdPagePredicate predicate;
        private PageOrder pageOrder;
    }

    /** The orders a page of results can be shown in, newest first. */
    private enum PageOrder {
        /** Newest row id first, the order a lookup of one table pages in. */
        ROW_ID,
        /** Newest time first, then newest row id, the order each table of a merged lookup is cut to. */
        TIME_THEN_ROW_ID
    }

    /**
     * Tells the reader the order the page is shown in, so that when it can test the whole query
     * itself it keeps only the compressed rows that order ranks high enough to appear.
     *
     * @param orderBy
     *            the ORDER BY expression of the page, with or without the ORDER BY keywords, or null
     *            when the page has no order the reader can rely on
     */
    public static void setPageOrder(String orderBy) {
        LookupContext context = LOOKUP.get();
        if (context != null) {
            context.pageOrder = pageOrder(orderBy);
        }
    }

    private static PageOrder pageOrder(String orderBy) {
        if (orderBy == null) {
            return null;
        }
        String normalized = orderBy.trim().replaceFirst("(?i)^ORDER\\s+BY\\s+", "").replaceAll("\\s+", "").toLowerCase(java.util.Locale.ROOT);
        switch (normalized) {
            case "rowiddesc":
                return PageOrder.ROW_ID;
            case "timedesc,rowiddesc":
            case "timedesc,iddesc":
                // The merged queries select the row id as id, so the two name the same thing.
                return PageOrder.TIME_THEN_ROW_ID;
            default:
                // An order the reader does not recognise could rank rows it would drop onto the
                // page, so every row is handed to the query instead.
                return null;
        }
    }

    /**
     * Puts the lookup into counting mode, where compressed rows are counted as they decode and
     * never built or copied into a temporary table.
     *
     * @param counting
     *            true when the query only needs a count and the reader can evaluate all of it
     */
    public static void setCounting(boolean counting) {
        LookupContext context = LOOKUP.get();
        if (context != null) {
            context.counting = counting;
            context.counts.clear();
        }
    }

    /**
     * @param table
     *            an unprefixed table name
     * @return how many compressed rows matched, for a lookup in counting mode
     */
    public static long countedRows(String table) {
        LookupContext context = LOOKUP.get();
        return context == null ? 0 : context.counts.getOrDefault(table, 0L);
    }

    /**
     * Limits how many compressed rows a lookup needs, which is what lets a page of results be built
     * without reading a player's entire history.
     *
     * <p>
     * Compressed rows always have lower row ids than live rows, and results are ordered newest
     * first, so the rows a page can possibly show are the live rows plus the newest compressed rows.
     * Reading segments newest first and stopping once the budget is met therefore produces the same
     * page as reading everything. If the surrounding query turns out to reject much of what was
     * read, {@link #wasTruncated()} reports it so the lookup can be repeated without a budget.
     * </p>
     *
     * @param rows
     *            the greatest number of rows the page can show, or 0 for no limit
     */
    public static void setRowBudget(long rows) {
        LookupContext context = LOOKUP.get();
        if (context != null) {
            context.rowBudget = rows;
            context.truncated = false;
        }
    }

    /**
     * Confines what a lookup reads of one table to a stretch of its page order, live rows and
     * packed rows alike, so a page deep into a long history reads only the rows it shows.
     *
     * @param table
     *            an unprefixed table name
     * @param window
     *            the stretch to read, or null to read the whole table
     */
    public static void setPageWindow(String table, KeyWindow window) {
        LookupContext context = LOOKUP.get();
        if (context != null) {
            if (window == null) {
                context.windows.remove(table);
            }
            else {
                context.windows.put(table, window);
            }
        }
    }

    /**
     * Says whether a read that would write more rows than {@link #materializeLimit()} is handed
     * back as a {@link ReadLimitException} for the caller to divide into smaller windows, or
     * treated as a read that failed.
     *
     * @param divisible
     *            true when the caller walks the history a window at a time
     */
    static void setDivisible(boolean divisible) {
        LookupContext context = LOOKUP.get();
        if (context != null) {
            context.divisible = divisible;
        }
    }

    /**
     * Drops every temporary table the lookup has written so far, so the next window of a walk
     * starts with none held.
     *
     * @param connection
     *            the connection the lookup runs on
     */
    static void releaseReads(Connection connection) {
        LookupContext context = LOOKUP.get();
        if (context == null) {
            return;
        }
        for (String table : context.materialized) {
            releaseMaterialized(connection, table);
        }
        context.materialized.clear();
        context.heldRows = 0;
    }

    /**
     * Keeps a temporary table the lookup wrote itself until the lookup ends, and then drops it with
     * the rest.
     *
     * @param name
     *            the table's name after the {@code cp_cold_} prefix every such table has
     */
    static void holdUntilEnd(String name) {
        LookupContext context = LOOKUP.get();
        if (context != null && !context.materialized.contains(name)) {
            context.materialized.add(name);
        }
    }

    /**
     * The most rows one lookup may hold in temporary tables at once.
     *
     * <p>
     * Temporary tables are written to disk, and a lookup that copied a player's whole history into
     * one could fill it. No read may write more than this; a lookup that needs more reads its
     * history a window at a time instead, and anything that cannot is refused.
     * </p>
     */
    static final long MATERIALIZE_LIMIT = 250_000L;

    private static volatile long materializeLimit = MATERIALIZE_LIMIT;

    /**
     * @return the most rows one lookup may hold in temporary tables at once
     */
    static long materializeLimit() {
        return materializeLimit;
    }

    /**
     * Changes {@link #materializeLimit()}, so tests can reach it with small histories.
     *
     * @param rows
     *            the new limit, or 0 to go back to {@link #MATERIALIZE_LIMIT}
     */
    static void setMaterializeLimitForTesting(long rows) {
        materializeLimit = rows <= 0 ? MATERIALIZE_LIMIT : rows;
    }

    /** A read that would have written more rows to a temporary table than one lookup may hold. */
    static final class ReadLimitException extends SQLException {
        private static final long serialVersionUID = 1L;

        ReadLimitException(String table, long limit) {
            super("Reading " + table + " would hold more than " + limit + " rows in temporary storage");
        }
    }

    /**
     * A stretch of one table in the order a page is shown in, bounds included.
     *
     * <p>
     * A merged page is ordered by time and then by row id within each table, so a stretch of it is
     * bounded by a time and row id pair at each end; a page of one table is ordered by row id alone,
     * and the times are left open. Rows of the live table and packed rows are confined alike, so the
     * query sees exactly the rows between the two bounds.
     * </p>
     */
    public static final class KeyWindow {
        final boolean byTime;
        final long lowTime;
        final long lowRowId;
        final long highTime;
        final long highRowId;

        private KeyWindow(boolean byTime, long lowTime, long lowRowId, long highTime, long highRowId) {
            this.byTime = byTime;
            this.lowTime = lowTime;
            this.lowRowId = lowRowId;
            this.highTime = highTime;
            this.highRowId = highRowId;
        }

        /**
         * @return a stretch of rows between two time and row id pairs, both included; use
         *         {@link Long#MIN_VALUE} and {@link Long#MAX_VALUE} for an open end
         */
        static KeyWindow ofTime(long lowTime, long lowRowId, long highTime, long highRowId) {
            return new KeyWindow(true, lowTime, lowRowId, highTime, highRowId);
        }

        /**
         * @return a stretch of rows between two row ids, both included
         */
        static KeyWindow ofRowIds(long lowRowId, long highRowId) {
            return new KeyWindow(false, Long.MIN_VALUE, lowRowId, Long.MAX_VALUE, highRowId);
        }

        /**
         * @return true if a row with this time and id lies inside the stretch
         */
        boolean contains(long time, long rowId) {
            if (!byTime) {
                return rowId >= lowRowId && rowId <= highRowId;
            }
            boolean aboveLow = time > lowTime || (time == lowTime && rowId >= lowRowId);
            boolean belowHigh = time < highTime || (time == highTime && rowId <= highRowId);
            return aboveLow && belowHigh;
        }

        /**
         * @return true if no row can lie inside the stretch
         */
        boolean isEmpty() {
            if (!byTime) {
                return lowRowId > highRowId;
            }
            return lowTime > highTime || (lowTime == highTime && lowRowId > highRowId);
        }

        /**
         * @return true if the segment may hold a row inside the stretch
         */
        boolean mayHold(ColdSegment segment) {
            if (isEmpty()) {
                return false;
            }
            if (byTime) {
                return segment.maxTime >= lowTime && segment.minTime <= highTime;
            }
            return segment.endRowId >= lowRowId && segment.startRowId <= highRowId;
        }

        /**
         * @return the condition that confines the live table to the stretch, in SQL
         */
        String toSql() {
            if (isEmpty()) {
                return "0";
            }
            StringBuilder condition = new StringBuilder("1");
            if (byTime) {
                if (lowTime != Long.MIN_VALUE) {
                    condition.append(" AND time >= ").append(lowTime);
                    if (lowRowId != Long.MIN_VALUE) {
                        condition.append(" AND (time > ").append(lowTime).append(" OR rowid >= ").append(lowRowId).append(')');
                    }
                }
                if (highTime != Long.MAX_VALUE) {
                    condition.append(" AND time <= ").append(highTime);
                    if (highRowId != Long.MAX_VALUE) {
                        condition.append(" AND (time < ").append(highTime).append(" OR rowid <= ").append(highRowId).append(')');
                    }
                }
                return condition.toString();
            }
            if (lowRowId != Long.MIN_VALUE) {
                condition.append(" AND rowid >= ").append(lowRowId);
            }
            if (highRowId != Long.MAX_VALUE) {
                condition.append(" AND rowid <= ").append(highRowId);
            }
            return condition.toString();
        }

        /**
         * Builds the test packed rows are held to. Its ranges let whole groups of rows outside the
         * stretch go undecoded; the row test is exact.
         */
        ColdSegmentCodec.RowFilter filter(TableLayout layout) {
            int timeColumn = byTime ? layout.timeColumn : -1;
            return new ColdSegmentCodec.RowFilter() {
                @Override
                public boolean accept(long[] columns, boolean[] present) {
                    return true;
                }

                @Override
                public boolean acceptRow(long rowId, long[] columns, boolean[] present) {
                    if (!byTime) {
                        return contains(0, rowId);
                    }
                    // A row without a time fails every comparison the query makes on time.
                    return timeColumn >= 0 && present[timeColumn] && contains(columns[timeColumn], rowId);
                }

                @Override
                public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                    if (!byTime || timeColumn < 0 || !known[timeColumn]) {
                        return true;
                    }
                    return maximum[timeColumn] >= lowTime && minimum[timeColumn] <= highTime;
                }
            };
        }

        @Override
        public String toString() {
            return byTime ? "(" + lowTime + "," + lowRowId + ")..(" + highTime + "," + highRowId + ")" : lowRowId + ".." + highRowId;
        }
    }

    /**
     * @return true if compressed rows the lookup needed could not be read at all
     */
    public static boolean readFailed() {
        LookupContext context = LOOKUP.get();
        return context != null && context.readFailed;
    }

    /**
     * @return true if a budget stopped a lookup before every matching compressed row was read
     */
    public static boolean wasTruncated() {
        LookupContext context = LOOKUP.get();
        return context != null && context.truncated;
    }

    /**
     * Records the identifiers a lookup is restricted to, so segments that cannot hold any of them
     * are skipped and their rows are never decoded into the temporary table.
     *
     * @param users
     *            a comma separated list of user ids, or an empty string for no restriction
     * @param types
     *            a comma separated list of block type ids, or an empty string for no restriction
     */
    public static void setLookupFilters(String users, String types) {
        setLookupFilters(users, types, null);
    }

    /**
     * Records the identifiers a lookup is restricted to, so segments that cannot hold any of them
     * are skipped and their rows are never decoded into the temporary table.
     *
     * @param users
     *            a comma separated list of user ids, or an empty string for no restriction
     * @param types
     *            a comma separated list of block type ids, or an empty string for no restriction
     * @param actions
     *            a comma separated list of action ids, or null for no restriction
     */
    public static void setLookupFilters(String users, String types, String actions) {
        LookupContext context = LOOKUP.get();
        if (context == null) {
            return;
        }
        context.users = parseIdentifiers(users);
        context.types = parseIdentifiers(types);
        context.actions = parseIdentifiers(actions);
    }

    /**
     * Records the values a lookup asks to leave out, so the compressed reader applies the same NOT
     * IN tests the query does.
     *
     * @param users
     *            a comma separated list of user ids to exclude, or an empty string
     * @param types
     *            a comma separated list of block type ids to exclude, or an empty string
     * @param actionsByTable
     *            action ids to exclude, keyed by the unprefixed table they are excluded from
     */
    public static void setLookupExclusions(String users, String types, String actions) {
        LookupContext context = LOOKUP.get();
        if (context == null) {
            return;
        }
        context.excludedUsers = parseIdentifiers(users);
        context.excludedTypes = parseIdentifiers(types);
        context.excludedActions = parseIdentifiers(actions);
    }

    /**
     * Reads the restrictions a query places on one table straight out of the query itself, so the
     * compressed reader applies exactly what the query does.
     *
     * <p>
     * Each table of a lookup can be restricted differently: the same request may ask for one set of
     * actions from stored items and another from placed blocks. Deriving the restrictions from the
     * assembled clause rather than from the request means the two can never drift apart, which
     * matters most for counts, where the compressed rows are never handed to the query at all.
     * </p>
     *
     * @param where
     *            the assembled WHERE clause for this table
     * @param userColumn
     *            the name of the user column in this query
     */
    public static void setPredicateFilters(String where, String userColumn) {
        LookupContext context = LOOKUP.get();
        if (context == null) {
            return;
        }
        ColdPagePredicate parsed = where == null ? null : ColdPagePredicate.parse(where, userColumn);
        context.predicateUnmodelled = where == null || (ColdPagePredicate.hasAlternatives(where) && parsed == null);
        context.predicateGiven = true;
        context.predicate = context.predicateUnmodelled ? null : parsed;
        if (context.predicateUnmodelled) {
            // Alternatives the reader cannot test leave no part of the clause certain to be
            // required, and a count taken from the reader alone would describe more rows than the
            // query returns.
            context.users = null;
            context.types = null;
            context.actions = null;
            context.excludedUsers = null;
            context.excludedTypes = null;
            context.excludedActions = null;
            return;
        }

        // Only what every row has to satisfy restricts what is read; a value named in one of
        // several alternatives is not required of anything.
        String required = ColdPagePredicate.requiredPart(where);
        String user = userColumn == null || userColumn.isEmpty() ? "user" : userColumn;
        context.users = parseIdentifiers(listedValues(required, user, false));
        context.types = parseIdentifiers(listedValues(required, "type", false));
        context.actions = parseIdentifiers(listedValues(required, "action", false));
        context.excludedUsers = parseIdentifiers(listedValues(required, user, true));
        context.excludedTypes = parseIdentifiers(listedValues(required, "type", true));
        context.excludedActions = parseIdentifiers(listedValues(required, "action", true));
    }

    /**
     * @param where
     *            a WHERE clause
     * @param column
     *            the column to look for
     * @param negated
     *            true to read a NOT IN list, false to read an IN list
     * @return the listed values, or null when the clause does not restrict that column that way
     */
    static String listedValues(String where, String column, boolean negated) {
        // A column is restricted either to a list or, for a single action, by equality. Either way
        // the values are read out as one list; an inequality is not a restriction to anything.
        String restriction = negated
                ? "\\s+NOT\\s+IN\\s*\\(([-0-9,\\s]+)\\)"
                : "(?:\\s+IN\\s*\\(([-0-9,\\s]+)\\)|\\s*=\\s*(-?[0-9]+))";
        Matcher matcher = Pattern.compile("(?<![A-Za-z0-9_])" + Pattern.quote(column) + restriction).matcher(where);
        if (!matcher.find()) {
            return null;
        }
        String values = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
        if (matcher.find()) {
            return null; // the same column restricted twice cannot be reduced to one list
        }
        return values;
    }

    /**
     * @param users
     *            a comma separated list of user ids to leave out, or null
     * @param types
     *            a comma separated list of block type ids to leave out, or null
     * @param actions
     *            a comma separated list of action ids to leave out, or null
     * @return the exclusions those lists describe
     */
    static Exclusions exclusionsOf(String users, String types, String actions) {
        return new Exclusions(parseIdentifiers(users), parseIdentifiers(types), parseIdentifiers(actions));
    }

    private static Exclusions exclusions(LookupContext context) {
        if (context == null) {
            return Exclusions.NONE;
        }
        return new Exclusions(context.excludedUsers, context.excludedTypes, context.excludedActions);
    }

    private static long[] parseIdentifiers(String list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        String[] parts = list.split(",");
        long[] values = new long[parts.length];
        int count = 0;
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                values[count++] = Long.parseLong(trimmed);
            }
            catch (NumberFormatException exception) {
                // A list that cannot be read simply means no extra pruning.
                return null;
            }
        }
        return count == 0 ? null : Arrays.copyOf(values, count);
    }

    private static final ThreadLocal<LookupContext> LOOKUP = new ThreadLocal<>();

    /**
     * Keeps a lookup and the packing of rows from overlapping.
     *
     * <p>
     * A lookup decides which segments to read from the metadata in memory, and reads the live table
     * afterwards. Packing moves rows from the live table into a new segment in one transaction, and
     * the metadata learns of the segment just after. A lookup that chose its segments before a
     * segment was added and read the live table after its rows had left would find those rows in
     * neither place. Lookups hold this shared from start to finish; packing holds it exclusively
     * from the moment it removes rows until the metadata describes where they went.
     * </p>
     *
     * <p>
     * Packing only ever asks for it while it already holds the database's write lock, so a lookup
     * running inside a writer's transaction can never be waiting for packing that is in turn waiting
     * for that writer. Both sides wait a bounded time, and a lookup that cannot get it proceeds
     * anyway, so neither can stall the other indefinitely.
     * </p>
     */
    private static final StampedLock SEAL_LOCK = new StampedLock();

    /** The stamp of the current thread's hold on {@link #SEAL_LOCK}, while its lookup runs. */
    private static final ThreadLocal<Long> LOOKUP_STAMP = new ThreadLocal<>();

    /**
     * Every thread holding the lock for a lookup, with its stamp. A lookup that ends without
     * releasing it, on a thread that then exits, would otherwise hold packing off for good; a
     * stamped lock lets the hold be released from another thread once its owner is gone.
     */
    private static final ConcurrentHashMap<Thread, Long> LOOKUP_HOLDERS = new ConcurrentHashMap<>();

    /** The longest a lookup waits for packing to finish before reading anyway. */
    private static final long LOOKUP_WAIT_MILLIS = 5000L;

    /** The stamp of the thread holding the exclusive side, or 0. */
    private static volatile long sealStamp;

    /**
     * @return true if any lookup currently holds the lock, so packing can wait for it before it
     *         takes the database's write lock rather than while holding it
     */
    static boolean lookupsInProgress() {
        releaseAbandonedLookups();
        return SEAL_LOCK.isReadLocked();
    }

    /**
     * Takes the exclusive side of the lock between lookups and packing, waiting a bounded time for
     * lookups in progress to finish.
     *
     * @param timeoutMillis
     *            the longest to wait
     * @return true if the lock was taken and must be released with {@link #unlockAfterSeal()}
     * @throws InterruptedException
     *             if the wait is interrupted
     */
    static boolean lockForSeal(long timeoutMillis) throws InterruptedException {
        releaseAbandonedLookups();
        long stamp = SEAL_LOCK.tryWriteLock(timeoutMillis, TimeUnit.MILLISECONDS);
        if (stamp == 0) {
            return false;
        }
        sealStamp = stamp;
        return true;
    }

    /** Releases what {@link #lockForSeal(long)} took. */
    static void unlockAfterSeal() {
        long stamp = sealStamp;
        if (stamp != 0 && SEAL_LOCK.validate(stamp) && SEAL_LOCK.isWriteLocked()) {
            sealStamp = 0;
            SEAL_LOCK.unlockWrite(stamp);
        }
    }

    private static void lockForLookup() {
        if (!ConfigHandler.databaseType.isSQLite() || LOOKUP_STAMP.get() != null || (SEAL_LOCK.isWriteLocked() && sealStamp != 0 && isSealingThread())) {
            return;
        }
        try {
            long stamp = SEAL_LOCK.tryReadLock(LOOKUP_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            if (stamp != 0) {
                LOOKUP_STAMP.set(stamp);
                LOOKUP_HOLDERS.put(Thread.currentThread(), stamp);
            }
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static void unlockAfterLookup() {
        Long stamp = LOOKUP_STAMP.get();
        if (stamp == null) {
            return;
        }
        LOOKUP_STAMP.remove();
        LOOKUP_HOLDERS.remove(Thread.currentThread());
        SEAL_LOCK.unlockRead(stamp);
    }

    /** Lets go of holds left by lookups whose threads have gone, so they cannot hold packing off. */
    private static void releaseAbandonedLookups() {
        for (Map.Entry<Thread, Long> holder : LOOKUP_HOLDERS.entrySet()) {
            if (!holder.getKey().isAlive() && LOOKUP_HOLDERS.remove(holder.getKey(), holder.getValue())) {
                SEAL_LOCK.unlockRead(holder.getValue());
                debug("released the compressed storage hold of a lookup that ended without releasing it");
            }
        }
    }

    private static boolean isSealingThread() {
        // The sealing thread never runs lookups, but if it ever did, waiting for itself would
        // deadlock; the write lock is held, so the read side cannot be taken anyway.
        return Thread.currentThread().getName().startsWith("CoreProtect-StoragePacking") || Thread.currentThread().getName().startsWith("CoreProtect-Compact")
                || Thread.currentThread().getName().startsWith("CoreProtect-AutoPurge");
    }

    /**
     * Marks the start of a lookup, so the tables it reads from can take its time range into account
     * and the temporary tables it creates can be cleaned up afterwards.
     *
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0 for no lower bound
     * @param endTime
     *            the latest timestamp the lookup wants, or 0 for no upper bound
     */
    public static void beginLookup(long startTime, long endTime) {
        lockForLookup();
        LOOKUP.set(new LookupContext(startTime, endTime));
    }

    /**
     * Restricts a lookup to the rows of particular entities.
     *
     * <p>
     * Rows about an entity are found by the entity's row id rather than by where they happened, so
     * there are no coordinates to narrow the segments down with. Each segment records which entities
     * it holds rows for, which serves the same purpose: a lookup about one entity opens only the
     * segments that could hold something of it.
     * </p>
     *
     * @param spawnRowIds
     *            the entity spawn row ids wanted, or null for no restriction
     */
    public static void setSpawnFilter(long[] spawnRowIds) {
        LookupContext context = LOOKUP.get();
        if (context != null) {
            context.spawns = spawnRowIds;
        }
    }

    /**
     * Marks the end of a lookup and drops anything it materialized.
     *
     * @param connection
     *            the connection the lookup ran on, may be null
     */
    public static void endLookup(Connection connection) {
        LookupContext context = LOOKUP.get();
        LOOKUP.remove();
        try {
            if (context == null || connection == null) {
                return;
            }
            for (String table : context.materialized) {
                releaseMaterialized(connection, table);
            }
        }
        finally {
            unlockAfterLookup();
        }
    }

    /**
     * Returns the table expression a lookup should read from: the live table on its own when no
     * cold segment can match, or the live table unioned with the matching cold rows.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param table
     *            an unprefixed table name
     * @param worldId
     *            the world the lookup is restricted to, or 0 for any world
     * @param bounds
     *            the coordinate bounds of the lookup, or null
     * @return the table expression
     * @throws SQLException
     *             if the segments cannot be read
     */
    public static String sourceExpression(Connection connection, String table, int worldId, Integer[] bounds) throws SQLException {
        String hotTable = ConfigHandler.prefix + table;
        if (!ConfigHandler.databaseType.isSQLite()) {
            return hotTable;
        }
        LookupContext context = LOOKUP.get();
        KeyWindow window = context == null ? null : context.windows.get(table);
        // The live rows a window leaves out have to be left out of the query too, so a windowed
        // table is never read as the plain table, even when it holds no packed rows at all.
        String liveTable = window == null ? hotTable : "(SELECT rowid AS rowid,* FROM " + hotTable + " WHERE " + window.toSql() + ")";
        String liveOnly = window == null ? hotTable : liveTable + " AS " + table + "_rows";
        if (!SEGMENTED_TABLES.containsKey(table)) {
            return liveOnly;
        }

        ensureLoaded(connection);
        if (!hasSegments()) {
            return liveOnly;
        }

        // The clause describes the one table it was given for, so it is used for this read and
        // then forgotten, and a later table read without a clause of its own cannot inherit it.
        ColdPagePredicate predicate = context == null ? null : context.predicate;
        boolean predicateGiven = context != null && context.predicateGiven;
        if (context != null) {
            context.predicate = null;
            context.predicateGiven = false;
        }
        long startTime = context == null ? 0 : context.startTime;
        long endTime = context == null ? 0 : context.endTime;
        long[] users = context == null ? null : context.users;
        long[] types = context == null ? null : context.types;
        long[] actions = context == null ? null : context.actions;
        Exclusions excluded = exclusions(context);
        List<ColdSegment> selected = selectSegments(connection, table, worldId, bounds, startTime, endTime, null);
        selected = restrictToIdentifiers(selected, users, types, context == null ? null : context.spawns);
        if (window != null) {
            List<ColdSegment> inside = new ArrayList<>(selected.size());
            for (ColdSegment segment : selected) {
                if (window.mayHold(segment)) {
                    inside.add(segment);
                }
            }
            selected = inside;
        }
        if (selected.isEmpty()) {
            return liveOnly;
        }

        long started = System.nanoTime();
        if (context != null && context.counting && !context.predicateUnmodelled && window == null) {
            // Counting needs no rows, only how many there are, so nothing is built or copied. The
            // count has to describe exactly what the query keeps, which the lookup's own clause
            // does when there is one; without one, the reader's restrictions describe the query
            // only when it is not about a place, whose height and exact edges they do not know.
            ColdSegmentCodec.RowFilter clauseFilter = null;
            boolean countable = predicate == null ? bounds == null : false;
            boolean described = true;
            if (predicate != null) {
                TableLayout layout = layout(connection, table);
                ColdPagePredicate.Bound clause = layout == null ? null : predicate.bind(layout);
                if (clause != null) {
                    clauseFilter = clause.exactFilter(clause.testsRolledBack() ? readOverlay(connection, table, selected) : null);
                    described = countsDescribe(predicate, context, worldId, bounds);
                    countable = true;
                }
            }
            if (countable) {
                long counted = countRows(connection, table, selected, worldId, bounds, startTime, endTime, users, types, actions, excluded, described, clauseFilter);
                context.counts.put(table, counted);
                debug("count " + table + ": " + selected.size() + " of " + segments.getOrDefault(SEGMENTED_TABLES.get(table), Collections.emptyList()).size()
                        + " segments, " + counted + " rows, " + millis(started) + " ms");
                return hotTable;
            }
        }

        String expression;
        try {
            expression = materialize(connection, table, selected, worldId, bounds, startTime, endTime, users, types, actions, excluded, context, predicate, predicateGiven);
        }
        catch (SQLException exception) {
            releaseMaterialized(connection, table);
            if (exception instanceof ReadLimitException && context != null && context.divisible) {
                // The caller reads the history a window at a time and makes this one smaller.
                throw exception;
            }
            // Scratch space ran out, the read was larger than a lookup may hold, or the copy
            // failed part way. The lookup still has to answer, so it falls back to live rows alone
            // and says so rather than failing outright, and the cause is logged.
            if (context != null) {
                context.readFailed = true;
            }
            ErrorReporter.report(exception, true);
            debug("read " + table + ": could not be read (" + exception.getMessage() + "), live rows only");
            return liveOnly;
        }
        debug("read " + table + ": " + selected.size() + " of " + segments.getOrDefault(SEGMENTED_TABLES.get(table), Collections.emptyList()).size()
                + " segments, budget " + (context == null ? 0 : context.rowBudget) + (window == null ? "" : ", window " + window) + ", " + millis(started) + " ms"
                + ((context != null && context.truncated) ? " (stopped early)" : ""));
        if (context != null) {
            context.materialized.add(table);
        }
        return expression;
    }

    /**
     * Writes a line to the console when compressed storage debugging is switched on.
     *
     * @param message
     *            what happened
     */
    static void debug(String message) {
        if (Config.getGlobal().STORAGE_DEBUG) {
            net.coreprotect.utility.Chat.sendConsoleMessage(net.coreprotect.utility.Color.GREY + "[CoreProtect] cold: " + message);
        }
    }

    private static long millis(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000L;
    }

    /**
     * What planning a page needs to know about one table of a lookup: which packed segments can
     * hold its rows, how many of them each holds where the counts recorded at seal time say so,
     * and the test the decoder applies to find the rest.
     *
     * <p>
     * Everything here is derived from the very clause the query applies to the table, so a count
     * taken from it is a count of what the query keeps.
     * </p>
     */
    static final class TableCounts {
        final String table;
        final TableLayout layout;
        /** The segments that can hold a matching row, oldest first. */
        final List<ColdSegment> segments;
        /** Accepts exactly the packed rows the query keeps. */
        final ColdSegmentCodec.RowFilter filter;
        private final boolean described;
        private final int worldId;
        private final Integer[] bounds;
        private final int[] area;
        private final long startTime;
        private final long endTime;
        private final long[] users;
        private final long[] types;
        private final long[] actions;
        private final Exclusions excluded;

        private TableCounts(String table, TableLayout layout, List<ColdSegment> segments, ColdSegmentCodec.RowFilter filter, boolean described, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions,
                Exclusions excluded) {
            this.table = table;
            this.layout = layout;
            this.segments = segments;
            this.filter = filter;
            this.described = described;
            this.worldId = worldId;
            this.bounds = bounds;
            this.area = chunkArea(worldId, bounds);
            this.startTime = startTime;
            this.endTime = endTime;
            this.users = users;
            this.types = types;
            this.actions = actions;
            this.excluded = excluded;
        }

        /**
         * @return how many rows of the segment the query keeps, or -1 when only reading it can tell
         */
        long exactRows(ColdSegment segment) {
            return described ? SQLiteColdIndex.exactRows(segment, worldId, bounds, area, startTime, endTime, users, types, actions, excluded) : -1;
        }

        /**
         * @return a number of rows the segment cannot hold more matches than
         */
        long mostRows(ColdSegment segment) {
            long most = segment.rowCount;
            if (users != null && segment.userStats != null) {
                most = Math.min(most, segment.userStats.count(users));
            }
            if (types != null && segment.typeStats != null) {
                most = Math.min(most, segment.typeStats.count(types));
            }
            if (actions != null && segment.actionStats != null) {
                most = Math.min(most, segment.actionStats.count(actions));
            }
            if (area != null && segment.chunkCounts != null) {
                // Rows without a place never match a restriction on coordinates.
                most = Math.min(most, segment.chunkCounts.rowsIn(worldId, area[0], area[1], area[2], area[3]));
            }
            return most;
        }
    }

    /**
     * Reads the clause a query applies to one table into what planning a page needs.
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param where
     *            the clause the query applies to the table
     * @param userColumn
     *            the name of the user column in the clause
     * @param worldId
     *            the world the reader is restricted to for this table, or 0
     * @param bounds
     *            the coordinate bounds the reader is restricted to for this table, or null
     * @return the table's counts, or null when the reader cannot test the clause itself, which
     *         leaves only reading the rows to find out what it keeps
     * @throws SQLException
     *             if the metadata cannot be read
     */
    static TableCounts tableCounts(Connection connection, String table, String where, String userColumn, int worldId, Integer[] bounds) throws SQLException {
        if (where == null || where.contains("?")) {
            return null;
        }
        ColdPagePredicate predicate = ColdPagePredicate.parse(where, userColumn);
        TableLayout layout = layout(connection, table);
        if (predicate == null || layout == null || layout.timeColumn < 0 || layout.types[layout.timeColumn] != ColdSegmentCodec.TYPE_INTEGER) {
            return null;
        }
        ColdPagePredicate.Bound clause = predicate.bind(layout);
        if (clause == null) {
            return null;
        }

        // The same restrictions the reader takes from the clause when it reads, so segments are
        // chosen and counted exactly as a read of them would be.
        String user = userColumn == null || userColumn.isEmpty() ? "user" : userColumn;
        long startTime = 0;
        long endTime = 0;
        for (ColdPagePredicate.Condition condition : predicate.conditions()) {
            if (!"time".equals(condition.column)) {
                continue;
            }
            if (condition.operator == ColdPagePredicate.GREATER) {
                startTime = Math.max(startTime, condition.value);
            }
            else if (condition.operator == ColdPagePredicate.GREATER_OR_EQUAL && condition.value > 1) {
                // The reader's start is exclusive, so the second before is where it starts.
                startTime = Math.max(startTime, condition.value - 1);
            }
            else if (condition.operator == ColdPagePredicate.LESS_OR_EQUAL) {
                endTime = endTime == 0 ? condition.value : Math.min(endTime, condition.value);
            }
            else if (condition.operator == ColdPagePredicate.LESS && condition.value > 1) {
                endTime = endTime == 0 ? condition.value - 1 : Math.min(endTime, condition.value - 1);
            }
        }
        LookupContext view = new LookupContext(startTime, endTime);
        String required = ColdPagePredicate.requiredPart(where);
        view.users = parseIdentifiers(listedValues(required, user, false));
        view.types = parseIdentifiers(listedValues(required, "type", false));
        view.actions = parseIdentifiers(listedValues(required, "action", false));
        view.excludedUsers = parseIdentifiers(listedValues(required, user, true));
        view.excludedTypes = parseIdentifiers(listedValues(required, "type", true));
        view.excludedActions = parseIdentifiers(listedValues(required, "action", true));
        Exclusions excluded = exclusions(view);
        // A lookup about one entity names it in its clause, and only the segments that can hold
        // rows for it are worth counting.
        long[] spawns = layout.spawnColumn >= 0 ? parseIdentifiers(listedValues(required, layout.columns[layout.spawnColumn], false)) : null;

        List<ColdSegment> selected = SEGMENTED_TABLES.containsKey(table) ? selectSegments(connection, table, worldId, bounds, startTime, endTime, null) : Collections.emptyList();
        selected = restrictToIdentifiers(selected, view.users, view.types, spawns);
        // A row's rolled back state may have changed since it was packed, and the change is what
        // the query sees, so it is what the rows are counted by.
        Map<Long, Integer> overlay = clause.testsRolledBack() ? readOverlay(connection, table, selected) : Collections.emptyMap();
        ColdSegmentCodec.RowFilter filter = both(rowFilter(layout, worldId, bounds, startTime, endTime, view.users, view.types, view.actions, excluded), clause.exactFilter(overlay));
        boolean described = countsDescribe(predicate, view, worldId, bounds);
        return new TableCounts(table, layout, selected, filter, described, worldId, bounds, startTime, endTime, view.users, view.types, view.actions, excluded);
    }

    /**
     * Chooses the segments a lookup of one table could read, by where and when the lookup is and
     * with nothing else known about what it keeps.
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param worldId
     *            the world the lookup is restricted to, or 0
     * @param bounds
     *            the coordinate bounds of the lookup, or null
     * @return the segments, oldest first
     * @throws SQLException
     *             if the metadata cannot be read
     */
    static List<ColdSegment> candidateSegments(Connection connection, String table, int worldId, Integer[] bounds) throws SQLException {
        if (!SEGMENTED_TABLES.containsKey(table)) {
            return Collections.emptyList();
        }
        LookupContext context = LOOKUP.get();
        long startTime = context == null ? 0 : context.startTime;
        long endTime = context == null ? 0 : context.endTime;
        List<ColdSegment> selected = selectSegments(connection, table, worldId, bounds, startTime, endTime, null);
        return restrictToIdentifiers(selected, null, null, context == null ? null : context.spawns);
    }

    /**
     * Counts, segment by segment, the rows a filter accepts, from the scalar frames alone and
     * several segments at a time.
     *
     * @return the count for each segment, in the order given
     * @throws SQLException
     *             if a segment cannot be read
     */
    static long[] countEach(Connection connection, List<ColdSegment> segments, ColdSegmentCodec.RowFilter filter) throws SQLException {
        long[] counts = new long[segments.size()];
        for (int wave = 0; wave < segments.size(); wave += DECODE_WAVE) {
            List<ColdSegment> batch = segments.subList(wave, Math.min(segments.size(), wave + DECODE_WAVE));
            List<java.util.concurrent.Callable<Integer>> counting = new ArrayList<>(batch.size());
            for (SegmentFrames read : readBatch(connection, batch, filter)) {
                counting.add(() -> {
                    ColdSegmentReadCounter.recordRead();
                    return ColdSegmentCodec.count(read.scalars(), read.units, filter, read.columns);
                });
            }
            List<Integer> counted = runAll(counting);
            for (int index = 0; index < counted.size(); index++) {
                counts[wave + index] = counted.get(index);
            }
        }
        return counts;
    }

    /**
     * Hands over the id and time of every row a filter accepts, from the scalar frames alone, a
     * few segments at a time so that no more than a few are held at once.
     *
     * @param timeColumn
     *            the column holding each row's time, or -1 for ids alone
     * @param consumer
     *            given the rows of each segment in turn, on the calling thread
     * @throws SQLException
     *             if a segment cannot be read
     */
    static void scanKeys(Connection connection, List<ColdSegment> segments, ColdSegmentCodec.RowFilter filter, int timeColumn, java.util.function.Consumer<ColdSegmentCodec.KeyRows> consumer) throws SQLException {
        for (int wave = 0; wave < segments.size(); wave += DECODE_WAVE) {
            List<ColdSegment> batch = segments.subList(wave, Math.min(segments.size(), wave + DECODE_WAVE));
            List<java.util.concurrent.Callable<ColdSegmentCodec.KeyRows>> scanning = new ArrayList<>(batch.size());
            for (SegmentFrames read : readBatch(connection, batch, filter)) {
                scanning.add(() -> {
                    ColdSegmentReadCounter.recordRead();
                    return ColdSegmentCodec.keys(read.scalars(), read.units, filter, timeColumn, read.columns);
                });
            }
            for (ColdSegmentCodec.KeyRows keys : runAll(scanning)) {
                consumer.accept(keys);
            }
        }
    }

    /**
     * Drops segments that cannot hold any of the users or block types a lookup asked for.
     *
     * @param selected
     *            the segments chosen so far
     * @param users
     *            the user ids the lookup wants, or null
     * @param types
     *            the block type ids the lookup wants, or null
     * @return the segments still worth reading
     */
    private static List<ColdSegment> restrictToIdentifiers(List<ColdSegment> selected, long[] users, long[] types, long[] spawns) {
        if ((users == null && types == null && spawns == null) || selected.isEmpty()) {
            return selected;
        }

        List<ColdSegment> kept = new ArrayList<>(selected.size());
        for (ColdSegment segment : selected) {
            if (users != null && segment.userStats != null && segment.userStats.count(users) == 0) {
                continue;
            }
            if (types != null && segment.typeStats != null && segment.typeStats.count(types) == 0) {
                continue;
            }
            if (users != null && segment.userStats == null && segment.userFilter != null && !segment.userFilter.mightContainAny(users)) {
                continue;
            }
            if (types != null && segment.typeStats == null && segment.typeFilter != null && !segment.typeFilter.mightContainAny(types)) {
                continue;
            }
            // A lookup about one entity is answered by the few segments that hold rows for it, which
            // is what makes reading them affordable when there are no coordinates to go on.
            if (spawns != null && segment.spawnFilter != null && !segment.spawnFilter.mightContainAny(spawns)) {
                continue;
            }
            kept.add(segment);
        }
        return kept;
    }

    /**
     * Tells whether any packed row about an entity may belong to a given entity spawn.
     *
     * <p>
     * Segments record which entities they hold rows for as a set that can only err towards "maybe",
     * so a false answer is certain and a true one is only likely. A segment written before those
     * sets existed could hold anything, and answers true.
     * </p>
     *
     * @param spawnRowId
     *            an entity spawn row id
     * @return true unless no segment can hold a row for that entity
     */
    static boolean spawnMayBeReferenced(long spawnRowId) {
        for (String table : new String[] { "entity_container", "entity_interaction" }) {
            Integer tableId = SEGMENTED_TABLES.get(table);
            for (ColdSegment segment : segments.getOrDefault(tableId, Collections.emptyList())) {
                if (segment.spawnFilter == null || segment.spawnFilter.mightContain(spawnRowId)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param connection
     *            an open connection
     * @param id
     *            a segment id
     * @return that segment, or null when it is not loaded
     * @throws SQLException
     *             if the metadata cannot be read
     */
    static ColdSegment segmentById(Connection connection, long id) throws SQLException {
        ensureLoaded(connection);
        for (List<ColdSegment> tableSegments : segments.values()) {
            for (ColdSegment segment : tableSegments) {
                if (segment.id == id) {
                    return segment;
                }
            }
        }
        return null;
    }

    /**
     * @param table
     *            an unprefixed table name
     * @return the highest row id that has been rolled into a segment, or 0 when there is none
     */
    public static long coldHighWaterMark(String table) {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null) {
            return 0;
        }
        long highWater = 0;
        for (ColdSegment segment : segments.getOrDefault(tableId, Collections.emptyList())) {
            highWater = Math.max(highWater, segment.endRowId);
        }
        return highWater;
    }

    /**
     * Chooses the segments a lookup has to read.
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param worldId
     *            the world the lookup is restricted to, or 0 for any world
     * @param bounds
     *            the coordinate bounds of the lookup, or null when it is not location restricted
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0 for no lower bound
     * @param endTime
     *            the latest timestamp the lookup wants, or 0 for no upper bound
     * @param rowIds
     *            specific row ids the lookup is fetching, or null
     * @return the segments to read, oldest first
     * @throws SQLException
     *             if the metadata cannot be read
     */
    public static List<ColdSegment> selectSegments(Connection connection, String table, int worldId, Integer[] bounds, long startTime, long endTime, List<Long> rowIds) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null) {
            return Collections.emptyList();
        }

        ensureLoaded(connection);
        List<ColdSegment> candidates = segments.getOrDefault(tableId, Collections.emptyList());
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        int[] area = chunkArea(worldId, bounds);
        long[] chunkKeys = null;
        List<ColdSegment> selected = new ArrayList<>();
        for (ColdSegment segment : candidates) {
            if (startTime > 0 && segment.maxTime < startTime) {
                continue;
            }
            if (endTime > 0 && segment.minTime > endTime) {
                continue;
            }
            if (rowIds != null && !containsRowId(segment, rowIds)) {
                continue;
            }
            if (worldId > 0 && segment.worldIds.length > 0 && !contains(segment.worldIds, worldId)) {
                continue;
            }
            if (area != null) {
                if (segment.chunkCounts != null) {
                    if (!segment.chunkCounts.mayHoldAny(worldId, area[0], area[1], area[2], area[3])) {
                        continue;
                    }
                }
                else if (segment.chunkFilter != null) {
                    // A segment written before chunks were counted falls back to its filter, which
                    // needs the lookup's chunks one by one; they are only listed once one needs them.
                    if (chunkKeys == null) {
                        chunkKeys = chunkKeys(worldId, bounds);
                    }
                    if (chunkKeys != null && !matchesChunk(segment, chunkKeys)) {
                        continue;
                    }
                }
            }
            selected.add(segment);
        }

        return selected;
    }

    private static boolean containsRowId(ColdSegment segment, List<Long> rowIds) {
        for (Long rowId : rowIds) {
            if (rowId >= segment.startRowId && rowId <= segment.endRowId) {
                return true;
            }
        }
        return false;
    }

    private static boolean contains(int[] values, int value) {
        for (int candidate : values) {
            if (candidate == value) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesChunk(ColdSegment segment, long[] chunkKeys) {
        for (long key : chunkKeys) {
            if (segment.chunkFilter.mightContain(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the rectangle of chunks a location restricted lookup covers, as the lowest and highest
     *         chunk x and then the lowest and highest chunk z, or null when the lookup is not about
     *         one world and a rectangle of it
     */
    static int[] chunkArea(int worldId, Integer[] bounds) {
        if (worldId <= 0 || bounds == null || bounds.length < 7 || bounds[1] == null || bounds[2] == null || bounds[5] == null || bounds[6] == null) {
            return null;
        }
        return new int[] { SegmentFilter.chunkOf(bounds[1]), SegmentFilter.chunkOf(bounds[2]), SegmentFilter.chunkOf(bounds[5]), SegmentFilter.chunkOf(bounds[6]) };
    }

    /**
     * Builds the chunk keys a location restricted lookup covers, or null when there are too many to
     * be worth testing.
     */
    static long[] chunkKeys(int worldId, Integer[] bounds) {
        if (worldId <= 0 || bounds == null || bounds.length < 7) {
            return null;
        }

        int minChunkX = SegmentFilter.chunkOf(bounds[1]);
        int maxChunkX = SegmentFilter.chunkOf(bounds[2]);
        int minChunkZ = SegmentFilter.chunkOf(bounds[5]);
        int maxChunkZ = SegmentFilter.chunkOf(bounds[6]);
        long width = (long) maxChunkX - minChunkX + 1;
        long depth = (long) maxChunkZ - minChunkZ + 1;
        if (width <= 0 || depth <= 0 || width * depth > 4096) {
            return null;
        }

        long[] keys = new long[(int) (width * depth)];
        int index = 0;
        for (int chunkX = minChunkX; chunkX <= maxChunkX; chunkX++) {
            for (int chunkZ = minChunkZ; chunkZ <= maxChunkZ; chunkZ++) {
                keys[index++] = SegmentFilter.chunkKey(worldId, chunkX, chunkZ);
            }
        }
        return keys;
    }

    /**
     * @param bounds
     *            the coordinate bounds of a lookup
     * @return true if the bounds take in whole chunks and nothing else, and leave the height open,
     *         so the rows a segment holds in the chunks they cover are exactly the rows they match
     */
    static boolean coversWholeChunks(Integer[] bounds) {
        if (bounds == null || bounds.length < 7 || bounds[1] == null || bounds[2] == null || bounds[5] == null || bounds[6] == null) {
            return false;
        }
        if (bounds[3] != null || bounds[4] != null) {
            return false; // the height is not summarised, so a restriction on it cannot be counted
        }
        return Math.floorMod(bounds[1], 16) == 0 && Math.floorMod((long) bounds[2] + 1, 16L) == 0 && Math.floorMod(bounds[5], 16) == 0
                && Math.floorMod((long) bounds[6] + 1, 16L) == 0 && bounds[1] <= bounds[2] && bounds[5] <= bounds[6];
    }

    /**
     * Decodes the given segments into a temporary table and returns a table expression that reads
     * the live table and the decoded rows together.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param table
     *            an unprefixed table name
     * @param selected
     *            the segments to decode
     * @return the table expression to select from
     * @throws SQLException
     *             if the segments cannot be decoded
     */
    public static String materialize(Connection connection, String table, List<ColdSegment> selected) throws SQLException {
        return materialize(connection, table, selected, 0, null, 0, 0, null, null);
    }

    /**
     * Decodes the given segments into a temporary table, keeping only rows that can match.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param table
     *            an unprefixed table name
     * @param selected
     *            the segments to decode
     * @param worldId
     *            the world the lookup is restricted to, or 0 for any world
     * @param bounds
     *            the coordinate bounds of the lookup, or null
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0 for no lower bound
     * @param endTime
     *            the latest timestamp the lookup wants, or 0 for no upper bound
     * @return the table expression to select from
     * @throws SQLException
     *             if the segments cannot be decoded
     */
    public static String materialize(Connection connection, String table, List<ColdSegment> selected, int worldId, Integer[] bounds, long startTime, long endTime) throws SQLException {
        return materialize(connection, table, selected, worldId, bounds, startTime, endTime, null, null);
    }

    /**
     * Decodes the given segments into a temporary table, keeping only the rows that can match the
     * lookup. The full predicate still runs in SQL afterwards, so keeping too much is merely slower,
     * never wrong; the bounds passed here just avoid paying for rows the lookup cannot want.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param table
     *            an unprefixed table name
     * @param selected
     *            the segments to decode
     * @param worldId
     *            the world the lookup is restricted to, or 0 for any world
     * @param bounds
     *            the coordinate bounds of the lookup, or null
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0 for no lower bound
     * @param endTime
     *            the latest timestamp the lookup wants, or 0 for no upper bound
     * @return the table expression to select from
     * @throws SQLException
     *             if the segments cannot be decoded
     */
    public static String materialize(Connection connection, String table, List<ColdSegment> selected, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types) throws SQLException {
        return materialize(connection, table, selected, worldId, bounds, startTime, endTime, users, types, null, Exclusions.NONE, null, null, false);
    }

    private static String materialize(Connection connection, String table, List<ColdSegment> selected, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded, LookupContext context, ColdPagePredicate predicate, boolean predicateGiven) throws SQLException {
        TableLayout layout = layout(connection, table);
        if (layout == null) {
            return ConfigHandler.prefix + table;
        }
        String temporary = "cp_cold_" + table;
        String hotTable = ConfigHandler.prefix + table;
        KeyWindow window = context == null ? null : context.windows.get(table);

        // One transaction for the whole scratch table: committing every batch separately costs far
        // more than decoding the rows does. The transaction is asked for in SQL, as a savepoint,
        // rather than through the driver's auto-commit switch. The consumer opens its own
        // transaction with a plain BEGIN statement that the driver knows nothing about, and a
        // lookup run from inside one of those batches would have the driver begin a second
        // transaction, which SQLite refuses. A savepoint nests inside whatever is already open and
        // folds back into it when released, and on an idle connection it is the transaction, so
        // releasing it commits.
        boolean completed = false;
        beginScratch(connection);
        try {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE IF EXISTS temp." + temporary);
                statement.executeUpdate("CREATE TEMP TABLE " + temporary + " (" + columnDefinition(layout) + ")");
            }

            StringBuilder insert = new StringBuilder("INSERT INTO temp." + temporary + " (rowid");
            for (String column : layout.columns) {
                insert.append(',').append(column);
            }
            insert.append(") VALUES (?");
            for (int index = 0; index < layout.columns.length; index++) {
                insert.append(",?");
            }
            insert.append(')');

            Map<Long, Integer> overlay = readOverlay(connection, table, selected);
            int inserted = 0;
            long held = context == null ? 0 : context.heldRows;
            long limit = materializeLimit;
            try (PreparedStatement statement = connection.prepareStatement(insert.toString())) {
                long readStart = startTime;
                long readEnd = endTime;
                if (window != null && window.byTime) {
                    // The window's times go into the ordinary time test as well, which is what lets
                    // whole groups of rows outside it go undecoded.
                    if (window.lowTime != Long.MIN_VALUE) {
                        readStart = Math.max(readStart, window.lowTime - 1);
                    }
                    if (window.highTime != Long.MAX_VALUE) {
                        readEnd = readEnd > 0 ? Math.min(readEnd, window.highTime) : window.highTime;
                    }
                }
                ColdSegmentCodec.RowFilter rowFilter = rowFilter(layout, worldId, bounds, readStart, readEnd, users, types, actions, excluded,
                        context == null ? null : context.spawns);
                if (window != null) {
                    rowFilter = both(rowFilter, window.filter(layout));
                }
                long budget = context == null ? 0 : context.rowBudget;
                List<ColdSegment> order = selected;
                int[] area = chunkArea(worldId, bounds);

                // Every condition of the query that the decoder can test is tested while it reads, so
                // rows the query would reject are never built, whichever way they are kept.
                ColdPagePredicate.Bound clause = predicate == null ? null : predicate.bind(layout);
                if (clause != null) {
                    rowFilter = both(rowFilter, clause.exactFilter(overlay));
                }
                if (keepsTopRows(context, layout, clause, budget)) {
                    // The page's own order decides which compressed rows it can show, so only those
                    // are written. Nothing is left for the reads below.
                    inserted = insertTopRows(connection, statement, layout, selected, rowFilter, clause, overlay, budget, context, worldId, area, limit - held);
                    order = Collections.emptyList();
                    budget = 0;
                }

                if (budget > 0) {
                    // Newest first, so the rows a page can show are read before anything older.
                    order = new ArrayList<>(selected);
                    order.sort(Comparator.comparingLong((ColdSegment segment) -> segment.endRowId).reversed());

                    // Where the row counts are known, take exactly the segments the page needs. The
                    // rest are left unread, which is what keeps a page cheap on a long history.
                    long estimated = 0;
                    int needed = 0;
                    boolean countsKnown = true;
                    // Taken a row range at a time, for the same reason segments are skipped that way:
                    // the newest rows of a compacted run can lie in any of its segments.
                    while (needed < order.size() && countsKnown && estimated < budget) {
                        int end = sharedRangeEnd(order, needed);
                        for (int index = needed; index < end; index++) {
                            // Only counts that describe this lookup exactly may be trusted here. A
                            // segment the lookup cuts across holds fewer matching rows than its own
                            // count says, and trusting it would stop the read before the page was
                            // covered.
                            long rows = exactRows(order.get(index), worldId, bounds, area, startTime, endTime, users, types, actions, excluded);
                            if (rows < 0) {
                                countsKnown = false;
                                break;
                            }
                            estimated = estimated + rows;
                        }
                        needed = end;
                    }

                    if (countsKnown && needed < order.size()) {
                        context.truncated = true;
                        order = order.subList(0, needed);
                    }
                }
                // A small page can afford to decode several segments at once; a large read cannot.
                // Neither can a read of everything, unless it is about one place or one entity: then
                // each segment yields only the handful of rows kept there, and decoding is all the
                // work there is.
                boolean narrow = bounds != null || (context != null && context.spawns != null) || window != null;
                int wave = (budget > 0 && budget <= LARGE_READ_ROWS) || narrow ? DECODE_WAVE : 1;
                // A read with a budget goes newest first and may stop once it has enough rows, but
                // only where one row range ends and the next begins: the segments of a compacted run
                // share its range, and stopping part way through them could leave out rows newer
                // than some already read.
                boolean[] rangeStarts = new boolean[order.size()];
                if (budget > 0) {
                    for (int start = 0; start < order.size(); start = sharedRangeEnd(order, start)) {
                        rangeStarts[start] = true;
                    }
                }
                for (int position = 0; position < order.size(); position += wave) {
                    if (budget > 0 && inserted >= budget && rangeStarts[position]) {
                        context.truncated = true;
                        break;
                    }
                    List<ColdSegment> batch = order.subList(position, Math.min(order.size(), position + wave));
                    for (ColdSegmentCodec.Rows rows : decodeBatch(connection, batch, rowFilter)) {
                        for (int row = 0; row < rows.size(); row++) {
                            if (held + inserted >= limit) {
                                // Refused before the row is written, so no read ever holds more.
                                throw new ReadLimitException(table, limit);
                            }
                            long rowId = rows.getRowId(row);
                            Integer overlayFlag = layout.rolledBackColumn >= 0 ? overlay.get(rowId) : null;
                            bindRow(statement, layout, rowId, rows.getValues(row), overlayFlag);
                            statement.addBatch();
                            inserted++;
                            if (inserted % 2000 == 0) {
                                statement.executeBatch();
                            }
                        }
                    }
                }
                statement.executeBatch();
            }
            ColdSegmentReadCounter.recordScratchRows(inserted);
            ColdSegmentReadCounter.recordHeldRows(held + inserted);
            if (context != null) {
                context.heldRows = context.heldRows + inserted;
            }
            completed = true;
        }
        finally {
            endScratch(connection, completed);
        }

        String live = "SELECT rowid AS rowid,* FROM " + hotTable + (window == null ? "" : " WHERE " + window.toSql());
        return "(" + live + " UNION ALL SELECT rowid AS rowid,* FROM temp." + temporary + ") AS " + table + "_rows";
    }

    /**
     * Finds where the segments sharing rows with one segment end, in a list ordered newest first.
     *
     * <p>
     * Packing writes segments whose row ranges never overlap, so the newest segment holds the newest
     * rows and a page can be planned a segment at a time. Compaction replaces a run of them with a
     * few segments ordered by place across the whole run, and each of those spans the run's whole
     * range: the run's newest rows can lie in any of them. Planning therefore goes a row range at a
     * time, taking every segment that overlaps the range together, which for a segment that was
     * never compacted is just that segment.
     * </p>
     *
     * @param newestFirst
     *            segments ordered by their highest row id, highest first
     * @param start
     *            the position of the first segment of a range
     * @return the position just past the last segment overlapping that range
     */
    static int sharedRangeEnd(List<ColdSegment> newestFirst, int start) {
        long lowest = newestFirst.get(start).startRowId;
        int end = start + 1;
        while (end < newestFirst.size() && newestFirst.get(end).endRowId >= lowest) {
            lowest = Math.min(lowest, newestFirst.get(end).startRowId);
            end++;
        }
        return end;
    }

    /**
     * Sets the parameters of the scratch table insert for one row.
     *
     * @param statement
     *            the prepared insert
     * @param layout
     *            the column layout of the table
     * @param rowId
     *            the row's id
     * @param values
     *            the row's values, in table column order
     * @param overlayFlag
     *            the rolled back state recorded after the row was packed, or null
     * @throws SQLException
     *             if a parameter cannot be set
     */
    private static void bindRow(PreparedStatement statement, TableLayout layout, long rowId, Object[] values, Integer overlayFlag) throws SQLException {
        statement.setLong(1, rowId);
        for (int column = 0; column < layout.columns.length; column++) {
            Object value = column == layout.rolledBackColumn && overlayFlag != null ? Long.valueOf(overlayFlag.longValue()) : values[column];
            if (value == null) {
                statement.setNull(column + 2, java.sql.Types.NULL);
            }
            else if (value instanceof byte[]) {
                statement.setBytes(column + 2, (byte[]) value);
            }
            else if (value instanceof String) {
                statement.setString(column + 2, (String) value);
            }
            else if (value instanceof Double) {
                statement.setDouble(column + 2, (Double) value);
            }
            else {
                statement.setLong(column + 2, ((Number) value).longValue());
            }
        }
    }

    /**
     * Decides whether a read can keep only the compressed rows its page can show.
     *
     * <p>
     * That takes three things. The page has to want a known number of rows, and few enough that
     * holding them in memory while the rest are read is affordable. The reader has to be able to
     * test every condition of the query, or it could keep a row the query then rejects in place of
     * one the page needed. And the order the page is shown in has to be known, since that is what
     * decides which rows rank high enough to appear.
     * </p>
     */
    private static boolean keepsTopRows(LookupContext context, TableLayout layout, ColdPagePredicate.Bound clause, long budget) {
        if (context == null || clause == null || context.predicateUnmodelled || context.pageOrder == null) {
            return false;
        }
        if (budget <= 0 || budget > LARGE_READ_ROWS) {
            return false;
        }
        return context.pageOrder == PageOrder.ROW_ID || (layout.timeColumn >= 0 && layout.types[layout.timeColumn] == ColdSegmentCodec.TYPE_INTEGER);
    }

    /** A compressed row a page may show, with the key the page orders it by. */
    private static final class RankedRow {
        private final long time;
        private final long rowId;
        private final Object[] values;
        private final Integer overlayFlag;

        private RankedRow(long time, long rowId, Object[] values, Integer overlayFlag) {
            this.time = time;
            this.rowId = rowId;
            this.values = values;
            this.overlayFlag = overlayFlag;
        }
    }

    /**
     * Writes to the scratch table only the compressed rows that can appear on the page.
     *
     * <p>
     * Compressed rows all have lower row ids than live rows, but a page merged by time can still
     * rank a compressed row above a live one, so which compressed rows the page shows depends on
     * the live rows too. What does not depend on them is this: a compressed row that at least
     * {@code budget} other matching compressed rows outrank can never be within the first
     * {@code budget} rows of the page, however the live rows fall. Keeping the {@code budget}
     * highest ranked matching rows, in exactly the order the page uses, therefore gives the query
     * every compressed row it can show, and writing only those is what keeps a page from paying to
     * copy and sort a player's whole history.
     * </p>
     *
     * <p>
     * Segments are read best first by the highest key any of their rows could have. Once enough
     * rows are held, a segment whose best possible row ranks no higher than the weakest row held
     * cannot change the page, and nor can any after it, so reading stops there. Nothing that could
     * reach the page is left unread, which is why such a read is never reported as cut short.
     * </p>
     *
     * @return how many rows were written
     */
    private static int insertTopRows(Connection connection, PreparedStatement statement, TableLayout layout, List<ColdSegment> selected, ColdSegmentCodec.RowFilter rowFilter, ColdPagePredicate.Bound clause, Map<Long, Integer> overlay, long budget, LookupContext context, int worldId, int[] area, long room) throws SQLException {
        boolean byTime = context.pageOrder == PageOrder.TIME_THEN_ROW_ID;
        List<ColdSegment> order = new ArrayList<>(selected);
        if (byTime) {
            order.sort(Comparator.comparingLong((ColdSegment segment) -> segment.maxTime).thenComparingLong(segment -> segment.endRowId).reversed());
        }
        else {
            order.sort(Comparator.comparingLong((ColdSegment segment) -> segment.endRowId).reversed());
        }

        // The weakest row held is always at the head, ready to be displaced by a better one.
        Comparator<RankedRow> rank = Comparator.comparingLong((RankedRow row) -> row.time).thenComparingLong(row -> row.rowId);
        PriorityQueue<RankedRow> kept = new PriorityQueue<>((int) Math.min(budget, 4096), rank);
        // Rows holding a value only the query can compare are handed to it whatever they rank,
        // and never take a place a row the reader can rank would otherwise have.
        List<RankedRow> undecided = new ArrayList<>();
        long matched = 0;
        int read = 0;

        int position = 0;
        while (position < order.size()) {
            // Decode just enough segments at once to fill what is still missing, going by how many
            // matching rows each was recorded to hold, and none that cannot outrank what is held.
            int end = position;
            long expected = 0;
            while (end < order.size() && end - position < DECODE_WAVE) {
                ColdSegment segment = order.get(end);
                if (kept.size() >= budget ? !outranks(segment, kept.peek(), byTime) : expected >= budget - kept.size()) {
                    break;
                }
                long estimate = segment.estimatedRows(context.users, context.types);
                if (area != null && segment.chunkCounts != null) {
                    // A lookup about a place finds at most the rows a segment holds in its chunks,
                    // which is usually a handful, so several such segments are decoded together.
                    estimate = Math.min(estimate, segment.chunkCounts.rowsIn(worldId, area[0], area[1], area[2], area[3]));
                }
                expected = expected + estimate;
                end++;
            }
            if (end == position) {
                break;
            }

            List<ColdSegment> batch = order.subList(position, end);
            read = read + batch.size();
            for (ColdSegmentCodec.Rows rows : decodeBatch(connection, batch, rowFilter)) {
                for (int row = 0; row < rows.size(); row++) {
                    long rowId = rows.getRowId(row);
                    Object[] values = rows.getValues(row);
                    Integer overlayFlag = layout.rolledBackColumn >= 0 ? overlay.get(rowId) : null;
                    int verdict = clause.evaluate(values, overlayFlag);
                    if (verdict == ColdPagePredicate.NO_MATCH) {
                        continue;
                    }
                    matched++;

                    long time = 0;
                    if (byTime) {
                        Object value = values[layout.timeColumn];
                        if (value instanceof Long) {
                            time = (Long) value;
                        }
                        else {
                            verdict = ColdPagePredicate.UNKNOWN;
                        }
                    }
                    RankedRow ranked = new RankedRow(time, rowId, values, overlayFlag);
                    if (verdict == ColdPagePredicate.UNKNOWN) {
                        undecided.add(ranked);
                    }
                    else if (kept.size() < budget) {
                        kept.add(ranked);
                    }
                    else if (rank.compare(ranked, kept.peek()) > 0) {
                        kept.poll();
                        kept.add(ranked);
                    }
                }
            }
            position = end;
        }

        // Written in row id order, which is the order the scratch table stores them in, so each
        // row is appended rather than slotted in between rows written before it.
        List<RankedRow> chosen = new ArrayList<>(kept.size() + undecided.size());
        chosen.addAll(kept);
        chosen.addAll(undecided);
        chosen.sort(Comparator.comparingLong((RankedRow row) -> row.rowId));
        if (chosen.size() > room) {
            throw new ReadLimitException("a page", materializeLimit);
        }
        int inserted = 0;
        for (RankedRow ranked : chosen) {
            bindRow(statement, layout, ranked.rowId, ranked.values, ranked.overlayFlag);
            statement.addBatch();
            inserted++;
            if (inserted % 2000 == 0) {
                statement.executeBatch();
            }
        }
        debug("page " + (byTime ? "by time" : "by row id") + ": read " + read + " of " + order.size() + " segments, kept " + inserted + " of " + matched
                + " matching rows for a budget of " + budget);
        return inserted;
    }

    /**
     * @return true if some row of the segment could rank above the given row in the page's order
     */
    private static boolean outranks(ColdSegment segment, RankedRow weakest, boolean byTime) {
        if (!byTime) {
            return segment.endRowId > weakest.rowId;
        }
        // No row of the segment is newer than its newest time, nor has a higher row id than its last.
        return segment.maxTime > weakest.time || (segment.maxTime == weakest.time && segment.endRowId > weakest.rowId);
    }

    /**
     * Tells whether the counts recorded when segments were sealed describe exactly the rows the
     * query keeps, which is what skipping segments by arithmetic depends on.
     *
     * <p>
     * The counts only know about users, block types, actions, time, world and chunk, and only as
     * the reader was told them. A query that also asks about anything else, such as height or the
     * rolled back state, keeps fewer rows than they say, and skipping by them would move the page.
     * Coordinates are described when they are the very bounds the reader was given; whether those
     * take in whole chunks is for the counts of each segment to decide.
     * </p>
     */
    private static boolean countsDescribe(ColdPagePredicate predicate, LookupContext context, int worldId, Integer[] bounds) {
        if (predicate == null || context == null || context.predicateUnmodelled) {
            return false;
        }
        for (ColdPagePredicate.Condition condition : predicate.conditions()) {
            if (condition.column == null || condition.operator == ColdPagePredicate.HAS_TEXT) {
                return false; // no count recorded at seal time says which rows hold text or which alternative
            }
            boolean listed = condition.operator == ColdPagePredicate.IN || condition.operator == ColdPagePredicate.EQUAL;
            boolean unlisted = condition.operator == ColdPagePredicate.NOT_IN;
            long[] values = condition.values != null ? condition.values : new long[] { condition.value };
            boolean described;
            switch (condition.column) {
                case "user":
                    described = (listed && sameValues(values, context.users)) || (unlisted && sameValues(values, context.excludedUsers));
                    break;
                case "type":
                    described = (listed && sameValues(values, context.types)) || (unlisted && sameValues(values, context.excludedTypes));
                    break;
                case "action":
                    described = (listed && sameValues(values, context.actions)) || (unlisted && sameValues(values, context.excludedActions));
                    break;
                case "time":
                    described = (condition.operator == ColdPagePredicate.GREATER && context.startTime > 0 && context.startTime >= condition.value)
                            || (condition.operator == ColdPagePredicate.LESS_OR_EQUAL && context.endTime > 0 && context.endTime <= condition.value);
                    break;
                case "wid":
                    described = condition.operator == ColdPagePredicate.EQUAL && worldId > 0 && worldId == condition.value;
                    break;
                case "x":
                    described = isBound(condition, bounds, 1, 2);
                    break;
                case "z":
                    described = isBound(condition, bounds, 5, 6);
                    break;
                default:
                    described = false;
                    break;
            }
            if (!described) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if a condition on a coordinate is exactly the lower or upper bound the reader was
     *         given for it
     */
    private static boolean isBound(ColdPagePredicate.Condition condition, Integer[] bounds, int lower, int upper) {
        if (bounds == null || bounds.length < 7) {
            return false;
        }
        if (condition.operator == ColdPagePredicate.GREATER_OR_EQUAL) {
            return bounds[lower] != null && condition.value == bounds[lower];
        }
        if (condition.operator == ColdPagePredicate.LESS_OR_EQUAL) {
            return bounds[upper] != null && condition.value == bounds[upper];
        }
        return false;
    }

    private static boolean sameValues(long[] first, long[] second) {
        if (first == null || second == null) {
            return false;
        }
        long[] left = sorted(first);
        long[] right = sorted(second);
        return Arrays.equals(left, right);
    }

    /**
     * @return a filter accepting only what both accept, or whichever one exists
     */
    static ColdSegmentCodec.RowFilter both(ColdSegmentCodec.RowFilter first, ColdSegmentCodec.RowFilter second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return new ColdSegmentCodec.RowFilter() {
            @Override
            public boolean accept(long[] columns, boolean[] present) {
                return first.accept(columns, present) && second.accept(columns, present);
            }

            @Override
            public boolean acceptRow(long rowId, long[] columns, boolean[] present) {
                return first.acceptRow(rowId, columns, present) && second.acceptRow(rowId, columns, present);
            }

            @Override
            public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                return first.mayMatch(minimum, maximum, known) && second.mayMatch(minimum, maximum, known);
            }

            @Override
            public boolean mayMatchChunks(ColdSegmentCodec.ChunkList chunks) {
                return first.mayMatchChunks(chunks) && second.mayMatchChunks(chunks);
            }
        };
    }

    /**
     * Drops the temporary table a lookup materialized, if there was one.
     *
     * @param connection
     *            the connection the lookup ran on
     * @param table
     *            an unprefixed table name
     */
    public static void releaseMaterialized(Connection connection, String table) {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS temp.cp_cold_" + table);
        }
        catch (SQLException exception) {
            // A temporary table that cannot be dropped disappears when the connection is returned.
        }
    }

    /**
     * Opens the savepoint a read builds its scratch table under.
     *
     * @param connection
     *            the connection the lookup runs on
     * @throws SQLException
     *             if the savepoint cannot be opened
     */
    private static void beginScratch(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("SAVEPOINT " + SCRATCH_SAVEPOINT);
        }
    }

    /**
     * Closes the savepoint a read built its scratch table under: kept when the read finished,
     * taken back when it did not. Either way the caller is left with whatever transaction it had
     * before, untouched.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param succeeded
     *            whether the scratch table was built
     * @throws SQLException
     *             if a finished read cannot release its savepoint
     */
    private static void endScratch(Connection connection, boolean succeeded) throws SQLException {
        if (succeeded) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("RELEASE SAVEPOINT " + SCRATCH_SAVEPOINT);
            }
            return;
        }

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("ROLLBACK TO SAVEPOINT " + SCRATCH_SAVEPOINT);
            statement.executeUpdate("RELEASE SAVEPOINT " + SCRATCH_SAVEPOINT);
        }
        catch (SQLException exception) {
            // A full disk or a failed write takes the whole transaction back on its own, and there
            // is no savepoint left to unwind afterwards. Neither statement is allowed to stand in
            // front of the failure that brought us here.
        }
    }

    /**
     * Builds the column list of the temporary table. Row ids are written into SQLite's own hidden
     * row id column rather than a declared one, so that selecting from the temporary table returns
     * exactly the same columns as selecting from the live table.
     *
     * @param layout
     *            the layout of the live table
     * @return the column definitions
     */
    /**
     * Returns how many rows of a segment a lookup would match, but only when that can be known
     * without reading the segment.
     *
     * <p>
     * The counts recorded at seal time are exact, but each describes the whole segment along one
     * column: rows per player, per block type, per action, per chunk. So each restriction of the
     * lookup is first answered on its own. A time window that covers the whole segment keeps every
     * row, and one that cuts across it cannot be answered. A coordinate restriction is answered by
     * the rows the segment holds per chunk, but only when it takes in whole chunks and leaves the
     * height open. A world is answered by the segment holding that world alone, or by its chunk
     * counts when every row has a place.
     * </p>
     *
     * <p>
     * How two restrictions overlap is not recorded, so the answers are only combined where the
     * overlap does not matter. A restriction that keeps none of the segment's rows leaves nothing
     * for the others to keep, whatever they are. A restriction that keeps every row changes nothing,
     * and drops out. What is left is answered when at most one restriction remains and it could be
     * answered on its own.
     * </p>
     *
     * @return the exact number of matching rows, or -1 when it cannot be known from metadata alone
     */
    private static long exactRows(ColdSegment segment, int worldId, Integer[] bounds, int[] area, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded) {
        Exclusions exclusions = excluded == null ? Exclusions.NONE : excluded;
        long total = segment.rowCount;
        long[] kept = new long[5];
        int restrictions = 0;

        if ((startTime > 0 && segment.maxTime <= startTime) || (endTime > 0 && segment.minTime > endTime)) {
            return 0; // the window lies wholly before or after this segment
        }
        if ((startTime > 0 && segment.minTime <= startTime) || (endTime > 0 && segment.maxTime > endTime)) {
            kept[restrictions++] = -1; // the window starts or ends inside this segment
        }
        if (bounds != null) {
            kept[restrictions++] = placedRows(segment, worldId, bounds, area);
        }
        else if (worldId > 0) {
            kept[restrictions++] = worldRows(segment, worldId);
        }
        if (users != null || exclusions.users != null) {
            kept[restrictions++] = countFromStatistics(segment, segment.userStats, users, exclusions.users);
        }
        if (types != null || exclusions.types != null) {
            kept[restrictions++] = countFromStatistics(segment, segment.typeStats, types, exclusions.types);
        }
        if (actions != null || exclusions.actions != null) {
            kept[restrictions++] = countFromStatistics(segment, segment.actionStats, actions, exclusions.actions);
        }

        for (int index = 0; index < restrictions; index++) {
            if (kept[index] == 0) {
                return 0;
            }
        }
        long answer = total;
        int binding = 0;
        for (int index = 0; index < restrictions; index++) {
            if (kept[index] < 0) {
                return -1;
            }
            if (kept[index] != total) {
                binding++;
                answer = kept[index];
            }
        }
        return binding > 1 ? -1 : answer;
    }

    /**
     * @return how many rows of a segment lie within a lookup's coordinate bounds, or -1 when the
     *         chunk counts cannot say exactly
     */
    private static long placedRows(ColdSegment segment, int worldId, Integer[] bounds, int[] area) {
        if (area == null || segment.chunkCounts == null) {
            return -1; // the lookup is not about one world, or the segment has no counts
        }
        long located = segment.chunkCounts.rowsIn(worldId, area[0], area[1], area[2], area[3]);
        if (located == 0) {
            // No row of the segment lies anywhere the lookup covers. Rows without a place are never
            // matched by a restriction on coordinates, so none of them count either.
            return 0;
        }
        if (!coversWholeChunks(bounds)) {
            return -1; // the lookup cuts across chunks, so only reading tells which rows it keeps
        }
        return located;
    }

    /**
     * @return how many rows of a segment lie in one world, or -1 when that is not recorded
     */
    private static long worldRows(ColdSegment segment, int worldId) {
        if (segment.worldIds.length == 1 && segment.worldIds[0] == worldId) {
            return segment.rowCount;
        }
        // The chunk counts cover every row that has a world and both coordinates. A row with a world
        // but no coordinates would be missed, so they only answer when there is no such row.
        if (segment.chunkCounts != null && segment.chunkCounts.unlocatedRows() == 0) {
            return segment.chunkCounts.rowsIn(worldId, Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return -1;
    }

    /**
     * Counts the rows of one segment that a restriction on a single column leaves, using only the
     * counts recorded when the segment was sealed.
     *
     * @param segment
     *            the segment being counted
     * @param statistics
     *            the recorded counts for that column, or null when none were kept
     * @param wanted
     *            the values the lookup asked for, or null for all of them
     * @param unwanted
     *            the values the lookup asked to leave out, or null for none
     * @return the number of rows, or -1 when the counts cannot answer it
     */
    private static long countFromStatistics(ColdSegment segment, SegmentStatistics statistics, long[] wanted, long[] unwanted) {
        if (statistics == null) {
            return -1;
        }
        if (wanted == null) {
            // Statistics cover every row of the segment, so what is left is what is not excluded.
            return segment.rowCount - statistics.count(unwanted);
        }
        if (unwanted == null) {
            return statistics.count(wanted);
        }

        long counted = 0;
        long[] sortedUnwanted = sorted(unwanted);
        for (long value : wanted) {
            if (Arrays.binarySearch(sortedUnwanted, value) < 0) {
                counted = counted + statistics.count(value);
            }
        }
        return counted;
    }

    /**
     * Builds the test that decides which rows of a segment are worth building. The full lookup
     * predicate still runs in SQL afterwards, so this only has to avoid obvious waste; it never has
     * to be exact.
     *
     * @param layout
     *            the column layout of the table
     * @param worldId
     *            the world the lookup is restricted to, or 0 for any world
     * @param bounds
     *            the coordinate bounds of the lookup, or null
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0 for no lower bound
     * @param endTime
     *            the latest timestamp the lookup wants, or 0 for no upper bound
     * @param users
     *            the user ids the lookup wants, or null
     * @param types
     *            the block type ids the lookup wants, or null
     * @return the filter, or null when the lookup wants everything
     */
    static ColdSegmentCodec.RowFilter rowFilter(TableLayout layout, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded) {
        return rowFilter(layout, worldId, bounds, startTime, endTime, users, types, actions, excluded, null);
    }

    private static ColdSegmentCodec.RowFilter rowFilter(TableLayout layout, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded, long[] spawns) {
        boolean checkBounds = bounds != null && bounds.length >= 7 && layout.xColumn >= 0 && layout.zColumn >= 0;
        long[] wantedUsers = layout.userColumn >= 0 ? sorted(users) : null;
        long[] wantedTypes = layout.typeColumn >= 0 ? sorted(types) : null;
        long[] wantedActions = layout.actionColumn >= 0 ? sorted(actions) : null;
        long[] wantedSpawns = layout.spawnColumn >= 0 ? sorted(spawns) : null;
        Exclusions exclusions = excluded == null ? Exclusions.NONE : excluded;
        long[] unwantedUsers = layout.userColumn >= 0 ? sorted(exclusions.users) : null;
        long[] unwantedTypes = layout.typeColumn >= 0 ? sorted(exclusions.types) : null;
        long[] unwantedActions = layout.actionColumn >= 0 ? sorted(exclusions.actions) : null;
        boolean checkWorld = worldId > 0 && layout.worldColumn >= 0;
        boolean checkTime = layout.timeColumn >= 0 && (startTime > 0 || endTime > 0);
        int minChunkX = checkBounds ? SegmentFilter.chunkOf(bounds[1]) : 0;
        int maxChunkX = checkBounds ? SegmentFilter.chunkOf(bounds[2]) : 0;
        int minChunkZ = checkBounds ? SegmentFilter.chunkOf(bounds[5]) : 0;
        int maxChunkZ = checkBounds ? SegmentFilter.chunkOf(bounds[6]) : 0;

        if (!checkBounds && !checkWorld && !checkTime && wantedUsers == null && wantedTypes == null && wantedActions == null
                && wantedSpawns == null && unwantedUsers == null && unwantedTypes == null && unwantedActions == null) {
            return null;
        }

        return new ColdSegmentCodec.RowFilter() {
            @Override
            public boolean accept(long[] columns, boolean[] present) {
                if (checkTime && present[layout.timeColumn]) {
                    long time = columns[layout.timeColumn];
                    if ((startTime > 0 && time <= startTime) || (endTime > 0 && time > endTime)) {
                        return false;
                    }
                }
                if (checkWorld && present[layout.worldColumn] && columns[layout.worldColumn] != worldId) {
                    return false;
                }
                if (wantedUsers != null && present[layout.userColumn] && Arrays.binarySearch(wantedUsers, columns[layout.userColumn]) < 0) {
                    return false;
                }
                if (wantedTypes != null && present[layout.typeColumn] && Arrays.binarySearch(wantedTypes, columns[layout.typeColumn]) < 0) {
                    return false;
                }
                if (wantedActions != null && present[layout.actionColumn] && Arrays.binarySearch(wantedActions, columns[layout.actionColumn]) < 0) {
                    return false;
                }
                if (wantedSpawns != null && present[layout.spawnColumn] && Arrays.binarySearch(wantedSpawns, columns[layout.spawnColumn]) < 0) {
                    return false;
                }
                if (unwantedUsers != null && present[layout.userColumn] && Arrays.binarySearch(unwantedUsers, columns[layout.userColumn]) >= 0) {
                    return false;
                }
                if (unwantedTypes != null && present[layout.typeColumn] && Arrays.binarySearch(unwantedTypes, columns[layout.typeColumn]) >= 0) {
                    return false;
                }
                if (unwantedActions != null && present[layout.actionColumn] && Arrays.binarySearch(unwantedActions, columns[layout.actionColumn]) >= 0) {
                    return false;
                }
                if (checkBounds && present[layout.xColumn] && present[layout.zColumn]) {
                    long x = columns[layout.xColumn];
                    long z = columns[layout.zColumn];
                    if (x < bounds[1] || x > bounds[2] || z < bounds[5] || z > bounds[6]) {
                        return false;
                    }
                }
                return true;
            }

            /**
             * The same tests as {@link #accept}, asked of the range a whole group of rows spans. A
             * range is only known where every row of the group has a value, so a test that rejects
             * the range rejects every row in it.
             */
            @Override
            public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                if (checkTime && known[layout.timeColumn]) {
                    if ((startTime > 0 && maximum[layout.timeColumn] <= startTime) || (endTime > 0 && minimum[layout.timeColumn] > endTime)) {
                        return false;
                    }
                }
                if (checkWorld && known[layout.worldColumn] && (worldId < minimum[layout.worldColumn] || worldId > maximum[layout.worldColumn])) {
                    return false;
                }
                // A row is only tested against the bounds when it has both coordinates, so a group
                // can only be ruled out when every row in it has both.
                if (checkBounds && known[layout.xColumn] && known[layout.zColumn]
                        && (maximum[layout.xColumn] < bounds[1] || minimum[layout.xColumn] > bounds[2] || maximum[layout.zColumn] < bounds[5] || minimum[layout.zColumn] > bounds[6])) {
                    return false;
                }
                if (wantedUsers != null && known[layout.userColumn] && !anyWithin(wantedUsers, minimum[layout.userColumn], maximum[layout.userColumn])) {
                    return false;
                }
                if (wantedTypes != null && known[layout.typeColumn] && !anyWithin(wantedTypes, minimum[layout.typeColumn], maximum[layout.typeColumn])) {
                    return false;
                }
                if (wantedActions != null && known[layout.actionColumn] && !anyWithin(wantedActions, minimum[layout.actionColumn], maximum[layout.actionColumn])) {
                    return false;
                }
                if (wantedSpawns != null && known[layout.spawnColumn] && !anyWithin(wantedSpawns, minimum[layout.spawnColumn], maximum[layout.spawnColumn])) {
                    return false;
                }
                return true;
            }

            /**
             * The world and bounds tests of {@link #accept}, asked of the exact chunks a group of
             * rows lies in. A row outside every chunk the bounds touch is outside the bounds, and a
             * row of another world is of another world, so a group none of whose chunks passes
             * holds nothing this lookup keeps. A row without a place passes both tests in
             * {@link #accept}, so a group holding one is always read.
             */
            @Override
            public boolean mayMatchChunks(ColdSegmentCodec.ChunkList chunks) {
                if ((!checkWorld && !checkBounds) || chunks.unlocatedRows() > 0) {
                    return true;
                }
                for (int index = 0; index < chunks.size(); index++) {
                    if (checkWorld && chunks.worldId(index) != worldId) {
                        continue;
                    }
                    if (checkBounds && (chunks.chunkX(index) < minChunkX || chunks.chunkX(index) > maxChunkX || chunks.chunkZ(index) < minChunkZ || chunks.chunkZ(index) > maxChunkZ)) {
                        continue;
                    }
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * @param sorted
     *            values in ascending order
     * @param minimum
     *            the low end of a range
     * @param maximum
     *            the high end of a range
     * @return true if any of the values lies within the range
     */
    static boolean anyWithin(long[] sorted, long minimum, long maximum) {
        int index = Arrays.binarySearch(sorted, minimum);
        if (index >= 0) {
            return true;
        }
        int insertion = -index - 1;
        return insertion < sorted.length && sorted[insertion] <= maximum;
    }

    /**
     * Counts the compressed rows a lookup would match, without building any of them.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param table
     *            an unprefixed table name
     * @param selected
     *            the segments to examine
     * @param worldId
     *            the world the lookup is restricted to, or 0 for any world
     * @param bounds
     *            the coordinate bounds of the lookup, or null
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0 for no lower bound
     * @param endTime
     *            the latest timestamp the lookup wants, or 0 for no upper bound
     * @param users
     *            the user ids the lookup wants, or null
     * @param types
     *            the block type ids the lookup wants, or null
     * @param described
     *            whether the counts recorded with each segment describe the whole query, so a
     *            segment they answer need not be read
     * @param clauseFilter
     *            the query's own clause as the decoder tests it, or null when the restrictions above
     *            are all of it
     * @return the number of matching rows
     * @throws SQLException
     *             if a segment cannot be read
     */
    private static long countRows(Connection connection, String table, List<ColdSegment> selected, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded, boolean described, ColdSegmentCodec.RowFilter clauseFilter) throws SQLException {
        TableLayout layout = layout(connection, table);
        if (layout == null) {
            return 0;
        }

        ColdSegmentCodec.RowFilter filter = both(rowFilter(layout, worldId, bounds, startTime, endTime, users, types, actions, excluded), clauseFilter);
        int[] area = chunkArea(worldId, bounds);
        long counted = 0;
        List<ColdSegment> mustRead = new ArrayList<>(selected.size());

        for (ColdSegment segment : selected) {
            // Each segment recorded how many rows it holds for each player and block type when it
            // was sealed. Where the query covers the segment whole, adding those numbers up gives
            // the same answer as reading every row, so the rows are never touched.
            long exact = described ? exactRows(segment, worldId, bounds, area, startTime, endTime, users, types, actions, excluded) : -1;
            if (exact >= 0) {
                counted = counted + exact;
            }
            else {
                mustRead.add(segment);
            }
        }

        for (int wave = 0; wave < mustRead.size(); wave += DECODE_WAVE) {
            List<ColdSegment> batch = mustRead.subList(wave, Math.min(mustRead.size(), wave + DECODE_WAVE));
            counted = counted + countBatch(connection, batch, filter);
        }

        if (!selected.isEmpty()) {
            debug("count " + table + ": " + (selected.size() - mustRead.size()) + " of " + selected.size()
                    + " segments answered from recorded counts");
        }
        return counted;
    }

    /**
     * Counts the matching rows of a handful of segments in parallel, building none of them.
     *
     * <p>
     * A count never looks at a text or blob value, so only the scalar frames and the units the
     * filter can match are read, and the payload frames never are.
     * </p>
     *
     * @param connection
     *            the connection the lookup runs on
     * @param batch
     *            the segments to count
     * @param filter
     *            decides which rows count
     * @return the number of matching rows
     * @throws SQLException
     *             if a segment cannot be read
     */
    private static long countBatch(Connection connection, List<ColdSegment> batch, ColdSegmentCodec.RowFilter filter) throws SQLException {
        List<java.util.concurrent.Callable<Integer>> counting = new ArrayList<>(batch.size());
        for (SegmentFrames read : readBatch(connection, batch, filter)) {
            counting.add(() -> {
                ColdSegmentReadCounter.recordRead();
                return ColdSegmentCodec.count(read.scalars(), read.units, filter, read.columns);
            });
        }
        long counted = 0;
        for (int rows : runAll(counting)) {
            counted = counted + rows;
        }
        return counted;
    }

    /**
     * Reads and decodes a handful of segments, decoding them in parallel but returning them in the
     * order they were given, so the rows still enter the temporary table oldest first.
     *
     * <p>
     * Decoding happens in two steps, because the payload frame, which holds every text and blob
     * value and is most of what a segment stores, is only worth reading for a segment that keeps a
     * row with such a value. The first step decodes the kept rows from the scalar frames alone. The
     * payloads those rows need are then read here, on the thread that holds the connection, in one
     * statement, and the second step fills the values in. A segment none of whose kept rows has a
     * text or blob value, or that keeps no row at all, never has its payload read.
     * </p>
     *
     * @param connection
     *            the connection the lookup runs on
     * @param batch
     *            the segments to decode
     * @param filter
     *            decides which rows to build
     * @return the decoded rows, one entry per segment
     * @throws SQLException
     *             if a segment cannot be read
     */
    private static List<ColdSegmentCodec.Rows> decodeBatch(Connection connection, List<ColdSegment> batch, ColdSegmentCodec.RowFilter filter) throws SQLException {
        List<SegmentFrames> frames = readBatch(connection, batch, filter);
        List<java.util.concurrent.Callable<ColdSegmentCodec.Selection>> selecting = new ArrayList<>(frames.size());
        for (SegmentFrames read : frames) {
            selecting.add(() -> {
                ColdSegmentReadCounter.recordRead();
                return ColdSegmentCodec.select(read.scalars(), read.units, filter, read.columns);
            });
        }
        List<ColdSegmentCodec.Selection> selections = runAll(selecting);

        List<SegmentFrames> needing = new ArrayList<>();
        int[][] shares = new int[frames.size()][];
        for (int index = 0; index < frames.size(); index++) {
            SegmentFrames read = frames.get(index);
            if (!selections.get(index).needsPayload()) {
                continue;
            }
            if (read.entry.payloadInUnits()) {
                // Only the shares of the units holding a kept value are read, which for a lookup
                // about one place is one or two of a compacted segment's hundreds.
                shares[index] = selections.get(index).payloadUnits(read.entry.payloadStarts());
                readPayloadShares(connection, read, shares[index]);
            }
            else {
                needing.add(read);
            }
        }
        boolean anyShares = false;
        for (int[] wanted : shares) {
            anyShares = anyShares || wanted != null;
        }
        if (needing.isEmpty() && !anyShares) {
            List<ColdSegmentCodec.Rows> decoded = new ArrayList<>(selections.size());
            for (ColdSegmentCodec.Selection selection : selections) {
                decoded.add(selection.build(null));
            }
            return decoded;
        }

        if (!needing.isEmpty()) {
            readPayloads(connection, needing);
        }
        List<java.util.concurrent.Callable<ColdSegmentCodec.Rows>> building = new ArrayList<>(frames.size());
        for (int index = 0; index < frames.size(); index++) {
            SegmentFrames read = frames.get(index);
            ColdSegmentCodec.Selection selection = selections.get(index);
            int[] wanted = shares[index];
            if (wanted != null) {
                building.add(() -> selection.buildFromShares(read.entry.shares(wanted)));
            }
            else {
                building.add(() -> selection.build(selection.needsPayload() ? read.entry.payload() : null));
            }
        }
        return runAll(building);
    }

    /**
     * Reads the payload shares of some units of a segment whose units keep their own, unless they
     * are already held in memory, and loads the dictionary they were compressed with, on the thread
     * that holds the connection.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param read
     *            the segment's frames
     * @param wanted
     *            the units whose shares are needed
     * @throws SQLException
     *             if a share cannot be read or is missing
     */
    private static void readPayloadShares(Connection connection, SegmentFrames read, int[] wanted) throws SQLException {
        List<Integer> missing = new ArrayList<>();
        for (int unit : wanted) {
            if (read.entry.storedShare(unit) == null) {
                missing.add(unit);
            }
        }
        if (!missing.isEmpty()) {
            StringBuilder query = new StringBuilder("SELECT unit,payload FROM ").append(ConfigHandler.prefix).append("segment_unit WHERE segment_id = ? AND unit IN (");
            for (int index = 0; index < missing.size(); index++) {
                query.append(index == 0 ? "?" : ",?");
            }
            query.append(')');
            int found = 0;
            try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
                statement.setLong(1, read.segment.id);
                for (int index = 0; index < missing.size(); index++) {
                    statement.setInt(index + 2, missing.get(index));
                }
                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        byte[] stored = results.getBytes(2);
                        if (stored == null) {
                            throw new SQLException("Cold segment " + read.segment.id + " unit " + results.getInt(1) + " holds no payload");
                        }
                        ColdSegmentReadCounter.recordPayloadFetch(stored.length);
                        read.entry.holdShare(results.getInt(1), stored);
                        found++;
                    }
                }
            }
            if (found != missing.size()) {
                throw new SQLException("Cold segment " + read.segment.id + " is missing the payload of a unit");
            }
        }
        if (read.entry.sharesNeedDictionary(wanted)) {
            SegmentDictionary.preload(read.entry.dictionaryId(), connection);
        }
    }

    /**
     * Reads the scalar frames of a handful of segments, and the units of each that the filter can
     * match, on the thread that holds the connection.
     */
    private static List<SegmentFrames> readBatch(Connection connection, List<ColdSegment> batch, ColdSegmentCodec.RowFilter filter) throws SQLException {
        List<SegmentFrames> frames = new ArrayList<>(batch.size());
        for (ColdSegment segment : batch) {
            SegmentFrames read = readFrames(connection, segment);
            readUnits(connection, segment, read, filter);
            frames.add(read);
        }
        return frames;
    }

    /**
     * Runs a handful of decoding tasks on the decode pool, or here when there is only one, and
     * returns their results in the order the tasks were given.
     *
     * @throws SQLException
     *             if a task fails
     */
    private static <T> List<T> runAll(List<java.util.concurrent.Callable<T>> tasks) throws SQLException {
        List<T> results = new ArrayList<>(tasks.size());
        if (tasks.size() <= 1 || DECODE_THREADS == 1) {
            for (java.util.concurrent.Callable<T> task : tasks) {
                try {
                    results.add(task.call());
                }
                catch (SQLException | RuntimeException exception) {
                    throw exception;
                }
                catch (Exception exception) {
                    throw new SQLException("Unable to read compressed storage", exception);
                }
            }
            return results;
        }

        List<java.util.concurrent.Future<T>> pending = new ArrayList<>(tasks.size());
        for (java.util.concurrent.Callable<T> task : tasks) {
            pending.add(decodePool().submit(task));
        }
        for (java.util.concurrent.Future<T> future : pending) {
            try {
                results.add(future.get());
            }
            catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while reading compressed storage", exception);
            }
            catch (java.util.concurrent.ExecutionException exception) {
                throw new SQLException("Unable to read compressed storage", exception.getCause());
            }
        }
        return results;
    }

    private static long[] sorted(long[] values) {
        if (values == null || values.length == 0) {
            return null;
        }
        long[] copy = Arrays.copyOf(values, values.length);
        Arrays.sort(copy);
        return copy;
    }

    private static String hotTable(String table) {
        return ConfigHandler.prefix + table;
    }

    private static String columnDefinition(TableLayout layout) {
        StringBuilder definition = new StringBuilder();
        for (int index = 0; index < layout.columns.length; index++) {
            if (index > 0) {
                definition.append(',');
            }
            definition.append(layout.columns[index]).append(' ').append(layout.declaredTypes[index]);
        }
        return definition.toString();
    }

    private static Map<Long, Integer> readOverlay(Connection connection, String table, List<ColdSegment> selected) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        Map<Long, Integer> overlay = new HashMap<>();
        if (tableId == null || selected.isEmpty()) {
            return overlay;
        }

        // One query spanning every selected segment. Rows for segments in between that were not
        // selected are read too, harmlessly: the overlay is only consulted for rows that are built.
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        for (ColdSegment segment : selected) {
            lowest = Math.min(lowest, segment.startRowId);
            highest = Math.max(highest, segment.endRowId);
        }
        String query = "SELECT rowid_ref,rolled_back FROM " + ConfigHandler.prefix + "cold_flag WHERE table_id = ? AND rowid_ref BETWEEN ? AND ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, tableId);
            statement.setLong(2, lowest);
            statement.setLong(3, highest);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    overlay.put(results.getLong("rowid_ref"), results.getInt("rolled_back"));
                }
            }
        }
        return overlay;
    }

    /**
     * Reads and decodes the contents of one segment.
     *
     * @param connection
     *            an open connection
     * @param segment
     *            the segment to read
     * @return the decoded rows
     * @throws SQLException
     *             if the segment cannot be read
     */
    /** Decoding is CPU work, so segments are decoded on a small pool while inserts stay in order. */
    private static final int DECODE_THREADS = Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors() - 2));

    /** Segments decoded at once. Bounds how much decoded data is held before it is inserted. */
    private static final int DECODE_WAVE = 8;

    /** The savepoint a read builds its scratch table under. */
    private static final String SCRATCH_SAVEPOINT = "cp_cold_read";

    /**
     * Above this many rows a read decodes one segment at a time. Decoding several at once is
     * quicker, but each one holds its rows in memory until they are written, and a lookup reaching
     * far back through history must not be able to exhaust the server's memory to go faster.
     */
    private static final long LARGE_READ_ROWS = 100_000;


    private static volatile java.util.concurrent.ExecutorService decodePool;

    private static java.util.concurrent.ExecutorService decodePool() {
        java.util.concurrent.ExecutorService pool = decodePool;
        if (pool != null) {
            return pool;
        }
        synchronized (SQLiteColdIndex.class) {
            if (decodePool == null) {
                decodePool = java.util.concurrent.Executors.newFixedThreadPool(DECODE_THREADS, runnable -> {
                    Thread thread = new Thread(runnable, "CoreProtect-ColdDecode");
                    thread.setDaemon(true);
                    return thread;
                });
            }
            return decodePool;
        }
    }

    /** The frames of one segment, cached or just read, decompressed on first use. */
    private static final class SegmentFrames {
        private final ColdSegment segment;
        private final SegmentFrameCache.Entry entry;
        /** The columns the segment's rows are read as. */
        private final ColdSegmentCodec.Columns columns;
        /**
         * The unit bodies this read needs, by unit number, when the segment stores them apart from
         * its directory; null otherwise.
         */
        private byte[][] units;

        private SegmentFrames(ColdSegment segment, SegmentFrameCache.Entry entry, ColdSegmentCodec.Columns columns) {
            this.segment = segment;
            this.entry = entry;
            this.columns = columns;
        }

        private byte[] scalars() throws SQLException {
            return entry.scalars();
        }
    }

    /**
     * Reads a segment's scalar frame from the database, unless it is already held in memory. The
     * payload frame is left unread: most reads keep no row that needs it, and
     * {@link #readPayloads} reads it for those that do.
     *
     * @param connection
     *            the connection the lookup runs on
     * @param segment
     *            the segment to read
     * @return the frames
     * @throws SQLException
     *             if the segment cannot be read
     */
    private static SegmentFrames readFrames(Connection connection, ColdSegment segment) throws SQLException {
        SegmentFrameCache.Entry entry = SegmentFrameCache.get(segment);
        if (entry == null) {
            // Whether the payload is absent is read from the row's header, without reading the
            // payload itself.
            String query = "SELECT scalars,scalars_size,payload_size,dict_id,payload IS NULL AS payload_absent FROM " + ConfigHandler.prefix + "segment WHERE id = ? LIMIT 1";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setLong(1, segment.id);
                try (ResultSet results = statement.executeQuery()) {
                    if (!results.next()) {
                        throw new SQLException("Cold segment " + segment.id + " is missing");
                    }
                    ColdSegmentReadCounter.recordFrameRead();
                    byte[] scalars = results.getBytes("scalars");
                    ColdSegmentReadCounter.recordBytesRead(scalars.length);
                    entry = SegmentFrameCache.put(segment, new SegmentFrameCache.Entry(scalars, results.getInt("scalars_size"), results.getInt("payload_size"), results.getInt("dict_id"),
                            results.getBoolean("payload_absent")));
                }
            }
        }
        return new SegmentFrames(segment, entry, decodeColumns(connection, segment));
    }

    /**
     * Reads the payload frames of the segments whose kept rows need one, in one statement, unless
     * they are already held in memory, and loads the dictionaries they were compressed with. Both
     * have to happen here, on the thread that holds the connection, before the rows are finished
     * on the decode pool.
     *
     * <p>
     * The payload is read in a statement of its own, after the scalar frame, so a segment rewritten
     * in between would pair a directory with a payload it does not describe. The sizes and the
     * dictionary recorded beside the payload are compared with those read beside the scalar frame,
     * and a segment that no longer matches is refused rather than read wrongly.
     * </p>
     *
     * @param connection
     *            the connection the lookup runs on
     * @param needing
     *            the segments whose payload is needed, each at most once
     * @throws SQLException
     *             if a payload cannot be read, or its segment changed or is gone
     */
    private static void readPayloads(Connection connection, List<SegmentFrames> needing) throws SQLException {
        Map<Long, SegmentFrames> missing = new HashMap<>();
        for (SegmentFrames read : needing) {
            if (!read.entry.hasPayload()) {
                missing.put(read.segment.id, read);
            }
        }
        if (!missing.isEmpty()) {
            StringBuilder query = new StringBuilder("SELECT id,start_rowid,scalars_size,payload,payload_size,dict_id FROM ").append(ConfigHandler.prefix).append("segment WHERE id IN (");
            for (int index = 0; index < missing.size(); index++) {
                query.append(index == 0 ? "?" : ",?");
            }
            query.append(')');
            try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
                int parameter = 1;
                for (long id : missing.keySet()) {
                    statement.setLong(parameter++, id);
                }
                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        SegmentFrames read = missing.remove(results.getLong("id"));
                        if (read == null) {
                            continue;
                        }
                        SegmentFrameCache.Entry entry = read.entry;
                        if (results.getLong("start_rowid") != read.segment.startRowId || results.getInt("scalars_size") != entry.scalarSize()
                                || results.getInt("payload_size") != entry.payloadSize() || results.getInt("dict_id") != entry.dictionaryId()) {
                            SegmentFrameCache.forget(read.segment);
                            throw new SQLException("Cold segment " + read.segment.id + " was rewritten while it was being read");
                        }
                        byte[] payload = results.getBytes("payload");
                        ColdSegmentReadCounter.recordPayloadFetch(payload == null ? 0 : payload.length);
                        entry.holdPayload(payload);
                    }
                }
            }
            if (!missing.isEmpty()) {
                throw new SQLException("Cold segment " + missing.keySet().iterator().next() + " is missing");
            }
        }
        for (SegmentFrames read : needing) {
            if (read.entry.needsDictionary()) {
                SegmentDictionary.preload(read.entry.dictionaryId(), connection);
            }
        }
    }

    /**
     * Gathers the unit bodies a read of a segment needs, when the segment stores them apart from its
     * directory, taking what is already in memory and reading the rest in one statement.
     *
     * <p>
     * Which units are needed is decided here, on the thread that holds the connection, from the
     * directory and the same filter the decode will use, so the decode itself, which runs on a
     * thread without a connection, finds every unit it opens already read. A lookup about one
     * place reads the one or two units holding its groups rather than the whole segment.
     * </p>
     *
     * @param connection
     *            the connection the lookup runs on
     * @param segment
     *            the segment being read
     * @param frames
     *            its frames, which are given the unit bodies
     * @param filter
     *            the filter the decode will use, or null when every row is wanted
     * @throws SQLException
     *             if a unit cannot be read, or is missing
     */
    private static void readUnits(Connection connection, ColdSegment segment, SegmentFrames frames, ColdSegmentCodec.RowFilter filter) throws SQLException {
        SegmentFrameCache.Entry entry = frames.entry;
        if (!entry.isSplit()) {
            return;
        }
        byte[] directory = entry.scalars();
        int[] needed;
        try {
            needed = ColdSegmentCodec.neededUnits(directory, filter, frames.columns);
        }
        catch (IllegalArgumentException malformed) {
            throw new SQLException("Cold segment " + segment.id + " has an unreadable directory", malformed);
        }
        byte[][] units = new byte[ColdSegmentCodec.unitCount(directory)][];
        List<Integer> missing = new ArrayList<>();
        for (int unit : needed) {
            units[unit] = entry.unit(unit);
            if (units[unit] == null) {
                missing.add(unit);
            }
        }
        if (!missing.isEmpty()) {
            Map<Integer, byte[]> read = fetchUnits(connection, segment.id, missing);
            for (int unit : missing) {
                byte[] body = read.get(unit);
                if (body == null) {
                    throw new SQLException("Cold segment " + segment.id + " is missing unit " + unit);
                }
                units[unit] = body;
                entry.holdUnit(unit, body);
            }
        }
        frames.units = units;
    }

    /**
     * Reads unit bodies of one segment from the database.
     *
     * @param connection
     *            an open connection
     * @param segmentId
     *            the segment
     * @param wanted
     *            the unit numbers to read, or null for every unit the segment has
     * @return the bodies read, by unit number
     * @throws SQLException
     *             if they cannot be read
     */
    private static Map<Integer, byte[]> fetchUnits(Connection connection, long segmentId, List<Integer> wanted) throws SQLException {
        StringBuilder query = new StringBuilder("SELECT unit,body FROM ").append(ConfigHandler.prefix).append("segment_unit WHERE segment_id = ?");
        if (wanted != null) {
            query.append(" AND unit IN (");
            for (int index = 0; index < wanted.size(); index++) {
                query.append(index == 0 ? "?" : ",?");
            }
            query.append(')');
        }
        Map<Integer, byte[]> bodies = new HashMap<>();
        long bytes = 0;
        try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
            statement.setLong(1, segmentId);
            if (wanted != null) {
                for (int index = 0; index < wanted.size(); index++) {
                    statement.setInt(index + 2, wanted.get(index));
                }
            }
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    byte[] body = results.getBytes(2);
                    bodies.put(results.getInt(1), body);
                    bytes = bytes + body.length;
                }
            }
        }
        ColdSegmentReadCounter.recordUnitReads(bodies.size());
        ColdSegmentReadCounter.recordBytesRead(bytes);
        return bodies;
    }

    /**
     * Reads every unit body of a segment, for the maintenance work that decodes all of it.
     *
     * @param connection
     *            an open connection
     * @param segmentId
     *            the segment
     * @param directory
     *            its directory
     * @return the bodies by unit number
     * @throws SQLException
     *             if they cannot be read, or any is missing
     */
    private static byte[][] readAllUnits(Connection connection, long segmentId, byte[] directory) throws SQLException {
        byte[][] units = new byte[ColdSegmentCodec.unitCount(directory)][];
        for (Map.Entry<Integer, byte[]> read : fetchUnits(connection, segmentId, null).entrySet()) {
            int unit = read.getKey();
            if (unit < 0 || unit >= units.length) {
                throw new SQLException("Cold segment " + segmentId + " has a unit " + unit + " its directory does not describe");
            }
            units[unit] = read.getValue();
        }
        for (int unit = 0; unit < units.length; unit++) {
            if (units[unit] == null) {
                throw new SQLException("Cold segment " + segmentId + " is missing unit " + unit);
            }
        }
        return units;
    }

    static ColdSegmentCodec.Rows readRows(Connection connection, ColdSegment segment) throws SQLException {
        return readRows(connection, segment, null);
    }

    /**
     * Reads one segment, building only the rows a filter accepts.
     *
     * @param connection
     *            an open connection
     * @param segment
     *            the segment to read
     * @param filter
     *            decides which rows to build, or null to build all of them
     * @return the decoded rows
     * @throws SQLException
     *             if the segment cannot be read
     */
    static ColdSegmentCodec.Rows readRows(Connection connection, ColdSegment segment, ColdSegmentCodec.RowFilter filter) throws SQLException {
        String query = "SELECT scalars,scalars_size,payload,payload_size,dict_id FROM " + ConfigHandler.prefix + "segment WHERE id = ? LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, segment.id);
            try (ResultSet results = statement.executeQuery()) {
                if (!results.next()) {
                    throw new SQLException("Cold segment " + segment.id + " is missing");
                }

                byte[] scalars = SegmentDictionary.decompress(results.getBytes("scalars"), results.getInt("scalars_size"), 0, connection);
                byte[] payloadFrame = results.getBytes("payload");
                int payloadSize = results.getInt("payload_size");
                byte[] payload;
                if (payloadFrame == null && payloadSize > 0) {
                    // A compacted segment keeps each unit's share of the payload beside the unit,
                    // and the shares one after another are the payload frame.
                    payload = readWholePayloadFromShares(connection, segment.id, scalars, payloadSize, results.getInt("dict_id"));
                }
                else {
                    payload = payloadFrame == null ? new byte[0] : SegmentDictionary.decompress(payloadFrame, payloadSize, results.getInt("dict_id"), connection);
                }
                // A segment's units are written in the same transaction as its row and never change
                // afterwards, so a second statement finds exactly the ones this directory describes.
                byte[][] units = ColdSegmentCodec.isSplit(scalars) ? readAllUnits(connection, segment.id, scalars) : null;
                return ColdSegmentCodec.decode(scalars, units, payload, filter, decodeColumns(connection, segment));
            }
        }
    }

    /**
     * Joins the payload shares every unit of a compacted segment keeps back into the payload frame.
     *
     * @param connection
     *            an open connection
     * @param segmentId
     *            the segment
     * @param directory
     *            its directory
     * @param payloadSize
     *            the size of the whole payload frame
     * @param dictionaryId
     *            the dictionary the shares were compressed with
     * @return the payload frame
     * @throws SQLException
     *             if a share is missing or does not hold what the directory describes
     */
    static byte[] readWholePayloadFromShares(Connection connection, long segmentId, byte[] directory, int payloadSize, int dictionaryId) throws SQLException {
        int[] starts;
        try {
            starts = ColdSegmentCodec.unitPayloadStarts(directory);
        }
        catch (IllegalArgumentException malformed) {
            throw new SQLException("Cold segment " + segmentId + " has an unreadable directory", malformed);
        }
        byte[] payload = new byte[payloadSize];
        int found = 0;
        try (PreparedStatement statement = connection.prepareStatement("SELECT unit,payload FROM " + ConfigHandler.prefix + "segment_unit WHERE segment_id = ?")) {
            statement.setLong(1, segmentId);
            try (ResultSet results = statement.executeQuery()) {
                while (results.next()) {
                    int unit = results.getInt(1);
                    byte[] stored = results.getBytes(2);
                    if (unit < 0 || unit >= starts.length || stored == null) {
                        throw new SQLException("Cold segment " + segmentId + " unit " + unit + " has no payload its directory describes");
                    }
                    int end = unit + 1 < starts.length ? starts[unit + 1] : payloadSize;
                    byte[] share = SegmentDictionary.decompress(stored, end - starts[unit], dictionaryId, connection);
                    if (share.length != end - starts[unit]) {
                        throw new SQLException("Cold segment " + segmentId + " unit " + unit + " payload decompressed to the wrong length");
                    }
                    System.arraycopy(share, 0, payload, starts[unit], share.length);
                    found++;
                }
            }
        }
        if (found != starts.length) {
            throw new SQLException("Cold segment " + segmentId + " is missing the payload of a unit");
        }
        return payload;
    }

    /**
     * Walks every row of one segment through a filter, reading only its scalar frame. The payload
     * frame is never read, so a filter that builds no rows costs only the arithmetic of decoding.
     *
     * @param connection
     *            an open connection
     * @param segment
     *            the segment to walk
     * @param filter
     *            shown every row, as the plain numbers of its integer columns
     * @throws SQLException
     *             if the segment cannot be read
     */
    static void scanRows(Connection connection, ColdSegment segment, ColdSegmentCodec.RowFilter filter) throws SQLException {
        String query = "SELECT scalars,scalars_size FROM " + ConfigHandler.prefix + "segment WHERE id = ? LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, segment.id);
            try (ResultSet results = statement.executeQuery()) {
                if (!results.next()) {
                    throw new SQLException("Cold segment " + segment.id + " is missing");
                }
                byte[] scalars = SegmentDictionary.decompress(results.getBytes("scalars"), results.getInt("scalars_size"), 0, connection);
                byte[][] units = ColdSegmentCodec.isSplit(scalars) ? readAllUnits(connection, segment.id, scalars) : null;
                ColdSegmentCodec.count(scalars, units, filter, decodeColumns(connection, segment));
            }
        }
    }

    private static int[] readWorldIds(byte[] encoded) {
        if (encoded == null || encoded.length == 0) {
            return new int[0];
        }
        int[] worldIds = new int[encoded.length / 4];
        for (int index = 0; index < worldIds.length; index++) {
            int offset = index * 4;
            worldIds[index] = ((encoded[offset] & 0xFF) << 24) | ((encoded[offset + 1] & 0xFF) << 16)
                    | ((encoded[offset + 2] & 0xFF) << 8) | (encoded[offset + 3] & 0xFF);
        }
        return worldIds;
    }

    /**
     * @param worldIds
     *            the worlds a segment contains
     * @return the stored form of that set
     */
    static byte[] writeWorldIds(List<Integer> worldIds) {
        byte[] encoded = new byte[worldIds.size() * 4];
        for (int index = 0; index < worldIds.size(); index++) {
            int value = worldIds.get(index);
            int offset = index * 4;
            encoded[offset] = (byte) (value >>> 24);
            encoded[offset + 1] = (byte) (value >>> 16);
            encoded[offset + 2] = (byte) (value >>> 8);
            encoded[offset + 3] = (byte) value;
        }
        return encoded;
    }

    /**
     * @param data
     *            the payload of a segment
     * @return the payload compressed for storage, never null
     */
    static byte[] compressForStorage(byte[] data) {
        return BlobCompression.compressForStorage(data);
    }

    /**
     * @return the segment ids currently loaded, for tests and diagnostics
     */
    static List<Long> loadedSegmentIds() {
        List<Long> ids = new ArrayList<>();
        for (List<ColdSegment> tableSegments : segments.values()) {
            for (ColdSegment segment : tableSegments) {
                ids.add(segment.id);
            }
        }
        Collections.sort(ids);
        return ids;
    }

    /**
     * @param table
     *            an unprefixed table name
     * @return true if rows of this table are rolled into segments
     */
    public static boolean isSegmented(String table) {
        return SEGMENTED_TABLES.containsKey(table);
    }

    /**
     * @return the segment table ids, in table order
     */
    static Map<String, Integer> tableIds() {
        return SEGMENTED_TABLES;
    }

    /**
     * @param types
     *            column kinds
     * @return a copy of the kinds, so callers cannot mutate a cached layout
     */
    static int[] copyTypes(int[] types) {
        return Arrays.copyOf(types, types.length);
    }
}
