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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ConcurrentHashMap;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
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
        final SegmentFilter chunkFilter;
        final SegmentMembership userFilter;
        final SegmentMembership typeFilter;
        final SegmentMembership spawnFilter;
        final SegmentStatistics userStats;
        final SegmentStatistics typeStats;
        final SegmentStatistics actionStats;

        ColdSegment(long id, int tableId, long startRowId, long endRowId, int rowCount, long minTime, long maxTime, int[] worldIds, SegmentFilter chunkFilter, SegmentMembership userFilter, SegmentMembership typeFilter, SegmentMembership spawnFilter, SegmentStatistics userStats, SegmentStatistics typeStats, SegmentStatistics actionStats) {
            this.id = id;
            this.tableId = tableId;
            this.startRowId = startRowId;
            this.endRowId = endRowId;
            this.rowCount = rowCount;
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.worldIds = worldIds;
            this.chunkFilter = chunkFilter;
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

        TableLayout(String[] columns, int[] types, String[] declaredTypes) {
            this.columns = columns;
            this.types = types;
            this.declaredTypes = declaredTypes;
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
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("PRAGMA table_info(" + ConfigHandler.prefix + table + ")")) {
            while (results.next()) {
                String name = results.getString("name");
                String declaredType = results.getString("type");
                columns.add(name);
                declared.add(declaredType == null || declaredType.isEmpty() ? "INTEGER" : declaredType);
                types.add(columnType(declaredType));
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

        TableLayout layout = new TableLayout(columns.toArray(new String[0]), typeArray, declared.toArray(new String[0]));
        LAYOUTS.put(table, layout);
        return layout;
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
    public static void reload(Connection connection) throws SQLException {
        Map<Integer, List<ColdSegment>> loadedSegments = new HashMap<>();
        String query = "SELECT id,table_id,start_rowid,end_rowid,row_count,min_time,max_time,wid_set,chunk_filter,user_filter,type_filter,spawn_filter,user_stats,type_stats,action_stats FROM " + ConfigHandler.prefix + "segment ORDER BY table_id,start_rowid";
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                int tableId = results.getInt("table_id");
                ColdSegment segment = new ColdSegment(
                        results.getLong("id"),
                        tableId,
                        results.getLong("start_rowid"),
                        results.getLong("end_rowid"),
                        results.getInt("row_count"),
                        results.getLong("min_time"),
                        results.getLong("max_time"),
                        readWorldIds(results.getBytes("wid_set")),
                        SegmentFilter.fromBytes(results.getBytes("chunk_filter")),
                        SegmentMembership.decode(results.getBytes("user_filter")),
                        SegmentMembership.decode(results.getBytes("type_filter")),
                        SegmentMembership.decode(results.getBytes("spawn_filter")),
                        SegmentStatistics.decode(results.getBytes("user_stats")),
                        SegmentStatistics.decode(results.getBytes("type_stats")),
                        SegmentStatistics.decode(results.getBytes("action_stats")));
                loadedSegments.computeIfAbsent(tableId, key -> new ArrayList<>()).add(segment);
            }
        }

        segments = loadedSegments;
        loaded = true;
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

    private static void ensureLoaded(Connection connection) throws SQLException {
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
        private long plannedOffset;
        private boolean outOfReach;
        private final Map<String, Long> skipped = new HashMap<>();
        private boolean counting;
        private final Map<String, Long> counts = new HashMap<>();
        private long[] excludedUsers;
        private long[] excludedTypes;
        private long[] excludedActions;
        private boolean predicateUnmodelled;
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
     * Tells the reader that the lookup is showing a page this far into its results, so segments
     * holding only rows above the page can be skipped by arithmetic instead of being read.
     *
     * @param offset
     *            the offset of the page being shown, or 0 to read from the newest row
     */
    public static void setPlannedOffset(long offset) {
        LookupContext context = LOOKUP.get();
        if (context != null) {
            context.plannedOffset = Math.max(0, offset);
            context.skipped.clear();
        }
    }

    /**
     * @param table
     *            an unprefixed table name
     * @return how many matching rows the planner skipped without reading them, which is how much
     *         the query's own offset has to be reduced by
     */
    public static long skippedRows(String table) {
        LookupContext context = LOOKUP.get();
        return context == null ? 0 : context.skipped.getOrDefault(table, 0L);
    }

    /**
     * @return true if the lookup asked for more compressed rows than a single lookup may read
     */
    public static boolean isOutOfReach() {
        LookupContext context = LOOKUP.get();
        return context != null && context.outOfReach;
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
        context.predicateUnmodelled = where == null || where.contains(" OR ");
        if (context.predicateUnmodelled) {
            // Alternatives are not read, so no part of the clause can be treated as required, and
            // a count taken from the reader alone would describe more rows than the query returns.
            context.users = null;
            context.types = null;
            context.actions = null;
            context.excludedUsers = null;
            context.excludedTypes = null;
            context.excludedActions = null;
            return;
        }

        String user = userColumn == null || userColumn.isEmpty() ? "user" : userColumn;
        context.users = parseIdentifiers(listedValues(where, user, false));
        context.types = parseIdentifiers(listedValues(where, "type", false));
        context.actions = parseIdentifiers(listedValues(where, "action", false));
        context.excludedUsers = parseIdentifiers(listedValues(where, user, true));
        context.excludedTypes = parseIdentifiers(listedValues(where, "type", true));
        context.excludedActions = parseIdentifiers(listedValues(where, "action", true));
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
        Matcher matcher = Pattern.compile("(?<![A-Za-z0-9_])" + Pattern.quote(column) + "\\s+" + (negated ? "NOT\\s+IN" : "IN") + "\\s*\\(([-0-9,\\s]+)\\)").matcher(where);
        if (!matcher.find()) {
            return null;
        }
        String values = matcher.group(1);
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
     * Marks the start of a lookup, so the tables it reads from can take its time range into account
     * and the temporary tables it creates can be cleaned up afterwards.
     *
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0 for no lower bound
     * @param endTime
     *            the latest timestamp the lookup wants, or 0 for no upper bound
     */
    public static void beginLookup(long startTime, long endTime) {
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
        if (context == null || connection == null) {
            return;
        }
        for (String table : context.materialized) {
            releaseMaterialized(connection, table);
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
        if (!ConfigHandler.databaseType.isSQLite() || !SEGMENTED_TABLES.containsKey(table)) {
            return hotTable;
        }

        ensureLoaded(connection);
        if (!hasSegments()) {
            return hotTable;
        }

        LookupContext context = LOOKUP.get();
        long startTime = context == null ? 0 : context.startTime;
        long endTime = context == null ? 0 : context.endTime;
        long[] users = context == null ? null : context.users;
        long[] types = context == null ? null : context.types;
        long[] actions = context == null ? null : context.actions;
        Exclusions excluded = exclusions(context);
        List<ColdSegment> selected = selectSegments(connection, table, worldId, bounds, startTime, endTime, null);
        selected = restrictToIdentifiers(selected, users, types, context == null ? null : context.spawns);
        if (selected.isEmpty()) {
            return hotTable;
        }

        long started = System.nanoTime();
        // Coordinates are only summarised as chunks, and the height a lookup asks for is not read
        // at all, so a count taken without the rows would describe more of them than the query
        // returns. Those rows are built and handed over instead.
        if (context != null && context.counting && !context.predicateUnmodelled && bounds == null) {
            // Counting needs no rows, only how many there are, so nothing is built or copied.
            long counted = countRows(connection, table, selected, worldId, bounds, startTime, endTime, users, types, actions, excluded);
            context.counts.put(table, counted);
            debug("count " + table + ": " + selected.size() + " of " + segments.getOrDefault(SEGMENTED_TABLES.get(table), Collections.emptyList()).size()
                    + " segments, " + counted + " rows, " + millis(started) + " ms");
            return hotTable;
        }

        String expression;
        try {
            expression = materialize(connection, table, selected, worldId, bounds, startTime, endTime, users, types, actions, excluded, context);
        }
        catch (SQLException exception) {
            // Scratch space ran out, or the copy failed part way. The lookup still has to answer,
            // so it falls back to live rows alone and says so rather than failing outright.
            releaseMaterialized(connection, table);
            if (context != null) {
                context.outOfReach = true;
            }
            debug("read " + table + ": could not be read (" + exception.getMessage() + "), live rows only");
            return hotTable;
        }
        if (context != null) {
            // The offset applies to the one table it was set for. Clearing it here means a lookup
            // that reads several tables cannot skip rows in the others without asking again, which
            // would shift a merged result without the query's offset knowing about it.
            context.plannedOffset = 0;
        }
        debug("read " + table + ": " + selected.size() + " of " + segments.getOrDefault(SEGMENTED_TABLES.get(table), Collections.emptyList()).size()
                + " segments, budget " + (context == null ? 0 : context.rowBudget) + ", " + millis(started) + " ms"
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
        if (Config.getGlobal().COLD_DEBUG) {
            net.coreprotect.utility.Chat.sendConsoleMessage(net.coreprotect.utility.Color.GREY + "[CoreProtect] cold: " + message);
        }
    }

    private static long millis(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000L;
    }

    /**
     * Finds a timestamp old enough that roughly the requested number of matching rows lie above it.
     *
     * <p>
     * A lookup showing a page deep into a long history would otherwise have to read every row above
     * that page just to count past them. Segments record how many rows each player and block type
     * contributed, so walking them newest first gives a timestamp to cut at, after which only the
     * rows around the requested page have to be read at all.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            the unprefixed table to measure, normally the busiest one
     * @param users
     *            a comma separated list of user ids the lookup wants, or an empty string
     * @param types
     *            a comma separated list of block type ids the lookup wants, or an empty string
     * @param targetRows
     *            how many rows should lie above the cut
     * @return a timestamp to cut at, or 0 when there is no useful cut
     * @throws SQLException
     *             if the metadata cannot be read
     */
    public static long chooseCutTime(Connection connection, String table, String users, String types, long targetRows) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null || targetRows <= 0) {
            return 0;
        }

        ensureLoaded(connection);
        List<ColdSegment> candidates = new ArrayList<>(segments.getOrDefault(tableId, Collections.emptyList()));
        if (candidates.isEmpty()) {
            return 0;
        }
        candidates.sort(Comparator.comparingLong((ColdSegment segment) -> segment.endRowId).reversed());

        long[] wantedUsers = parseIdentifiers(users);
        long[] wantedTypes = parseIdentifiers(types);
        long above = 0;
        for (ColdSegment segment : candidates) {
            above = above + segment.estimatedRows(wantedUsers, wantedTypes);
            if (above >= targetRows) {
                // Everything in this segment and newer stays above the cut.
                return segment.minTime;
            }
        }
        return 0;
    }

    /**
     * Lists the timestamps a query could be cut at, newest first.
     *
     * <p>
     * Segments are contiguous runs of rows, so the moment one starts is a natural place to divide
     * the history. Cutting anywhere else would mean reading a segment to find out where inside it
     * the boundary fell.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @return candidate cut timestamps, newest first
     * @throws SQLException
     *             if the metadata cannot be read
     */
    public static long[] cutCandidates(Connection connection, String table) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null) {
            return new long[0];
        }

        ensureLoaded(connection);
        List<ColdSegment> candidates = new ArrayList<>(segments.getOrDefault(tableId, Collections.emptyList()));
        candidates.sort(Comparator.comparingLong((ColdSegment segment) -> segment.minTime).reversed());

        long[] times = new long[candidates.size()];
        for (int index = 0; index < times.length; index++) {
            times[index] = candidates.get(index).minTime;
        }
        return times;
    }

    /**
     * Counts exactly how many matching rows of a table lie above a cut, live rows included.
     *
     * <p>
     * Segments entirely above the cut contribute the counts recorded when they were sealed, so they
     * are never read. Only a segment straddling the cut has to be decoded, and there is at most one
     * of those per table. Live rows are counted by the database, which has an index for it.
     * </p>
     *
     * @param connection
     *            an open connection
     * @param table
     *            an unprefixed table name
     * @param cutTime
     *            rows newer than this are counted
     * @param users
     *            a comma separated list of user ids, or an empty string
     * @param types
     *            a comma separated list of block type ids, or an empty string
     * @param actions
     *            a comma separated list of action ids, or null
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0
     * @param endTime
     *            the latest timestamp the lookup wants, or 0
     * @return the exact number of matching rows above the cut, or -1 when it cannot be counted
     * @throws SQLException
     *             if the database cannot be read
     */
    public static long countAbove(Connection connection, String table, long cutTime, String users, String types, String actions, Exclusions excluded, long startTime, long endTime) throws SQLException {
        Integer tableId = SEGMENTED_TABLES.get(table);
        if (tableId == null) {
            return 0;
        }

        ensureLoaded(connection);
        long[] wantedUsers = parseIdentifiers(users);
        long[] wantedTypes = parseIdentifiers(types);
        long[] wantedActions = parseIdentifiers(actions);
        TableLayout layout = layout(connection, table);
        if (layout == null || layout.timeColumn < 0) {
            return -1;
        }

        long above = 0;
        for (ColdSegment segment : segments.getOrDefault(tableId, Collections.emptyList())) {
            if (segment.maxTime <= cutTime) {
                continue; // entirely at or below the cut
            }
            if (endTime > 0 && segment.minTime > endTime) {
                continue; // entirely outside the window the lookup asked for
            }

            boolean entirelyAbove = segment.minTime > cutTime && (startTime <= 0 || segment.minTime > startTime)
                    && (endTime <= 0 || segment.maxTime <= endTime);
            if (entirelyAbove) {
                long exact = exactRows(segment, 0, null, 0, 0, wantedUsers, wantedTypes, wantedActions, excluded);
                if (exact >= 0) {
                    above = above + exact;
                    continue;
                }
            }

            // The segment straddles the cut, or its counts cannot answer this lookup, so read it.
            ColdSegmentCodec.RowFilter filter = rowFilter(layout, 0, null, Math.max(cutTime, startTime), endTime, wantedUsers, wantedTypes, wantedActions, excluded);
            above = above + countBatch(connection, Collections.singletonList(segment), filter);
        }

        // Live rows are newer than every compressed row, so they all count when they match.
        above = above + countLiveRowsAbove(connection, table, layout, cutTime, endTime, wantedUsers, wantedTypes, wantedActions, excluded);
        return above;
    }

    private static long countLiveRowsAbove(Connection connection, String table, TableLayout layout, long cutTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded) throws SQLException {
        StringBuilder query = new StringBuilder("SELECT COUNT(*) FROM " + ConfigHandler.prefix + table + " WHERE time > " + cutTime);
        if (endTime > 0) {
            query.append(" AND time <= ").append(endTime);
        }
        appendIn(query, layout.userColumn >= 0 ? "user" : null, users);
        appendIn(query, layout.typeColumn >= 0 ? "type" : null, types);
        appendIn(query, layout.actionColumn >= 0 ? "action" : null, actions);
        Exclusions exclusions = excluded == null ? Exclusions.NONE : excluded;
        appendNotIn(query, layout.userColumn >= 0 ? "user" : null, exclusions.users);
        appendNotIn(query, layout.typeColumn >= 0 ? "type" : null, exclusions.types);
        appendNotIn(query, layout.actionColumn >= 0 ? "action" : null, exclusions.actions);

        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query.toString())) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private static void appendNotIn(StringBuilder query, String column, long[] values) {
        if (column == null || values == null || values.length == 0) {
            return;
        }
        query.append(" AND ").append(column).append(" NOT IN(");
        for (int index = 0; index < values.length; index++) {
            if (index > 0) {
                query.append(',');
            }
            query.append(values[index]);
        }
        query.append(')');
    }

    private static void appendIn(StringBuilder query, String column, long[] values) {
        if (column == null || values == null || values.length == 0) {
            return;
        }
        query.append(" AND ").append(column).append(" IN(");
        for (int index = 0; index < values.length; index++) {
            if (index > 0) {
                query.append(',');
            }
            query.append(values[index]);
        }
        query.append(')');
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

        long[] chunkKeys = chunkKeys(worldId, bounds);
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
            if (chunkKeys != null && segment.chunkFilter != null && !matchesChunk(segment, chunkKeys)) {
                continue;
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
     * Builds the chunk keys a location restricted lookup covers, or null when there are too many to
     * be worth testing.
     */
    private static long[] chunkKeys(int worldId, Integer[] bounds) {
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
        return materialize(connection, table, selected, worldId, bounds, startTime, endTime, users, types, null, Exclusions.NONE, null);
    }

    private static String materialize(Connection connection, String table, List<ColdSegment> selected, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded, LookupContext context) throws SQLException {
        TableLayout layout = layout(connection, table);
        if (layout == null) {
            return ConfigHandler.prefix + table;
        }
        String temporary = "cp_cold_" + table;
        String hotTable = ConfigHandler.prefix + table;

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
        // One transaction for the whole temporary table: committing every batch separately costs
        // far more than decoding the rows does.
        boolean autoCommit = connection.getAutoCommit();
        if (autoCommit) {
            connection.setAutoCommit(false);
        }
        try (PreparedStatement statement = connection.prepareStatement(insert.toString())) {
            ColdSegmentCodec.RowFilter rowFilter = rowFilter(layout, worldId, bounds, startTime, endTime, users, types, actions, excluded,
                    context == null ? null : context.spawns);
            long budget = context == null ? 0 : context.rowBudget;
            List<ColdSegment> order = selected;

            // Skip whole segments the page starts after. Their row counts were recorded when they
            // were sealed, so how many rows they hold for this lookup is known without reading
            // them, and the query's own offset is reduced by exactly that many.
            long plannedOffset = context == null ? 0 : context.plannedOffset;
            if (plannedOffset > 0 && !selected.isEmpty()) {
                List<ColdSegment> newestFirst = new ArrayList<>(selected);
                newestFirst.sort(Comparator.comparingLong((ColdSegment segment) -> segment.endRowId).reversed());

                long skippedRows = 0;
                int skippedSegments = 0;
                for (ColdSegment segment : newestFirst) {
                    long rows = exactRows(segment, worldId, bounds, startTime, endTime, users, types, actions, excluded);
                    if (rows < 0 || skippedRows + rows > plannedOffset) {
                        // Either the count is not known exactly, or the page starts inside this
                        // segment. Either way this segment and everything older has to be read.
                        break;
                    }
                    skippedRows = skippedRows + rows;
                    skippedSegments++;
                }

                if (skippedSegments > 0) {
                    order = newestFirst.subList(skippedSegments, newestFirst.size());
                    selected = order;
                    context.skipped.put(table, skippedRows);
                    budget = Math.max(0, budget - skippedRows);
                    context.rowBudget = budget;
                    debug("plan " + table + ": skipped " + skippedSegments + " segments holding " + skippedRows
                            + " rows, offset " + plannedOffset + " -> " + (plannedOffset - skippedRows));
                }
            }

            // Reading a scratch copy of tens of millions of rows would exhaust memory or disk, and
            // no page is worth that. The lookup is told the page is out of reach instead.
            long maximumRows = Config.getGlobal().COLD_MAX_ROWS > 0 ? Config.getGlobal().COLD_MAX_ROWS : DEFAULT_MAXIMUM_ROWS;
            if (budget > maximumRows) {
                context.outOfReach = true;
                debug("read " + table + ": page needs " + budget + " rows, more than the " + maximumRows + " allowed");
                try (Statement cleanup = connection.createStatement()) {
                    cleanup.executeUpdate("DROP TABLE IF EXISTS temp." + temporary);
                }
                if (autoCommit) {
                    connection.setAutoCommit(true);
                }
                return hotTable(table);
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
                for (ColdSegment segment : order) {
                    needed++;
                    // Only counts that describe this lookup exactly may be trusted here. A segment
                    // the lookup cuts across holds fewer matching rows than its own count says, and
                    // trusting it would stop the read before the page was covered.
                    long rows = exactRows(segment, worldId, bounds, startTime, endTime, users, types, actions, excluded);
                    if (rows < 0) {
                        countsKnown = false;
                        break;
                    }
                    estimated = estimated + rows;
                    if (estimated >= budget) {
                        break;
                    }
                }

                if (countsKnown && needed < order.size()) {
                    context.truncated = true;
                    order = order.subList(0, needed);
                }
            }
            // A small page can afford to decode several segments at once; a large read cannot.
            int wave = budget > 0 && budget <= LARGE_READ_ROWS ? DECODE_WAVE : 1;
            for (int position = 0; position < order.size(); position += wave) {
                if (budget > 0 && inserted >= budget) {
                    context.truncated = true;
                    break;
                }
                if (context != null && context.outOfReach) {
                    break;
                }
                List<ColdSegment> batch = order.subList(position, Math.min(order.size(), position + wave));
                for (ColdSegmentCodec.Rows rows : decodeBatch(connection, batch, rowFilter)) {
                for (int row = 0; row < rows.size(); row++) {
                    long rowId = rows.getRowId(row);
                    Object[] values = rows.getValues(row);
                    Integer overlayFlag = layout.rolledBackColumn >= 0 ? overlay.get(rowId) : null;

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
                    statement.addBatch();
                    inserted++;
                    if (inserted % 2000 == 0) {
                        statement.executeBatch();
                    }

                    // The limit is enforced on rows actually written, not only on what the read
                    // was expected to need, so a read can never grow beyond it whatever happens.
                    if (inserted > maximumRows) {
                        context.outOfReach = true;
                        debug("read " + table + ": stopped after " + inserted + " rows, more than the " + maximumRows + " allowed");
                        break;
                    }
                }
                }
            }
            statement.executeBatch();
        }
        finally {
            if (autoCommit) {
                if (inserted > 0) {
                    // SQLite only opens a transaction once something is written, so committing
                    // when nothing was inserted is an error rather than a no-op.
                    connection.commit();
                }
                connection.setAutoCommit(true);
            }
        }

        if (context != null && context.outOfReach) {
            try (Statement cleanup = connection.createStatement()) {
                cleanup.executeUpdate("DROP TABLE IF EXISTS temp." + temporary);
            }
            return hotTable(table);
        }

        return "(SELECT rowid AS rowid,* FROM " + hotTable + " UNION ALL SELECT rowid AS rowid,* FROM temp." + temporary + ") AS " + table + "_rows";
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
     * The counts recorded at seal time are exact, but they describe the whole segment. They can
     * only be used when the lookup does not cut across it: a coordinate restriction or a time
     * window that covers part of the segment would match fewer rows than the count says.
     * </p>
     *
     * @return the exact number of matching rows, or -1 when it cannot be known from metadata alone
     */
    private static long exactRows(ColdSegment segment, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded) {
        if (bounds != null) {
            return -1; // coordinates are not summarised, so the segment has to be read
        }
        if (startTime > 0 && segment.minTime <= startTime) {
            return -1; // the window starts inside this segment
        }
        if (endTime > 0 && segment.maxTime > endTime) {
            return -1; // the window ends inside this segment
        }
        if (worldId > 0 && (segment.worldIds.length != 1 || segment.worldIds[0] != worldId)) {
            return -1; // the segment mixes worlds, so its counts cover more than this one
        }

        Exclusions exclusions = excluded == null ? Exclusions.NONE : excluded;
        // Counts are recorded one column at a time, so they can answer a restriction on one column
        // but never the overlap between two of them.
        int constrained = 0;
        if (users != null || exclusions.users != null) {
            constrained++;
        }
        if (types != null || exclusions.types != null) {
            constrained++;
        }
        if (actions != null || exclusions.actions != null) {
            constrained++;
        }
        if (constrained > 1) {
            return -1;
        }

        if (users != null || exclusions.users != null) {
            return countFromStatistics(segment, segment.userStats, users, exclusions.users);
        }
        if (types != null || exclusions.types != null) {
            return countFromStatistics(segment, segment.typeStats, types, exclusions.types);
        }
        if (actions != null || exclusions.actions != null) {
            return countFromStatistics(segment, segment.actionStats, actions, exclusions.actions);
        }
        return segment.rowCount;
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
    private static ColdSegmentCodec.RowFilter rowFilter(TableLayout layout, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded) {
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

        if (!checkBounds && !checkWorld && !checkTime && wantedUsers == null && wantedTypes == null && wantedActions == null
                && wantedSpawns == null && unwantedUsers == null && unwantedTypes == null && unwantedActions == null) {
            return null;
        }

        return (columns, present) -> {
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
        };
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
     * @return the number of matching rows
     * @throws SQLException
     *             if a segment cannot be read
     */
    private static long countRows(Connection connection, String table, List<ColdSegment> selected, int worldId, Integer[] bounds, long startTime, long endTime, long[] users, long[] types, long[] actions, Exclusions excluded) throws SQLException {
        TableLayout layout = layout(connection, table);
        if (layout == null) {
            return 0;
        }

        ColdSegmentCodec.RowFilter filter = rowFilter(layout, worldId, bounds, startTime, endTime, users, types, actions, excluded);
        long counted = 0;
        List<ColdSegment> mustRead = new ArrayList<>(selected.size());

        for (ColdSegment segment : selected) {
            // Each segment recorded how many rows it holds for each player and block type when it
            // was sealed. Where the query covers the segment whole, adding those numbers up gives
            // the same answer as reading every row, so the rows are never touched.
            long exact = exactRows(segment, worldId, bounds, startTime, endTime, users, types, actions, excluded);
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
     * Reads and decodes a handful of segments, decoding them in parallel but returning them in the
     * order they were given, so the rows still enter the temporary table oldest first.
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
    /**
     * Counts the matching rows of a handful of segments in parallel, building none of them.
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
        List<SegmentFrames> frames = new ArrayList<>(batch.size());
        for (ColdSegment segment : batch) {
            SegmentFrames read = readFrames(connection, segment);
            SegmentDictionary.preload(read.dictionaryId, connection);
            frames.add(read);
        }

        if (frames.size() == 1 || DECODE_THREADS == 1) {
            return countFrames(frames.get(0), filter);
        }

        List<java.util.concurrent.Future<Integer>> pending = new ArrayList<>(frames.size());
        for (SegmentFrames read : frames) {
            pending.add(decodePool().submit(() -> countFrames(read, filter)));
        }

        long counted = 0;
        for (java.util.concurrent.Future<Integer> future : pending) {
            try {
                counted = counted + future.get();
            }
            catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while reading compressed storage", exception);
            }
            catch (java.util.concurrent.ExecutionException exception) {
                throw new SQLException("Unable to read compressed storage", exception.getCause());
            }
        }
        return counted;
    }

    private static int countFrames(SegmentFrames frames, ColdSegmentCodec.RowFilter filter) throws SQLException {
        ColdSegmentReadCounter.recordRead();
        byte[] scalars = SegmentDictionary.decompress(frames.scalars, frames.scalarSize, 0, null);
        byte[] payload = frames.payload == null ? new byte[0]
                : SegmentDictionary.decompress(frames.payload, frames.payloadSize, frames.dictionaryId, null);
        return ColdSegmentCodec.count(scalars, payload, filter);
    }

    private static List<ColdSegmentCodec.Rows> decodeBatch(Connection connection, List<ColdSegment> batch, ColdSegmentCodec.RowFilter filter) throws SQLException {
        List<SegmentFrames> frames = new ArrayList<>(batch.size());
        for (ColdSegment segment : batch) {
            SegmentFrames read = readFrames(connection, segment);
            // Load any dictionary while the connection is still ours to use.
            SegmentDictionary.preload(read.dictionaryId, connection);
            frames.add(read);
        }

        if (frames.size() == 1 || DECODE_THREADS == 1) {
            return Collections.singletonList(decodeFrames(frames.get(0), filter));
        }

        List<java.util.concurrent.Future<ColdSegmentCodec.Rows>> pending = new ArrayList<>(frames.size());
        for (SegmentFrames read : frames) {
            pending.add(decodePool().submit(() -> decodeFrames(read, filter)));
        }

        List<ColdSegmentCodec.Rows> decoded = new ArrayList<>(pending.size());
        for (java.util.concurrent.Future<ColdSegmentCodec.Rows> future : pending) {
            try {
                decoded.add(future.get());
            }
            catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while reading compressed storage", exception);
            }
            catch (java.util.concurrent.ExecutionException exception) {
                throw new SQLException("Unable to read compressed storage", exception.getCause());
            }
        }
        return decoded;
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

        String query = "SELECT rowid_ref,rolled_back FROM " + ConfigHandler.prefix + "cold_flag WHERE table_id = ? AND rowid_ref BETWEEN ? AND ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            for (ColdSegment segment : selected) {
                statement.setInt(1, tableId);
                statement.setLong(2, segment.startRowId);
                statement.setLong(3, segment.endRowId);
                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        overlay.put(results.getLong("rowid_ref"), results.getInt("rolled_back"));
                    }
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
    private static final int DECODE_THREADS = Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() - 1));

    /** Segments decoded at once. Bounds how much decoded data is held before it is inserted. */
    private static final int DECODE_WAVE = 8;

    /**
     * Above this many rows a read decodes one segment at a time. Decoding several at once is
     * quicker, but each one holds its rows in memory until they are written, and a lookup reaching
     * far back through history must not be able to exhaust the server's memory to go faster.
     */
    private static final long LARGE_READ_ROWS = 100_000;

    /** Used when the configured limit is unavailable, so a read is never accidentally unbounded. */
    private static final long DEFAULT_MAXIMUM_ROWS = 1_000_000;

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

    /** The stored frames of one segment, before they are decoded. */
    private static final class SegmentFrames {
        private final byte[] scalars;
        private final int scalarSize;
        private final byte[] payload;
        private final int payloadSize;
        private final int dictionaryId;

        private SegmentFrames(byte[] scalars, int scalarSize, byte[] payload, int payloadSize, int dictionaryId) {
            this.scalars = scalars;
            this.scalarSize = scalarSize;
            this.payload = payload;
            this.payloadSize = payloadSize;
            this.dictionaryId = dictionaryId;
        }
    }

    private static SegmentFrames readFrames(Connection connection, ColdSegment segment) throws SQLException {
        String query = "SELECT scalars,scalars_size,payload,payload_size,dict_id FROM " + ConfigHandler.prefix + "segment WHERE id = ? LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, segment.id);
            try (ResultSet results = statement.executeQuery()) {
                if (!results.next()) {
                    throw new SQLException("Cold segment " + segment.id + " is missing");
                }
                return new SegmentFrames(results.getBytes("scalars"), results.getInt("scalars_size"),
                        results.getBytes("payload"), results.getInt("payload_size"), results.getInt("dict_id"));
            }
        }
    }

    private static ColdSegmentCodec.Rows decodeFrames(SegmentFrames frames, ColdSegmentCodec.RowFilter filter) throws SQLException {
        ColdSegmentReadCounter.recordRead();
        byte[] scalars = SegmentDictionary.decompress(frames.scalars, frames.scalarSize, 0, null);
        byte[] payload = frames.payload == null ? new byte[0]
                : SegmentDictionary.decompress(frames.payload, frames.payloadSize, frames.dictionaryId, null);
        return ColdSegmentCodec.decode(scalars, payload, filter);
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
                byte[] payload = payloadFrame == null ? new byte[0]
                        : SegmentDictionary.decompress(payloadFrame, results.getInt("payload_size"), results.getInt("dict_id"), connection);
                return ColdSegmentCodec.decode(scalars, payload, filter);
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
