package net.coreprotect.database;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.SQLiteColdIndex.ColdSegment;
import net.coreprotect.database.SQLiteColdIndex.KeyWindow;

/**
 * Builds and runs a lookup query whose tables hold packed rows, deciding how much of each table's
 * packed history has to be read only once the whole query is known.
 *
 * <p>
 * While the query is assembled, each table it reads is given a placeholder instead of the table
 * expression, and the placeholders are filled in here. A page whose clauses the reader can test is
 * planned exactly, see {@link ColdPagePlan}, and reads only the rows it shows. A page whose clauses
 * it cannot test, a count of one, and a read of every matching row walk the history a window at a
 * time, in the order the rows are shown, so no more than one window of packed rows is ever held in
 * temporary storage. Walking costs time in proportion to how far back the page is, which is the
 * price of a clause the reader cannot evaluate: only the query itself can say which rows such a
 * clause keeps.
 * </p>
 *
 * <p>
 * Lookups outside the command path use this too, through
 * {@link net.coreprotect.database.lookup.InspectorSource} and the API's lookup filter, since a
 * single busy place can hold more packed rows than one lookup may copy out at once.
 * </p>
 */
public final class ColdLookupQuery {

    private static final String SOURCE = "/*cp_cold_source:";
    private static final String HINT = "/*cp_cold_hint:";
    private static final String COUNT = "/*cp_cold_count:";
    private static final String END = "*/";

    /** Stands in for the ORDER BY and LIMIT each table of a merged page is cut to. */
    static final String UNION_LIMIT = "/*cp_cold_union_limit*/";

    /** How each table of a merged page is ordered before it is cut, newest first. */
    private static final String UNION_ORDER = " ORDER BY time DESC, id DESC";

    /** How each table of a merged page is ordered before it is cut, oldest first. */
    private static final String UNION_ORDER_ASCENDING = " ORDER BY time ASC, id ASC";

    /** One table the query reads. */
    private static final class Branch {
        final String table;
        final String clause;
        final int source;
        final int worldId;
        final Integer[] bounds;
        final String alias;
        String hint = "";
        String expression;

        Branch(String table, String clause, int source, int worldId, Integer[] bounds, String alias) {
            this.table = table;
            this.clause = clause;
            this.source = source;
            this.worldId = worldId;
            this.bounds = bounds;
            this.alias = alias;
        }
    }

    private final Connection connection;
    private final String userColumn;
    private final List<Branch> branches = new ArrayList<>();
    private long total = -1;

    private ColdLookupQuery(Connection connection, String userColumn) {
        this.connection = connection;
        this.userColumn = userColumn;
    }

    /**
     * @return a query builder for a lookup on this connection, or null when nothing it reads can
     *         hold packed rows, so the tables can be read as they are
     */
    public static ColdLookupQuery forLookup(Connection connection, String userColumn) throws SQLException {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return null;
        }
        SQLiteColdIndex.ensureLoaded(connection);
        return SQLiteColdIndex.hasSegments() ? new ColdLookupQuery(connection, userColumn) : null;
    }

    /**
     * Registers a table the query reads and returns the placeholder to use in its place.
     *
     * @param table
     *            the unprefixed table name
     * @param clause
     *            the WHERE clause the query applies to it
     * @param source
     *            the number the query tells this table's rows apart by
     * @param worldId
     *            the world the reader may restrict the table to, or 0
     * @param bounds
     *            the coordinate bounds the reader may restrict the table to, or null
     * @return the placeholder
     */
    public String source(String table, String clause, int source, int worldId, Integer[] bounds) {
        return source(table, clause, source, worldId, bounds, null);
    }

    /**
     * Registers a table the query reads under a name of its own, as a query that joins it to
     * another table needs, and returns the placeholder to use in its place.
     *
     * @param alias
     *            the name the query gives the table, or null for none
     * @return the placeholder, which stands for the table and its name together
     * @see #source(String, String, int, int, Integer[])
     */
    public String source(String table, String clause, int source, int worldId, Integer[] bounds, String alias) {
        branches.add(new Branch(table, clause, source, worldId, bounds, alias));
        return "(" + SOURCE + (branches.size() - 1) + END + ")";
    }

    /**
     * @return true if the text is a placeholder this class gave out for a table
     */
    static boolean isSource(String text) {
        return text != null && text.startsWith("(" + SOURCE);
    }

    /**
     * @param source
     *            a placeholder for a table
     * @param index
     *            the index hint the query would give the plain table
     * @return a placeholder for the hint, which is kept only if the table is read as it is
     */
    public String hint(String source, String index) {
        int branch = branchOf(source);
        branches.get(branch).hint = index;
        return HINT + branch + END;
    }

    /**
     * @param table
     *            the unprefixed table a count reads
     * @return a placeholder for the count column of the most recently registered read of it
     */
    public String countColumn(String table) {
        for (int index = branches.size() - 1; index >= 0; index--) {
            if (branches.get(index).table.equals(table)) {
                return COUNT + index + END;
            }
        }
        return "COUNT(*) as count";
    }

    private static int branchOf(String source) {
        int start = source.indexOf(SOURCE) + SOURCE.length();
        return Integer.parseInt(source.substring(start, source.indexOf(END, start)));
    }

    /**
     * Runs a page of the lookup.
     *
     * @param statement
     *            the statement the lookup runs on
     * @param body
     *            the query with placeholders, without its final order or limit
     * @param order
     *            the final ORDER BY
     * @param offset
     *            how many rows come before the page
     * @param limit
     *            how many rows the page shows
     * @param bindings
     *            values for the query's parameters
     * @return the page's rows
     * @throws Exception
     *             if the lookup cannot be read
     */
    public ResultSet page(Statement statement, String body, String order, long offset, int limit, List<String> bindings) throws Exception {
        return page(statement, body, order, offset, limit, bindings, false);
    }

    /**
     * Runs a page of the lookup, and counts every matching row while planning it when asked to.
     *
     * <p>
     * Counting while planning costs little more than planning: the count of every segment that
     * can hold a match is settled once and serves both. Where the page cannot be planned the count
     * is left to {@link #count}, and {@link #total()} says so.
     * </p>
     *
     * @param counted
     *            true to count every matching row, see {@link #total()}
     * @see #page(Statement, String, String, long, int, List)
     */
    public ResultSet page(Statement statement, String body, String order, long offset, int limit, List<String> bindings, boolean counted) throws Exception {
        prepare();
        Boolean byTime = byTime(order);
        boolean ascending = ascending(order);
        String unionOrder = ascending ? UNION_ORDER_ASCENDING : UNION_ORDER;
        if (byTime == null) {
            resolveAll();
            return execute(statement, fill(body, unionOrder + " LIMIT " + (offset + limit)) + order + " LIMIT " + limit + " OFFSET " + offset, bindings);
        }

        List<ColdPagePlan.Branch> planned = new ArrayList<>();
        for (Branch branch : branches) {
            planned.add(new ColdPagePlan.Branch(branch.table, branch.source, branch.clause, branch.worldId, branch.bounds));
        }
        ColdPagePlan plan = ColdPagePlan.plan(connection, planned, byTime, userColumn, offset, limit, counted, ascending);
        if (plan != null) {
            total = plan.total();
            // The page is exactly the rows between its first and last, so the query reads it
            // with no offset, and each table has no more than the page to give.
            for (Branch branch : branches) {
                SQLiteColdIndex.setPageWindow(branch.table, plan.window(branch.table));
            }
            resolveAll();
            return execute(statement, fill(body, unionOrder + " LIMIT " + limit) + order + " LIMIT " + limit + " OFFSET 0", bindings);
        }

        SQLiteColdIndex.debug("page at " + offset + ": clause cannot be planned, walking the history a window at a time");
        return chain(new PageWalk(statement, body, order, offset, limit, bindings, new Windows(byTime, ascending), unionOrder));
    }

    /**
     * @return how many rows the lookup matches in all, counted by the last page asked to count
     *         them, or -1 when that page could not be planned and so did not count them
     */
    public long total() {
        return total;
    }

    /**
     * Runs a count of the lookup.
     *
     * @param statement
     *            the statement the lookup runs on
     * @param body
     *            the count query with placeholders, which gives a table number and a count per row
     * @param tail
     *            what follows the body in the query
     * @param bindings
     *            values for the query's parameters
     * @return a count per table, as the count query itself would give
     * @throws Exception
     *             if the lookup cannot be read
     */
    public ResultSet count(Statement statement, String body, String tail, List<String> bindings) throws Exception {
        prepare();
        boolean countable = true;
        for (Branch branch : branches) {
            if (SQLiteColdIndex.tableCounts(connection, branch.table, branch.clause, userColumn, branch.worldId, branch.bounds) == null) {
                countable = false;
                break;
            }
        }
        if (countable) {
            // Every table's clause is one the reader can count itself, so no row is built.
            SQLiteColdIndex.setCounting(true);
            resolveAll();
            return execute(statement, fill(body, "") + tail, bindings);
        }

        // Only the query can say what such a clause keeps, so it counts one window at a time. A
        // count does not care about order, so the tables of a merged lookup are divided by time
        // and a single table by row id.
        Map<Integer, Long> totals = new LinkedHashMap<>();
        Windows windows = new Windows(branches.size() > 1, false);
        while (windows.next(this) != null) {
            try (ResultSet results = execute(statement, fill(body, ""), bindings)) {
                while (results.next()) {
                    totals.merge(results.getInt("tbl"), results.getLong("count"), Long::sum);
                }
            }
        }
        SQLiteColdIndex.releaseReads(connection);
        StringBuilder query = new StringBuilder();
        for (Map.Entry<Integer, Long> total : totals.entrySet()) {
            query.append(query.length() == 0 ? "SELECT " : " UNION ALL SELECT ").append(total.getKey()).append(" AS tbl,").append(total.getValue()).append(" AS count");
        }
        return execute(statement, query.length() == 0 ? "SELECT 0 AS tbl,0 AS count" : query.toString(), null);
    }

    /**
     * Runs a lookup of every matching row a window at a time, in the order given, so no more than
     * one window of packed rows is held at once however many rows there are.
     */
    public ResultSet all(Statement statement, String body, String order, List<String> bindings) throws Exception {
        prepare();
        Boolean byTime = byTime(order);
        if (byTime == null) {
            resolveAll();
            return execute(statement, fill(body, "") + order, bindings);
        }
        Windows windows = new Windows(byTime, ascending(order));
        return chain(() -> windows.next(this) == null ? null : execute(statement, fill(body, "") + order, bindings));
    }

    /**
     * Runs a query over every matching row in a form this class does not walk: every table is read
     * whole, within the limit on what one lookup may hold.
     */
    ResultSet whole(Statement statement, String body, UnaryOperator<String> finish, List<String> bindings) throws Exception {
        prepare();
        resolveAll();
        return execute(statement, finish.apply(fill(body, "")), bindings);
    }

    /**
     * Runs a summary of every matching row, a window at a time.
     *
     * <p>
     * A summary adds up what each player did to each type, and sums add up across any division of
     * the rows. Each window's sums are added into a temporary table holding one row per player and
     * type, and the summary is then taken of that table.
     * </p>
     *
     * @param groups
     *            turns a query of rows into a query of their sums: the player, the type, and the
     *            amounts removed, placed and net
     * @param finish
     *            turns the table of sums, and how many rows were summed, into the summary
     */
    ResultSet summary(Statement statement, String body, UnaryOperator<String> groups, java.util.function.BiFunction<String, Long, String> finish, List<String> bindings) throws Exception {
        String sums = "temp.cp_cold_" + SUMMARY;
        update("DROP TABLE IF EXISTS " + sums, null);
        update("CREATE TEMP TABLE cp_cold_" + SUMMARY + " (" + userColumn + ",type,removed_amount,placed_amount,net_amount,PRIMARY KEY(" + userColumn + ",type))", null);
        prepare();
        long records = 0;
        Windows windows = new Windows(branches.size() > 1, false);
        while (windows.next(this) != null) {
            String source = fill(body, "");
            try (ResultSet counted = execute(statement, "SELECT COUNT(*) FROM (" + source + ")", bindings)) {
                records = records + (counted.next() ? counted.getLong(1) : 0);
            }
            update("INSERT INTO " + sums + " SELECT * FROM (" + groups.apply(source) + ") WHERE 1 ON CONFLICT(" + userColumn + ",type) DO UPDATE SET "
                    + "removed_amount=removed_amount+excluded.removed_amount,placed_amount=placed_amount+excluded.placed_amount,net_amount=net_amount+excluded.net_amount", bindings);
        }
        SQLiteColdIndex.releaseReads(connection);
        // Dropped with the lookup's other temporary tables, once the summary has been read.
        SQLiteColdIndex.holdUntilEnd(SUMMARY);
        return execute(statement, finish.apply(sums, records), null);
    }

    /** The name the table of a summary's sums is kept under while the lookup runs. */
    private static final String SUMMARY = "summary_sums";

    private void update(String query, List<String> bindings) throws SQLException {
        try (PreparedStatement prepared = connection.prepareStatement(query)) {
            if (bindings != null) {
                for (int index = 0; index < bindings.size(); index++) {
                    prepared.setString(index + 1, bindings.get(index));
                }
            }
            prepared.executeUpdate();
        }
    }

    /**
     * Leaves nothing behind from an earlier operation of the same lookup: a count's mode, and the
     * stretch a page or a window confined each table to. What an earlier operation read has to be
     * read to the end before the next is started.
     */
    private void prepare() {
        total = -1;
        SQLiteColdIndex.setCounting(false);
        for (Branch branch : branches) {
            SQLiteColdIndex.setPageWindow(branch.table, null);
        }
    }

    /**
     * @return true for an order merged by time, false for row id order, null for any other order,
     *         each newest first or oldest first
     */
    private static Boolean byTime(String order) {
        String normalized = normalize(order);
        if (normalized.equals("ORDER BY rowid DESC") || normalized.equals("ORDER BY rowid ASC")) {
            return Boolean.FALSE;
        }
        if (normalized.startsWith("ORDER BY time DESC") || normalized.equals("ORDER BY time ASC,rowid ASC") || normalized.equals("ORDER BY time ASC,id ASC")) {
            return Boolean.TRUE;
        }
        return null;
    }

    /**
     * @return true if the order puts the oldest row first
     */
    private static boolean ascending(String order) {
        String normalized = normalize(order);
        return normalized.equals("ORDER BY rowid ASC") || normalized.startsWith("ORDER BY time ASC");
    }

    private static String normalize(String order) {
        return order == null ? "" : order.replaceAll("\\s+", " ").replace(", ", ",").trim();
    }

    /**
     * Reads every table with whatever window it has been given, which builds the temporary tables
     * the query reads from.
     */
    private void resolveAll() throws SQLException {
        for (Branch branch : branches) {
            SQLiteColdIndex.setPredicateFilters(branch.clause, userColumn);
            branch.expression = SQLiteColdIndex.sourceExpression(connection, branch.table, branch.worldId, branch.bounds);
        }
    }

    /**
     * Puts the table expressions, hints and counts into the query.
     */
    private String fill(String body, String unionLimit) {
        String filled = body.replace(UNION_LIMIT, unionLimit);
        for (int index = 0; index < branches.size(); index++) {
            Branch branch = branches.get(index);
            String hotTable = ConfigHandler.prefix + branch.table;
            filled = filled.replace("(" + SOURCE + index + END + ")", named(branch));
            filled = filled.replace(HINT + index + END, branch.expression.equals(hotTable) ? branch.hint : "");
            long counted = SQLiteColdIndex.countedRows(branch.table);
            filled = filled.replace(COUNT + index + END, counted > 0 ? "COUNT(*) + " + counted + " as count" : "COUNT(*) as count");
        }
        return filled;
    }

    /**
     * @return the table expression under the name the query gives it, if it gives one; a table
     *         read with its packed rows is already named after the table, and is renamed
     */
    private static String named(Branch branch) {
        if (branch.alias == null || branch.alias.isEmpty()) {
            return branch.expression;
        }
        String own = " AS " + branch.table + "_rows";
        if (branch.expression.endsWith(own)) {
            return branch.expression.substring(0, branch.expression.length() - own.length()) + " AS " + branch.alias;
        }
        return branch.expression + " AS " + branch.alias;
    }

    /**
     * Runs a query on a statement of its own, which closes with its results. A walk runs one query
     * per window, and each has to stay open while the lookup reads it.
     */
    private ResultSet execute(Statement statement, String query, List<String> bindings) throws SQLException {
        PreparedStatement prepared = connection.prepareStatement(query);
        try {
            int timeout = statement.getQueryTimeout();
            if (timeout > 0) {
                prepared.setQueryTimeout(timeout);
            }
            if (bindings != null) {
                for (int index = 0; index < bindings.size(); index++) {
                    prepared.setString(index + 1, bindings.get(index));
                }
            }
            prepared.closeOnCompletion();
            return prepared.executeQuery();
        }
        catch (SQLException exception) {
            prepared.close();
            throw exception;
        }
    }

    /**
     * The history of a lookup divided into windows of its page order, newest first or oldest
     * first, each holding few enough packed rows to be read into temporary storage at once.
     *
     * <p>
     * How many rows a window holds is judged from the rows each segment recorded, assuming they are
     * spread evenly across its span. When a window turns out to hold more than one lookup may keep,
     * it is halved and read again, so the judgement only has to be roughly right.
     * </p>
     */
    private final class Windows {
        private final boolean byTime;
        private final boolean ascending;
        private final List<long[]> spans = new ArrayList<>();
        /** The part of the history not yet read: above the floor, and up to and including high. */
        private long high;
        private long floor;
        private boolean started;

        Windows(boolean byTime, boolean ascending) {
            this.byTime = byTime;
            this.ascending = ascending;
        }

        private void start() throws SQLException {
            started = true;
            long lowest = Long.MAX_VALUE;
            long highest = Long.MIN_VALUE;
            String key = byTime ? "time" : "rowid";
            for (Branch branch : branches) {
                int[] area = SQLiteColdIndex.chunkArea(branch.worldId, branch.bounds);
                for (ColdSegment segment : SQLiteColdIndex.candidateSegments(connection, branch.table, branch.worldId, branch.bounds)) {
                    long low = byTime ? segment.minTime : segment.startRowId;
                    long top = byTime ? segment.maxTime : segment.endRowId;
                    long rows = segment.rowCount;
                    if (area != null && segment.chunkCounts != null) {
                        // A lookup about a place reads only the rows the segment holds there.
                        rows = Math.min(rows, segment.chunkCounts.rowsIn(branch.worldId, area[0], area[1], area[2], area[3]));
                    }
                    spans.add(new long[] { low, top, rows });
                    lowest = Math.min(lowest, low);
                    highest = Math.max(highest, top);
                }
                try (Statement statement = connection.createStatement();
                        ResultSet results = statement.executeQuery("SELECT MIN(" + key + "), MAX(" + key + ") FROM " + ConfigHandler.prefix + branch.table)) {
                    if (results.next() && results.getObject(1) != null) {
                        lowest = Math.min(lowest, results.getLong(1));
                        highest = Math.max(highest, results.getLong(2));
                    }
                }
            }
            high = highest;
            floor = lowest == Long.MAX_VALUE ? highest : lowest - 1;
        }

        /** Roughly how many packed rows lie above the low end and up to the high end. */
        private double estimate(long low, long top) {
            double rows = 0;
            for (long[] span : spans) {
                long from = Math.max(low + 1, span[0]);
                long to = Math.min(top, span[1]);
                if (from > to) {
                    continue;
                }
                rows = rows + span[2] * ((double) (to - from + 1) / (double) (span[1] - span[0] + 1));
            }
            return rows;
        }

        /**
         * Reads the next window into temporary storage, releasing the one before.
         *
         * @return the window read, or null when the whole history has been walked
         */
        KeyWindow next(ColdLookupQuery query) throws SQLException {
            if (!started) {
                start();
            }
            SQLiteColdIndex.releaseReads(connection);
            if (high <= floor) {
                return null;
            }
            double target = SQLiteColdIndex.materializeLimit() / 2.0;
            // The window is the part above low and up to top.
            long low = floor;
            long top = high;
            if (estimate(floor, high) > target) {
                if (ascending) {
                    // The window reaching furthest forward whose rows still fit.
                    long lowest = floor + 1;
                    long highest = high;
                    while (lowest < highest) {
                        long middle = lowest + ((highest - lowest + 1) / 2);
                        if (estimate(floor, middle) > target) {
                            highest = middle - 1;
                        }
                        else {
                            lowest = middle;
                        }
                    }
                    top = lowest;
                }
                else {
                    // The window reaching furthest back whose rows still fit.
                    long lowest = floor;
                    long highest = high - 1;
                    while (lowest < highest) {
                        long middle = lowest + ((highest - lowest) / 2);
                        if (estimate(middle, high) > target) {
                            lowest = middle + 1;
                        }
                        else {
                            highest = middle;
                        }
                    }
                    low = lowest;
                }
            }

            SQLiteColdIndex.setDivisible(true);
            try {
                while (true) {
                    KeyWindow window = byTime ? KeyWindow.ofTime(low + 1, Long.MIN_VALUE, top, Long.MAX_VALUE) : KeyWindow.ofRowIds(low + 1, top);
                    try {
                        for (Branch branch : branches) {
                            SQLiteColdIndex.setPageWindow(branch.table, window);
                        }
                        query.resolveAll();
                        if (ascending) {
                            floor = top;
                        }
                        else {
                            high = low;
                        }
                        return window;
                    }
                    catch (SQLiteColdIndex.ReadLimitException exception) {
                        SQLiteColdIndex.releaseReads(connection);
                        if (top - low <= 1) {
                            throw exception; // one second or one row id cannot be divided further
                        }
                        // Halved from the end the walk comes from.
                        if (ascending) {
                            top = low + Math.max(1, (top - low) / 2);
                        }
                        else {
                            low = top - Math.max(1, (top - low) / 2);
                        }
                    }
                }
            }
            finally {
                SQLiteColdIndex.setDivisible(false);
            }
        }
    }

    /** Hands out one result set after another, each read once the one before is finished. */
    private interface Results {
        ResultSet next() throws Exception;
    }

    /**
     * The windows of a page that cannot be planned: counted in the page's order until the one
     * holding the page's first row, then read from there until the page is full.
     */
    private final class PageWalk implements Results {
        private final Statement statement;
        private final String body;
        private final String order;
        private final List<String> bindings;
        private final Windows windows;
        private final String unionOrder;
        private long skip;
        private long wanted;

        PageWalk(Statement statement, String body, String order, long offset, int limit, List<String> bindings, Windows windows, String unionOrder) {
            this.statement = statement;
            this.body = body;
            this.order = order;
            this.bindings = bindings;
            this.windows = windows;
            this.unionOrder = unionOrder;
            this.skip = offset;
            this.wanted = limit;
        }

        @Override
        public ResultSet next() throws Exception {
            while (wanted > 0 && windows.next(ColdLookupQuery.this) != null) {
                long rows;
                try (ResultSet counted = execute(statement, "SELECT COUNT(*) FROM (" + fill(body, "") + ")", bindings)) {
                    rows = counted.next() ? counted.getLong(1) : 0;
                }
                if (rows <= skip) {
                    skip = skip - rows;
                    continue;
                }
                long offset = skip;
                long taken = Math.min(wanted, rows - offset);
                skip = 0;
                wanted = wanted - taken;
                return execute(statement, fill(body, unionOrder + " LIMIT " + (offset + taken)) + order + " LIMIT " + taken + " OFFSET " + offset, bindings);
            }
            return null;
        }
    }

    /**
     * Presents a series of result sets as one, reading each only once the one before it is used up.
     */
    private ResultSet chain(Results results) throws Exception {
        ResultSet first = results.next();
        InvocationHandler handler = new InvocationHandler() {
            private ResultSet current = first;
            private boolean closed;

            @Override
            public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
                switch (method.getName()) {
                    case "next":
                        while (current != null) {
                            if (current.next()) {
                                return Boolean.TRUE;
                            }
                            closeCurrent();
                            current = results.next();
                        }
                        return Boolean.FALSE;
                    case "close":
                        closed = true;
                        closeCurrent();
                        current = null;
                        return null;
                    case "isClosed":
                        return closed;
                    case "unwrap":
                        return proxy;
                    case "isWrapperFor":
                        return Boolean.FALSE;
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == arguments[0];
                    case "toString":
                        return "cold window results";
                    default:
                        if (current == null) {
                            throw new SQLException("The lookup has no more rows");
                        }
                        try {
                            return method.invoke(current, arguments);
                        }
                        catch (InvocationTargetException exception) {
                            throw exception.getCause();
                        }
                }
            }

            private void closeCurrent() throws SQLException {
                if (current != null) {
                    Statement owner = current.getStatement();
                    current.close();
                    if (owner != null) {
                        owner.close();
                    }
                }
            }
        };
        return (ResultSet) Proxy.newProxyInstance(ColdLookupQuery.class.getClassLoader(), new Class<?>[] { ResultSet.class }, handler);
    }
}
