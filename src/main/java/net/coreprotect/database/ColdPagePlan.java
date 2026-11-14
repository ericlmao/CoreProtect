package net.coreprotect.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.SQLiteColdIndex.ColdSegment;
import net.coreprotect.database.SQLiteColdIndex.KeyWindow;
import net.coreprotect.database.SQLiteColdIndex.TableCounts;

/**
 * Works out exactly which rows a page of a lookup shows before any of them are read, however deep
 * into the history the page lies.
 *
 * <p>
 * A page is the rows ranked from its offset to its offset plus its size in the lookup's order:
 * newest row id first for a lookup of one table, and newest time first for a lookup that merges
 * several, with ties broken by table and then by row id. The planner finds the first and last row
 * of the page in that order, and confines every table the page reads to the stretch between them.
 * The query then reads the page with no offset at all, and nothing above or below the page is ever
 * decoded into a temporary table, so the work and the temporary space a page takes do not grow
 * with how deep it is.
 * </p>
 *
 * <p>
 * Finding the two rows needs only counting, never the rows themselves. Packed segments whose rows
 * all lie above a candidate boundary contribute the counts recorded when they were sealed; live rows
 * are counted by the database through its indexes; and the few segments a boundary falls inside
 * are read for the time and id of each matching row, from their scalar frames alone. Compacted
 * segments each span a whole day, so the segments of one day per table are the ones read.
 * </p>
 *
 * <p>
 * This is only possible when the reader can test the query's whole clause itself. A lookup whose
 * clause it cannot, one with alternatives, text matching or subqueries, gets no plan, and reads its
 * history a window at a time instead.
 * </p>
 */
public final class ColdPagePlan {

    /**
     * The most matching rows whose times and ids are gathered in memory at once. Beyond this the
     * stretch holding a boundary is first narrowed down by counting rows per slice of it.
     */
    static final int COLLECT_ROWS = 500_000;

    /** How many slices a stretch is counted in when it is too large to gather. */
    static final int SLICES = 1 << 16;

    /** Segments counted at once while their counts are being settled. */
    private static final int COUNT_BATCH = 16;

    /** One table a page reads, and the clause the query applies to it. */
    public static final class Branch {
        final String table;
        final int source;
        final String clause;
        final int worldId;
        final Integer[] bounds;

        /**
         * @param table
         *            the unprefixed table name
         * @param source
         *            the number the query tells this table's rows apart by, which breaks ties in
         *            time between tables, higher first
         * @param clause
         *            the WHERE clause the query applies to the table, without the keyword
         * @param worldId
         *            the world the reader is restricted to for this table, or 0
         * @param bounds
         *            the coordinate bounds the reader is restricted to for this table, or null
         */
        public Branch(String table, int source, String clause, int worldId, Integer[] bounds) {
            this.table = table;
            this.source = source;
            this.clause = clause;
            this.worldId = worldId;
            this.bounds = bounds;
        }
    }

    private final Map<String, KeyWindow> windows;
    private final boolean empty;
    private long total = -1;

    private ColdPagePlan(Map<String, KeyWindow> windows, boolean empty) {
        this.windows = windows;
        this.empty = empty;
    }

    /**
     * @return how many rows the lookup matches in all, when the plan was asked to count them, or -1
     */
    public long total() {
        return total;
    }

    /**
     * @param table
     *            an unprefixed table name
     * @return the stretch of that table the page lies in; reading it with no offset gives the page
     */
    public KeyWindow window(String table) {
        return windows.get(table);
    }

    /**
     * @return true if the page lies past the last row, so it shows nothing
     */
    public boolean isEmpty() {
        return empty;
    }

    /**
     * Plans a page.
     *
     * @param connection
     *            an open connection
     * @param branches
     *            every table the page reads, each once
     * @param byTime
     *            true for a page merged by time across tables, false for a page of one table in
     *            row id order
     * @param userColumn
     *            the name of the user column in the clauses
     * @param offset
     *            how many rows come before the page
     * @param limit
     *            how many rows the page shows
     * @return the plan, or null when the reader cannot test some table's clause itself
     * @throws SQLException
     *             if the database cannot be read
     */
    public static ColdPagePlan plan(Connection connection, List<Branch> branches, boolean byTime, String userColumn, long offset, int limit) throws SQLException {
        return plan(connection, branches, byTime, userColumn, offset, limit, false, false);
    }

    /**
     * Plans a page, and can count every matching row on the way.
     *
     * <p>
     * Counting settles the count of every segment that can hold a match, which the page then
     * narrows by without reading anything more, so a lookup that shows a page and how many pages
     * there are pays for the count once. A page counted from the oldest row is a page of the
     * newest first order counted back from the end, which needs the count too.
     * </p>
     *
     * @param counted
     *            true to count every matching row, see {@link #total()}
     * @param fromOldest
     *            true when the offset counts from the oldest row rather than the newest
     * @return the plan, or null when the reader cannot test some table's clause itself
     * @throws SQLException
     *             if the database cannot be read
     */
    static ColdPagePlan plan(Connection connection, List<Branch> branches, boolean byTime, String userColumn, long offset, int limit, boolean counted, boolean fromOldest) throws SQLException {
        if (branches.isEmpty() || offset < 0 || limit <= 0 || (!byTime && branches.size() != 1)) {
            return null;
        }
        Set<String> tables = new HashSet<>();
        Set<Integer> sources = new HashSet<>();
        TableCounts[] counts = new TableCounts[branches.size()];
        for (int index = 0; index < branches.size(); index++) {
            Branch branch = branches.get(index);
            if (!tables.add(branch.table) || !sources.add(branch.source)) {
                // One table read twice could not be confined to one stretch, and two tables ranked
                // alike would leave ties the page's order does not settle.
                return null;
            }
            counts[index] = SQLiteColdIndex.tableCounts(connection, branch.table, branch.clause, userColumn, branch.worldId, branch.bounds);
            if (counts[index] == null) {
                return null;
            }
        }
        long started = System.nanoTime();
        ColdPagePlan plan = new Planner(connection, branches, counts, byTime).plan(offset, limit, counted || fromOldest, fromOldest);
        SQLiteColdIndex.debug("plan page at " + offset + (plan == null ? ": not planned" : plan.empty ? ": past the end" : ": " + plan.windows) + ", "
                + (System.nanoTime() - started) / 1_000_000L + " ms");
        return plan;
    }

    /** Where a row stands in a page's order. */
    private static final class Position {
        final long time;
        final int source;
        final long rowId;

        Position(long time, int source, long rowId) {
            this.time = time;
            this.source = source;
            this.rowId = rowId;
        }
    }

    /**
     * A run of rows that can match: one packed segment, or the live rows of a table, with the
     * lowest and highest key any of them can have and how many match, where that is known.
     */
    private static final class Item {
        final int branch;
        final ColdSegment segment;
        final long low;
        final long high;
        long exact;
        final long most;

        Item(int branch, ColdSegment segment, long low, long high, long exact, long most) {
            this.branch = branch;
            this.segment = segment;
            this.low = low;
            this.high = high;
            this.exact = exact;
            this.most = most;
        }

        long bound() {
            return exact >= 0 ? exact : most;
        }
    }

    /** The keys of the matching rows in a stretch, gathered in memory. */
    private static final class Gathered {
        long[] times = new long[256];
        long[] rowIds = new long[256];
        int[] sources = new int[256];
        int size;
        long above;

        void add(long time, long rowId, int source) {
            if (size == rowIds.length) {
                times = Arrays.copyOf(times, size * 2);
                rowIds = Arrays.copyOf(rowIds, size * 2);
                sources = Arrays.copyOf(sources, size * 2);
            }
            times[size] = time;
            rowIds[size] = rowId;
            sources[size] = source;
            size++;
        }
    }

    /** The matching rows of a stretch counted slice by slice. */
    private static final class Sliced {
        final long width;
        final long[] slices;
        long above;

        Sliced(long width, int count) {
            this.width = width;
            this.slices = new long[count];
        }
    }

    /** A stretch known to hold a boundary: its ends, and exactly how many rows rank above it. */
    private static final class Stretch {
        final long low;
        final long high;
        final long above;
        final long rows;

        Stretch(long low, long high, long above, long rows) {
            this.low = low;
            this.high = high;
            this.above = above;
            this.rows = rows;
        }
    }

    private static final class Planner {
        private final Connection connection;
        private final List<Branch> branches;
        private final TableCounts[] counts;
        private final boolean byTime;
        private final List<Item> items = new ArrayList<>();

        Planner(Connection connection, List<Branch> branches, TableCounts[] counts, boolean byTime) {
            this.connection = connection;
            this.branches = branches;
            this.counts = counts;
            this.byTime = byTime;
        }

        ColdPagePlan plan(long offset, int limit, boolean counted, boolean fromOldest) throws SQLException {
            collectItems();
            long total = -1;
            if (counted) {
                List<Item> unknown = new ArrayList<>();
                for (Item item : items) {
                    if (item.exact < 0) {
                        unknown.add(item);
                    }
                }
                countExactly(unknown);
                total = 0;
                for (Item item : items) {
                    total = total + item.exact;
                }
            }
            if (fromOldest) {
                // The same rows, ranked from the other end.
                long reversed = total - offset - limit;
                limit = (int) Math.min(limit, total - offset);
                offset = Math.max(0, reversed);
                if (limit <= 0) {
                    return counted(empty(), total);
                }
            }
            if (items.isEmpty()) {
                return counted(empty(), total);
            }
            return counted(plan(offset, limit), total);
        }

        private ColdPagePlan counted(ColdPagePlan plan, long total) {
            if (plan != null) {
                plan.total = total;
            }
            return plan;
        }

        private ColdPagePlan plan(long offset, int limit) throws SQLException {
            long end = offset + limit;
            long lowest = Long.MAX_VALUE;
            for (Item item : items) {
                lowest = Math.min(lowest, item.low);
            }
            long floor = lowest - 1;

            // Narrow the stretch holding both ends of the page by the counts alone, reading only
            // segments whose counts are unknown and matter.
            long[] stretch = settle(offset, end, floor);
            long low = stretch[0];
            long high = stretch[1];

            long above = 0;
            List<Item> crossing = new ArrayList<>();
            for (Item item : items) {
                if (item.low > high) {
                    above = above + item.exact;
                }
                else if (item.high > low) {
                    crossing.add(item);
                }
            }

            long bound = 0;
            for (Item item : crossing) {
                bound = bound + item.bound();
            }
            Position first;
            Position last;
            if (bound <= COLLECT_ROWS) {
                Gathered gathered = gather(crossing, low, high, true);
                long aboveHigh = above + gathered.above;
                long total = aboveHigh + gathered.size;
                if (offset >= total) {
                    return empty(); // the stretch reaches the lowest row, so this is everything
                }
                first = pick(gathered, offset - aboveHigh);
                last = end - 1 >= total ? null : pick(gathered, end - 1 - aboveHigh);
            }
            else {
                Sliced sliced = slice(crossing, low, high, true);
                long aboveHigh = above + sliced.above;
                long total = aboveHigh;
                for (long rows : sliced.slices) {
                    total = total + rows;
                }
                if (offset >= total) {
                    return empty();
                }
                first = locate(narrow(sliced, low, high, aboveHigh, offset), offset);
                last = end - 1 >= total ? null : locate(narrow(sliced, low, high, aboveHigh, end - 1), end - 1);
                if (first == null || (end - 1 < total && last == null)) {
                    return null; // a single second holds more rows than can be ranked in memory
                }
            }
            return windows(first, last);
        }

        /**
         * Lists every packed segment and every table's live rows that can hold a match.
         */
        private void collectItems() throws SQLException {
            for (int index = 0; index < branches.size(); index++) {
                TableCounts table = counts[index];
                for (ColdSegment segment : table.segments) {
                    long exact = table.exactRows(segment);
                    long most = exact >= 0 ? exact : table.mostRows(segment);
                    if (most <= 0) {
                        continue;
                    }
                    items.add(new Item(index, segment, byTime ? segment.minTime : segment.startRowId, byTime ? segment.maxTime : segment.endRowId, exact, most));
                }
                String key = byTime ? "time" : "rowid";
                String query = "SELECT COUNT(*), MIN(" + key + "), MAX(" + key + ") FROM " + ConfigHandler.prefix + branches.get(index).table + " WHERE (" + branches.get(index).clause + ")";
                try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
                    if (results.next() && results.getLong(1) > 0) {
                        long rows = results.getLong(1);
                        items.add(new Item(index, null, results.getLong(2), results.getLong(3), rows, rows));
                    }
                }
            }
        }

        /**
         * Finds a stretch, above its low end and up to and including its high end, that holds the
         * first row of the page and the last. Above the high end lie at most as many rows as come
         * before the page; above the low end lie at least as many as the page reaches down to. Both
         * are judged from what the counts say for certain, reading segments only where a count is
         * unknown and it matters, until the stretch is small enough to gather or nothing unknown
         * is left in it.
         *
         * @return the low and high ends
         */
        private long[] settle(long offset, long end, long floor) throws SQLException {
            while (true) {
                long[] stretch = bounds(offset, end, floor);
                List<Item> pending = new ArrayList<>();
                long crossingRows = 0;
                List<Item> unknownCrossing = new ArrayList<>();
                for (Item item : items) {
                    if (item.low > stretch[1]) {
                        if (item.exact < 0) {
                            pending.add(item); // wholly above the page: its exact count is needed
                        }
                    }
                    else if (item.high > stretch[0]) {
                        crossingRows = crossingRows + item.bound();
                        if (item.exact < 0) {
                            unknownCrossing.add(item);
                        }
                    }
                }
                if (pending.isEmpty()) {
                    if (crossingRows <= COLLECT_ROWS || unknownCrossing.isEmpty()) {
                        return stretch;
                    }
                    // Knowing these exactly can only narrow the stretch, which is worth a count
                    // when it is too large to gather.
                    pending = unknownCrossing;
                }
                pending.sort(Comparator.comparingLong((Item item) -> item.high).reversed());
                countExactly(pending.subList(0, Math.min(COUNT_BATCH, pending.size())));
            }
        }

        /**
         * @return the low and high ends of the stretch the counts known so far guarantee
         */
        private long[] bounds(long offset, long end, long floor) {
            // The low end: as high as possible while at least the page's end lies above it, by
            // counts known for certain.
            List<Item> byLow = new ArrayList<>(items);
            byLow.sort(Comparator.comparingLong((Item item) -> item.low).reversed());
            long low = floor;
            long certain = 0;
            for (int index = 0; index < byLow.size();) {
                long value = byLow.get(index).low;
                while (index < byLow.size() && byLow.get(index).low == value) {
                    certain = certain + Math.max(0, byLow.get(index).exact);
                    index++;
                }
                if (certain >= end) {
                    low = value - 1;
                    break;
                }
            }

            // The high end: as low as possible while no more rows than the offset can lie above it.
            List<Item> byHigh = new ArrayList<>(items);
            byHigh.sort(Comparator.comparingLong((Item item) -> item.high).reversed());
            long high = byHigh.get(0).high;
            long possible = 0;
            boolean exhausted = true;
            for (int index = 0; index < byHigh.size();) {
                long value = byHigh.get(index).high;
                if (possible > offset) {
                    exhausted = false;
                    break;
                }
                high = value;
                while (index < byHigh.size() && byHigh.get(index).high == value) {
                    possible = possible + byHigh.get(index).bound();
                    index++;
                }
            }
            if (exhausted && possible <= offset) {
                high = floor;
            }
            return new long[] { low, Math.max(low, high) };
        }

        /**
         * Settles the exact count of segments whose recorded counts cannot answer the query, from
         * their scalar frames.
         */
        private void countExactly(List<Item> unknown) throws SQLException {
            Map<Integer, List<Item>> byBranch = new HashMap<>();
            for (Item item : unknown) {
                byBranch.computeIfAbsent(item.branch, ignored -> new ArrayList<>()).add(item);
            }
            for (Map.Entry<Integer, List<Item>> entry : byBranch.entrySet()) {
                List<ColdSegment> segments = new ArrayList<>();
                for (Item item : entry.getValue()) {
                    segments.add(item.segment);
                }
                long[] exact = SQLiteColdIndex.countEach(connection, segments, counts[entry.getKey()].filter);
                for (int index = 0; index < exact.length; index++) {
                    entry.getValue().get(index).exact = exact[index];
                }
            }
        }

        /**
         * The test that keeps the rows whose key lies above a low end, and, unless the rows above
         * the high end are being counted too, no higher than it.
         */
        private ColdSegmentCodec.RowFilter keyRange(TableCounts table, long low, long high, boolean countAbove) {
            int timeColumn = table.layout.timeColumn;
            ColdSegmentCodec.RowFilter range = new ColdSegmentCodec.RowFilter() {
                @Override
                public boolean accept(long[] columns, boolean[] present) {
                    return true;
                }

                @Override
                public boolean acceptRow(long rowId, long[] columns, boolean[] present) {
                    if (!byTime) {
                        return rowId > low && (countAbove || rowId <= high);
                    }
                    return present[timeColumn] && columns[timeColumn] > low && (countAbove || columns[timeColumn] <= high);
                }

                @Override
                public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                    if (!byTime || !known[timeColumn]) {
                        return true;
                    }
                    return maximum[timeColumn] > low && (countAbove || minimum[timeColumn] <= high);
                }
            };
            return SQLiteColdIndex.both(table.filter, range);
        }

        /** Receives the key of each matching row a scan finds in a stretch. */
        private interface Visitor {
            void row(long time, long rowId, int source);
        }

        /**
         * Visits every matching row of the given items whose key lies in the stretch, and returns
         * how many lie above it when asked to count them.
         */
        private long scan(List<Item> scanned, long low, long high, boolean countAbove, Visitor visitor, Sliced sliced) throws SQLException {
            long[] above = new long[1];
            Map<Integer, List<ColdSegment>> segmentsByBranch = new HashMap<>();
            List<Item> live = new ArrayList<>();
            for (Item item : scanned) {
                if (item.segment == null) {
                    live.add(item);
                }
                else {
                    segmentsByBranch.computeIfAbsent(item.branch, ignored -> new ArrayList<>()).add(item.segment);
                }
            }
            for (Map.Entry<Integer, List<ColdSegment>> entry : segmentsByBranch.entrySet()) {
                TableCounts table = counts[entry.getKey()];
                int source = branches.get(entry.getKey()).source;
                SQLiteColdIndex.scanKeys(connection, entry.getValue(), keyRange(table, low, high, countAbove), byTime ? table.layout.timeColumn : -1, keys -> {
                    for (int row = 0; row < keys.size(); row++) {
                        long key = byTime ? keys.time(row) : keys.rowId(row);
                        if (key > high) {
                            above[0]++;
                        }
                        else {
                            visitor.row(keys.time(row), keys.rowId(row), source);
                        }
                    }
                });
            }
            for (Item item : live) {
                Branch branch = branches.get(item.branch);
                String key = byTime ? "time" : "rowid";
                String table = ConfigHandler.prefix + branch.table + " WHERE (" + branch.clause + ")";
                try (Statement statement = connection.createStatement()) {
                    if (countAbove) {
                        try (ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + table + " AND " + key + " > " + high)) {
                            above[0] = above[0] + (results.next() ? results.getLong(1) : 0);
                        }
                    }
                    String range = " AND " + key + " > " + low + " AND " + key + " <= " + high;
                    if (sliced != null) {
                        // Counted per slice by the database, rather than row by row here.
                        String query = "SELECT (" + key + " - " + low + " - 1) / " + sliced.width + ", COUNT(*) FROM " + table + range + " GROUP BY 1";
                        try (ResultSet results = statement.executeQuery(query)) {
                            while (results.next()) {
                                sliced.slices[(int) results.getLong(1)] += results.getLong(2);
                            }
                        }
                    }
                    else {
                        try (ResultSet results = statement.executeQuery("SELECT time, rowid FROM " + table + range)) {
                            while (results.next()) {
                                visitor.row(results.getLong(1), results.getLong(2), branch.source);
                            }
                        }
                    }
                }
            }
            return above[0];
        }

        private List<Item> crossing(long low, long high) {
            List<Item> crossing = new ArrayList<>();
            for (Item item : items) {
                if (item.low <= high && item.high > low) {
                    crossing.add(item);
                }
            }
            return crossing;
        }

        /**
         * Gathers the key of every matching row in the stretch.
         */
        private Gathered gather(List<Item> scanned, long low, long high, boolean countAbove) throws SQLException {
            Gathered gathered = new Gathered();
            gathered.above = scan(scanned, low, high, countAbove, gathered::add, null);
            return gathered;
        }

        /**
         * Counts the matching rows of the stretch slice by slice.
         */
        private Sliced slice(List<Item> scanned, long low, long high, boolean countAbove) throws SQLException {
            long span = high - low;
            long width = Math.max(1, (span + SLICES - 1) / SLICES);
            int count = (int) ((span + width - 1) / width);
            Sliced sliced = new Sliced(width, Math.max(1, count));
            sliced.above = scan(scanned, low, high, countAbove, (time, rowId, source) -> {
                long key = byTime ? time : rowId;
                sliced.slices[(int) ((key - low - 1) / width)]++;
            }, sliced);
            return sliced;
        }

        /**
         * @return the slice of a counted stretch that holds the row at a rank, with exactly how many
         *         rows rank above it
         */
        private Stretch narrow(Sliced sliced, long low, long high, long aboveHigh, long rank) {
            long above = aboveHigh;
            for (int index = sliced.slices.length - 1; index >= 0; index--) {
                long rows = sliced.slices[index];
                if (rank < above + rows) {
                    long sliceLow = low + (index * sliced.width);
                    long sliceHigh = Math.min(high, sliceLow + sliced.width);
                    return new Stretch(sliceLow, sliceHigh, above, rows);
                }
                above = above + rows;
            }
            return null;
        }

        /**
         * Finds the row at a rank inside a stretch known to hold it, narrowing the stretch by
         * counting slices of it until its rows can be gathered.
         *
         * @return where the row stands, or null when a single second holds too many rows to rank
         */
        private Position locate(Stretch stretch, long rank) throws SQLException {
            Stretch current = stretch;
            while (current != null) {
                List<Item> crossing = crossing(current.low, current.high);
                if (current.rows <= COLLECT_ROWS) {
                    Gathered gathered = gather(crossing, current.low, current.high, false);
                    return pick(gathered, rank - current.above);
                }
                if (current.high - current.low <= 1) {
                    return null;
                }
                Sliced sliced = slice(crossing, current.low, current.high, false);
                current = narrow(sliced, current.low, current.high, current.above, rank);
            }
            return null;
        }

        /**
         * Picks the row at a rank among gathered rows, counted from the highest.
         */
        private Position pick(Gathered gathered, long rank) {
            int size = gathered.size;
            if (!byTime) {
                long[] rowIds = Arrays.copyOf(gathered.rowIds, size);
                Arrays.sort(rowIds);
                return new Position(0, 0, rowIds[size - 1 - (int) rank]);
            }
            long[] times = Arrays.copyOf(gathered.times, size);
            Arrays.sort(times);
            long time = times[size - 1 - (int) rank];
            int higher = size - upperBound(times, time);
            // Rows sharing the time are ranked by table, then by row id, both highest first.
            List<long[]> tied = new ArrayList<>();
            for (int row = 0; row < size; row++) {
                if (gathered.times[row] == time) {
                    tied.add(new long[] { gathered.sources[row], gathered.rowIds[row] });
                }
            }
            tied.sort(Comparator.comparingLong((long[] row) -> row[0]).thenComparingLong(row -> row[1]).reversed());
            long[] chosen = tied.get((int) (rank - higher));
            return new Position(time, (int) chosen[0], chosen[1]);
        }

        private static int upperBound(long[] sorted, long value) {
            int low = 0;
            int high = sorted.length;
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (sorted[middle] <= value) {
                    low = middle + 1;
                }
                else {
                    high = middle;
                }
            }
            return low;
        }

        /**
         * Confines each table to the stretch between the first row of the page and the last, both
         * included. Where tables tie on time, the table ranked higher comes first, so at the first
         * row's time a higher ranked table has nothing left on the page, and at the last row's time
         * a lower ranked one has nothing yet.
         */
        private ColdPagePlan windows(Position first, Position last) {
            Map<String, KeyWindow> windows = new HashMap<>();
            for (Branch branch : branches) {
                if (!byTime) {
                    windows.put(branch.table, KeyWindow.ofRowIds(last == null ? Long.MIN_VALUE : last.rowId, first.rowId));
                    continue;
                }
                long highTime = first.time;
                long highRowId = Long.MAX_VALUE;
                if (branch.source == first.source) {
                    highRowId = first.rowId;
                }
                else if (branch.source > first.source) {
                    highTime = first.time - 1;
                }
                long lowTime = Long.MIN_VALUE;
                long lowRowId = Long.MIN_VALUE;
                if (last != null) {
                    lowTime = last.time;
                    if (branch.source == last.source) {
                        lowRowId = last.rowId;
                    }
                    else if (branch.source < last.source) {
                        lowTime = last.time + 1;
                    }
                }
                windows.put(branch.table, KeyWindow.ofTime(lowTime, lowRowId, highTime, highRowId));
            }
            return new ColdPagePlan(windows, false);
        }

        private ColdPagePlan empty() {
            Map<String, KeyWindow> windows = new HashMap<>();
            for (Branch branch : branches) {
                windows.put(branch.table, KeyWindow.ofRowIds(Long.MAX_VALUE, Long.MIN_VALUE));
            }
            return new ColdPagePlan(Collections.unmodifiableMap(windows), true);
        }
    }
}
