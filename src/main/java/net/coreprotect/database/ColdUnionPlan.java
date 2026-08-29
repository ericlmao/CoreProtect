package net.coreprotect.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Works out where a page of a merged lookup starts, without reading the rows it starts after.
 *
 * <p>
 * A lookup across several tables merges them by time, so a page cannot be found by skipping rows in
 * one table alone. Instead the whole query is limited to rows at or below a timestamp, and its
 * offset is reduced by exactly how many matching rows lie above that timestamp. Time is the first
 * thing the merge sorts by, so those rows are precisely the ones the page was going to skip past.
 * </p>
 *
 * <p>
 * The count above the cut is exact rather than estimated. Segments that sit entirely above it
 * contribute the row counts recorded when they were sealed; only the one segment per table that
 * straddles the cut has to be read, and live rows are counted by the database itself.
 * </p>
 */
public final class ColdUnionPlan {

    /** Pages shallower than this are already cheap, so planning them is not worth the counting. */
    static final int MINIMUM_OFFSET = 1000;

    private static final ColdUnionPlan NONE = new ColdUnionPlan(0, 0);

    private final long cutTime;
    private final long skippedRows;

    private ColdUnionPlan(long cutTime, long skippedRows) {
        this.cutTime = cutTime;
        this.skippedRows = skippedRows;
    }

    /**
     * @return a plan that changes nothing, for lookups that cannot be planned
     */
    public static ColdUnionPlan none() {
        return NONE;
    }

    /**
     * @return the timestamp the query should be limited to, or 0 when there is no cut
     */
    public long getCutTime() {
        return cutTime;
    }

    /**
     * @return how many matching rows lie above the cut, which the query's offset is reduced by
     */
    public long getSkippedRows() {
        return skippedRows;
    }

    /**
     * @return true if this plan changes the query
     */
    public boolean isPlanned() {
        return cutTime > 0;
    }

    /**
     * Plans a page of a merged lookup.
     *
     * @param connection
     *            an open connection
     * @param tables
     *            the unprefixed tables the lookup reads, busiest first
     * @param users
     *            a comma separated list of user ids, or an empty string
     * @param types
     *            a comma separated list of block type ids, or an empty string
     * @param actions
     *            a comma separated list of action ids, or null
     * @param excludeUsers
     *            a comma separated list of user ids the lookup leaves out, or null
     * @param excludeTypes
     *            a comma separated list of block type ids the lookup leaves out, or null
     * @param excludeActions
     *            action ids the lookup leaves out, keyed by the table they are left out of
     * @param startTime
     *            the earliest timestamp the lookup wants, or 0
     * @param endTime
     *            the latest timestamp the lookup wants, or 0
     * @param offset
     *            the offset of the page being shown
     * @return the plan, which may be {@link #none()}
     * @throws SQLException
     *             if the database cannot be read
     */
    public static ColdUnionPlan forPage(Connection connection, String[] tables, String users, String types, String actions, String excludeUsers, String excludeTypes, Map<String, String> excludeActions, long startTime, long endTime, long offset) throws SQLException {
        if (offset < MINIMUM_OFFSET || tables.length == 0 || !SQLiteColdIndex.hasSegments()) {
            return NONE;
        }

        // Segment starts are the places the history can be divided. The further back the cut, the
        // more rows lie above it, so the deepest usable cut can be found by halving the choices
        // rather than by reading anything: the best cut is the last one whose rows above still fit
        // inside the offset the page is skipping anyway.
        long[] candidates = SQLiteColdIndex.cutCandidates(connection, tables[0]);
        if (candidates.length == 0) {
            return NONE;
        }

        int low = 0;
        int high = candidates.length - 1;
        long bestCut = 0;
        long bestAbove = 0;

        while (low <= high) {
            int middle = (low + high) >>> 1;
            long cut = candidates[middle];
            if (cut <= startTime || (endTime > 0 && cut >= endTime)) {
                low = middle + 1;
                continue;
            }

            long above = 0;
            boolean exact = true;
            for (String table : tables) {
                SQLiteColdIndex.Exclusions excluded = SQLiteColdIndex.exclusionsOf(excludeUsers, excludeTypes,
                        excludeActions == null ? null : excludeActions.get(table));
                long counted = SQLiteColdIndex.countAbove(connection, table, cut, users, types, actions, excluded, startTime, endTime);
                if (counted < 0) {
                    exact = false;
                    break;
                }
                above = above + counted;
            }

            if (!exact) {
                return NONE;
            }

            if (above <= offset) {
                bestCut = cut;
                bestAbove = above;
                low = middle + 1; // this cut fits, so try an older one
            }
            else {
                high = middle - 1; // too far back, the page would lose rows it has to show
            }
        }

        return bestCut > 0 && bestAbove > 0 ? new ColdUnionPlan(bestCut, bestAbove) : NONE;
    }
}
