package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;

import net.coreprotect.config.ConfigHandler;

/**
 * Times the cold read path against a real server database, using the same entry points a lookup
 * uses.
 *
 * <p>
 * Run with: {@code mvn -DskipTests=false -Dcoreprotect.live=/path/to/database.db -Dtest=LiveDatabaseBenchmark test}
 * </p>
 */
class LiveDatabaseBenchmark {

    @Test
    void timesLookupsAgainstARealDatabase() throws Exception {
        String path = System.getProperty("coreprotect.live");
        assumeTrue(path != null, "benchmark runs only when a database is given");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path)) {
            // The plugin runs this at startup; it adds any columns a newer build expects.
            try (Statement statement = connection.createStatement()) {
                SQLiteSchema.createTables("co_", statement);
            }
            SQLiteColdIndex.reload(connection);
            List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null);
            System.out.println("segments=" + all.size() + " cold_rows=" + one(connection, "SELECT COALESCE(SUM(row_count),0) FROM co_segment"));
            System.out.println("live_rows=" + one(connection, "SELECT COUNT(*) FROM co_block"));

            long topUser = one(connection, "SELECT user FROM co_block GROUP BY user ORDER BY COUNT(*) DESC LIMIT 1");
            long topType = one(connection, "SELECT type FROM co_block GROUP BY type ORDER BY COUNT(*) DESC LIMIT 1");

            // Split the cost: decode with the filter, versus the whole materialize step.
            long decodeStart = System.nanoTime();
            long kept = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment,
                        (columns, present) -> present[1] && columns[1] == topUser);
                kept += rows.size();
            }
            System.out.println("decode_all_segments_ms=" + (System.nanoTime() - decodeStart) / 1_000_000 + " kept=" + kept);

            long rawStart = System.nanoTime();
            long rawRows = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                rawRows += SQLiteColdIndex.readRows(connection, segment, (columns, present) -> false).size();
            }
            System.out.println("decode_reject_all_ms=" + (System.nanoTime() - rawStart) / 1_000_000 + " built=" + rawRows);

            // The count query the lookup command runs before showing a page.
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                long countStart = System.nanoTime();
                SQLiteColdIndex.setLookupFilters(Long.toString(topUser), "");
                SQLiteColdIndex.setCounting(true);
                String countSource = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
                long cold = SQLiteColdIndex.countedRows("block");
                long hot = one(connection, "SELECT COUNT(*) FROM " + countSource + " WHERE user IN(" + topUser + ")");
                System.out.println("count_query_ms=" + (System.nanoTime() - countStart) / 1_000_000 + " total=" + (hot + cold) + " (hot=" + hot + " cold=" + cold + ")");
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }

            // The case that hurts: a lookup whose result is smaller than one page.
            long rareUser = one(connection, "SELECT user FROM co_block GROUP BY user ORDER BY COUNT(*) ASC LIMIT 1");
            page(connection, "small result (before)", Long.toString(rareUser), 10);

            long backfillStart = System.nanoTime();
            long updated = ColdRollupTask.backfillStatistics(connection, () -> {
            });
            System.out.println("backfill_ms=" + (System.nanoTime() - backfillStart) / 1_000_000 + " segments=" + updated);

            page(connection, "small result (after)", Long.toString(rareUser), 10);
            for (int offset : new int[] { 10000, 50000, 60000 }) {
                plannedPage(connection, topUser, offset, 4);
            }
            for (int offset : new int[] { 20000, 66000, 8000000 }) {
                plannedUnionPage(connection, topUser, offset, 4);
            }
            page(connection, "user page (10)", Long.toString(topUser), 10);
            page(connection, "user page (100)", Long.toString(topUser), 100);
            run(connection, "no filters", "", "", 0, null);
            run(connection, "radius (populated)", "", "", 1, new Integer[] { 0, -100, 100, 0, 0, -100, 100 });
            run(connection, "user lookup", Long.toString(topUser), "", 0, null);
            run(connection, "user lookup (repeat)", Long.toString(topUser), "", 0, null);
            run(connection, "absent user", "999999", "", 0, null);
            run(connection, "block type lookup", "", Long.toString(topType), 0, null);
            run(connection, "radius r:50", "", "", 1, new Integer[] { 0, 400, 500, 0, 0, 400, 500 });
            run(connection, "user + radius", Long.toString(topUser), "", 1, new Integer[] { 0, 400, 500, 0, 0, 400, 500 });
        }
    }

    /**
     * Compares a deep page read the old way against the same page planned, checking both the timing
     * and that the rows are identical.
     */
    private void plannedPage(Connection connection, long user, int offset, int pageSize) throws SQLException {
        long unplannedStart = System.nanoTime();
        List<String> unplanned = page(connection, new String[] { "block" }, false, user, offset, pageSize, null);
        long unplannedMs = (System.nanoTime() - unplannedStart) / 1_000_000;
        long unplannedRows = lastRows;

        long plannedStart = System.nanoTime();
        ColdPagePlan plan = ColdPagePlan.plan(connection, List.of(new ColdPagePlan.Branch("block", 0, " user = " + user, 0, null)), false, "user", offset, pageSize);
        List<String> planned = page(connection, new String[] { "block" }, false, user, offset, pageSize, plan);
        long plannedMs = (System.nanoTime() - plannedStart) / 1_000_000;

        System.out.printf("planned page offset=%-7d unplanned %5d ms (%d rows) | planned %5d ms (%d rows) | same rows: %s%n",
                offset, unplannedMs, unplannedRows, plannedMs, lastRows, unplanned.equals(planned));
    }

    /**
     * Compares a deep page of a merged lookup read the old way against the planned one.
     */
    private void plannedUnionPage(Connection connection, long user, int offset, int pageSize) throws SQLException {
        String[] tables = { "block", "container", "item" };

        long unplannedStart = System.nanoTime();
        List<String> unplanned = page(connection, tables, true, user, offset, pageSize, null);
        long unplannedMs = (System.nanoTime() - unplannedStart) / 1_000_000;
        long unplannedRows = lastRows;

        long plannedStart = System.nanoTime();
        List<ColdPagePlan.Branch> branches = new java.util.ArrayList<>();
        for (int index = 0; index < tables.length; index++) {
            branches.add(new ColdPagePlan.Branch(tables[index], index, " user = " + user, 0, null));
        }
        ColdPagePlan plan = ColdPagePlan.plan(connection, branches, true, "user", offset, pageSize);
        List<String> planned = page(connection, tables, true, user, offset, pageSize, plan);
        long plannedMs = (System.nanoTime() - plannedStart) / 1_000_000;

        System.out.printf("union page offset=%-7d unplanned %5d ms (%d rows) | planned %5d ms (%d rows) | same rows: %s%n",
                offset, unplannedMs, unplannedRows, plannedMs, lastRows, unplanned.equals(planned));
    }

    private long lastRows;

    private List<String> page(Connection connection, String[] tables, boolean byTime, long user, int offset, int pageSize, ColdPagePlan plan) throws SQLException {
        List<String> page = new java.util.ArrayList<>();
        lastRows = 0;
        long shifted = plan == null ? offset : 0;

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters(Long.toString(user), "");
            SQLiteColdIndex.setRowBudget(shifted + pageSize);
            SQLiteColdIndex.setPageOrder(byTime ? "time DESC, id DESC" : "rowid DESC");

            StringBuilder union = new StringBuilder();
            for (int index = 0; index < tables.length; index++) {
                SQLiteColdIndex.setPredicateFilters(" user = " + user, "user");
                if (plan != null) {
                    SQLiteColdIndex.setPageWindow(tables[index], plan.window(tables[index]));
                }
                String source = SQLiteColdIndex.sourceExpression(connection, tables[index], 0, null);
                if (source.contains("temp.cp_cold_")) {
                    lastRows = lastRows + one(connection, "SELECT COUNT(*) FROM temp.cp_cold_" + tables[index]);
                }
                if (index > 0) {
                    union.append(" UNION ALL ");
                }
                union.append("SELECT * FROM (SELECT '").append(index).append("' tbl, rowid id, time FROM ").append(source)
                        .append(" WHERE user = ").append(user)
                        .append(" ORDER BY ").append(byTime ? "time DESC, id DESC" : "id DESC").append(" LIMIT ").append(shifted + pageSize).append(')');
            }

            String query = "SELECT tbl, id FROM (" + union + ") ORDER BY " + (byTime ? "time DESC, tbl DESC, id DESC" : "id DESC") + " LIMIT " + pageSize + " OFFSET " + shifted;
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
                while (results.next()) {
                    page.add(results.getString(1) + ":" + results.getLong(2));
                }
            }
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        return page;
    }

    private void page(Connection connection, String label, String users, int pageSize) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            long start = System.nanoTime();
            SQLiteColdIndex.setLookupFilters(users, "");
            SQLiteColdIndex.setRowBudget(pageSize);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            long rows = 0;
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT rowid FROM " + source + " WHERE user IN(" + users + ") ORDER BY rowid DESC LIMIT " + pageSize)) {
                while (results.next()) {
                    rows++;
                }
            }
            long decoded = source.startsWith("(") ? one(connection, "SELECT COUNT(*) FROM temp.cp_cold_block") : 0;
            System.out.printf("%-22s %7d ms  page_rows=%-6d decoded_rows=%-9d%n", label, (System.nanoTime() - start) / 1_000_000, rows, decoded);
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private void run(Connection connection, String label, String users, String types, int worldId, Integer[] bounds) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            long start = System.nanoTime();
            SQLiteColdIndex.setLookupFilters(users, types);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", worldId, bounds);

            StringBuilder query = new StringBuilder("SELECT COUNT(*) FROM " + source + " WHERE 1");
            if (!users.isEmpty()) {
                query.append(" AND user IN(").append(users).append(')');
            }
            if (!types.isEmpty()) {
                query.append(" AND type IN(").append(types).append(')');
            }
            if (bounds != null) {
                query.append(" AND wid = ").append(worldId)
                        .append(" AND x >= ").append(bounds[1]).append(" AND x <= ").append(bounds[2])
                        .append(" AND z >= ").append(bounds[5]).append(" AND z <= ").append(bounds[6]);
            }

            long matches;
            long materialized = 0;
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query.toString())) {
                results.next();
                matches = results.getLong(1);
            }
            if (source.startsWith("(")) {
                materialized = one(connection, "SELECT COUNT(*) FROM temp.cp_cold_block");
            }

            System.out.printf("%-22s %7d ms  matches=%-9d decoded_rows=%-9d source=%s%n",
                    label, (System.nanoTime() - start) / 1_000_000, matches, materialized, source.startsWith("(") ? "cold+live" : "live only");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private static long one(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
