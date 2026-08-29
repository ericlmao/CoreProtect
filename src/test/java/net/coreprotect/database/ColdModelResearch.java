package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import net.coreprotect.config.ConfigHandler;

/**
 * Prototypes alternative cold data models against a real database and measures what each would buy:
 * per-segment statistics for instant counts, newest-first materialization that stops when a page is
 * full, and user-clustered row order inside segments.
 *
 * <p>
 * Run with: {@code mvn -DskipTests=false -Dcoreprotect.research=/path/to/database.db -Dtest=ColdModelResearch test}
 * </p>
 */
class ColdModelResearch {

    @Test
    void measuresAlternativeColdModels() throws Exception {
        String path = System.getProperty("coreprotect.research");
        assumeTrue(path != null, "research runs only when a database is given");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path)) {
            SQLiteColdIndex.reload(connection);
            List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null);
            System.out.println("segments=" + all.size());

            SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "block");
            int userColumn = -1;
            int timeColumn = -1;
            for (int index = 0; index < layout.columns.length; index++) {
                if (layout.columns[index].equalsIgnoreCase("user")) {
                    userColumn = index;
                }
                if (layout.columns[index].equalsIgnoreCase("time")) {
                    timeColumn = index;
                }
            }

            // Model 1: per-segment user statistics, collected once at roll-up time.
            long statsStart = System.nanoTime();
            Map<Long, Map<Long, Integer>> statistics = new HashMap<>(); // segment id -> user -> rows
            long topUser = 0;
            Map<Long, Long> totals = new HashMap<>();
            for (SQLiteColdIndex.ColdSegment segment : all) {
                ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
                Map<Long, Integer> perUser = new HashMap<>();
                for (int row = 0; row < rows.size(); row++) {
                    Object user = rows.getValues(row)[userColumn];
                    if (user != null) {
                        long id = ((Number) user).longValue();
                        perUser.merge(id, 1, Integer::sum);
                        totals.merge(id, 1L, Long::sum);
                    }
                }
                statistics.put(segment.getId(), perUser);
            }
            for (Map.Entry<Long, Long> entry : totals.entrySet()) {
                if (topUser == 0 || entry.getValue() > totals.get(topUser)) {
                    topUser = entry.getKey();
                }
            }
            System.out.println("stats_build_ms=" + (System.nanoTime() - statsStart) / 1_000_000 + " (one-off, would run during roll-up)");

            // With statistics, a count is a sum over metadata: no decode at all.
            long countStart = System.nanoTime();
            long counted = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                counted += statistics.get(segment.getId()).getOrDefault(topUser, 0);
            }
            System.out.println("count_from_stats_ms=" + (System.nanoTime() - countStart) / 1_000_000 + " matches=" + counted);

            // Model 2: newest-first materialization that stops once a page is full.
            final long wantedUser = topUser;
            final int uc = userColumn;
            for (int pageSize : new int[] { 100, 1000 }) {
                long pageStart = System.nanoTime();
                List<SQLiteColdIndex.ColdSegment> newestFirst = new ArrayList<>(all);
                newestFirst.sort(Comparator.comparingLong(SQLiteColdIndex.ColdSegment::getEndRowId).reversed());
                long collected = 0;
                int segmentsRead = 0;
                for (SQLiteColdIndex.ColdSegment segment : newestFirst) {
                    if (statistics.get(segment.getId()).getOrDefault(wantedUser, 0) == 0) {
                        continue; // statistics prune the segment without opening it
                    }
                    ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment,
                            (columns, present) -> present[uc] && columns[uc] == wantedUser);
                    collected += rows.size();
                    segmentsRead++;
                    if (collected >= pageSize) {
                        break;
                    }
                }
                System.out.println("page_" + pageSize + "_ms=" + (System.nanoTime() - pageStart) / 1_000_000
                        + " rows=" + collected + " segments_read=" + segmentsRead + "/" + all.size());
            }

            // Model 3: user-clustered row order. Re-encode a sample of segments sorted by
            // (user, time) and compare the compressed sizes and the per-user segment spread.
            long originalScalars = 0;
            long originalPayload = 0;
            long clusteredScalars = 0;
            long clusteredPayload = 0;
            int sample = Math.min(20, all.size());
            for (int index = 0; index < sample; index++) {
                SQLiteColdIndex.ColdSegment segment = all.get(index);
                ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);

                List<Object[]> asRead = new ArrayList<>(rows.size());
                long[] rowIds = new long[rows.size()];
                Integer[] order = new Integer[rows.size()];
                for (int row = 0; row < rows.size(); row++) {
                    asRead.add(rows.getValues(row));
                    rowIds[row] = rows.getRowId(row);
                    order[row] = row;
                }

                ColdSegmentCodec.Frames original = ColdSegmentCodec.encode(layout.types, rowIds, asRead);
                originalScalars += SegmentDictionary.compress(original.getScalars(), 0, connection).length;
                byte[] payload = original.getPayload();
                originalPayload += payload.length == 0 ? 0 : SegmentDictionary.compress(payload, 0, connection).length;

                final int tc = timeColumn;
                java.util.Arrays.sort(order, (a, b) -> {
                    long userA = number(asRead.get(a)[uc]);
                    long userB = number(asRead.get(b)[uc]);
                    if (userA != userB) {
                        return Long.compare(userA, userB);
                    }
                    return Long.compare(number(asRead.get(a)[tc]), number(asRead.get(b)[tc]));
                });

                List<Object[]> clustered = new ArrayList<>(rows.size());
                long[] clusteredIds = new long[rows.size()];
                for (int row = 0; row < rows.size(); row++) {
                    clustered.add(asRead.get(order[row]));
                    clusteredIds[row] = rowIds[order[row]];
                }
                // Row ids are no longer sorted, so they cost more to store; that loss is the point
                // of measuring this.
                ColdSegmentCodec.Frames sortedFrames = encodeUnsorted(layout.types, clusteredIds, clustered);
                clusteredScalars += SegmentDictionary.compress(sortedFrames.getScalars(), 0, connection).length;
                byte[] clusteredPayloadFrame = sortedFrames.getPayload();
                clusteredPayload += clusteredPayloadFrame.length == 0 ? 0 : SegmentDictionary.compress(clusteredPayloadFrame, 0, connection).length;
            }

            System.out.println("rowid_order_compressed_kb=" + (originalScalars + originalPayload) / 1024);
            System.out.println("user_clustered_compressed_kb=" + (clusteredScalars + clusteredPayload) / 1024);
            System.out.printf("clustering_size_ratio=%.2f%n", (clusteredScalars + clusteredPayload) / (double) (originalScalars + originalPayload));
        }
    }

    private static long number(Object value) {
        return value == null ? Long.MIN_VALUE : ((Number) value).longValue();
    }

    /** Encodes rows whose ids are not ascending, which the production encoder refuses. */
    private static ColdSegmentCodec.Frames encodeUnsorted(int[] types, long[] rowIds, List<Object[]> rows) {
        // The production format stores row ids as deltas, which handles any order; the encoder just
        // never sees unsorted input in production. Reuse it directly.
        return ColdSegmentCodec.encode(types, rowIds, rows);
    }
}
