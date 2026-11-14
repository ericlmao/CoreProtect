package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.github.luben.zstd.Zstd;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;

/**
 * Re-encodes segments taken from a production database in the current format, and measures what
 * that does to their size and to how long a lookup about one place takes to read them.
 *
 * <p>
 * The database is opened read only and immutable, so a copy taken from a running server works
 * even when its live tables are torn, and nothing is ever written to it. Run with:
 * {@code mvn -DskipTests=false -Dcoreprotect.prod=/path/to/database.db -Dtest=ProductionSegmentBenchmark test}
 * </p>
 */
class ProductionSegmentBenchmark {

    @Test
    void reencodesProductionSegments() throws Exception {
        String path = System.getProperty("coreprotect.prod");
        assumeTrue(path != null && !path.isEmpty(), "benchmark runs only when given a database");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        Config.getGlobal().BLOB_COMPRESSION = true;
        Config.getGlobal().BLOB_COMPRESSION_LEVEL = 19;
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        int sample = Integer.getInteger("coreprotect.prod.segments", 40);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:file:" + path + "?immutable=1")) {
            SQLiteColdIndex.reload(connection);
            for (String table : new String[] { "block", "container", "item", "entity_container", "chat", "command" }) {
                measure(connection, table, sample);
            }
        }
    }

    /**
     * Counts, for lookups about real places, how many block segments the chunk filter opens and how
     * many the exact rows per chunk open, and what holding those counts for every segment costs.
     * Every segment's scalar frame is read once to count its chunks, as the backfill does. Run with:
     * {@code mvn -DskipTests=false -Dcoreprotect.prod=/path/to/database.db -Dtest=ProductionSegmentBenchmark test}
     */
    @Test
    void comparesChunkFiltersWithChunkCounts() throws Exception {
        String path = System.getProperty("coreprotect.prod");
        assumeTrue(path != null && !path.isEmpty(), "benchmark runs only when given a database");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:file:" + path + "?immutable=1")) {
            SQLiteColdIndex.reload(connection);
            List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null);
            SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "block");
            int[] location = layout.chunkColumns();
            List<SegmentChunkCounts> counts = new ArrayList<>();
            long heap = 0;
            long chunks = 0;
            long stored = 0;
            long filterBytes = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                ColdSegmentCodec.ChunkTally tally = new ColdSegmentCodec.ChunkTally(Integer.MAX_VALUE);
                SQLiteColdIndex.scanRows(connection, segment, (columns, present) -> {
                    tally.add(present[location[0]] && present[location[1]] && present[location[2]], columns[location[0]], columns[location[1]], columns[location[2]]);
                    return false;
                });
                byte[] encoded = ColdSegmentCodec.encodeChunkCounts(tally.toList());
                SegmentChunkCounts segmentCounts = SegmentChunkCounts.decode(encoded);
                counts.add(segmentCounts);
                heap += segmentCounts.heapBytes();
                chunks += segmentCounts.distinctChunks();
                stored += encoded.length;
                filterBytes += segment.chunkFilter == null ? 0 : (segment.chunkFilter.isExact() ? 0 : SegmentFilter.CHUNK_BYTES);
            }

            Random random = new Random(29);
            long byFilter = 0;
            long byCounts = 0;
            int lookups = 100;
            for (int pick = 0; pick < lookups; pick++) {
                ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, all.get(random.nextInt(all.size())));
                Object[] row = rows.getValues(random.nextInt(rows.size()));
                int world = ((Number) row[layout.worldColumn]).intValue();
                int x = ((Number) row[layout.xColumn]).intValue();
                int z = ((Number) row[layout.zColumn]).intValue();
                long key = SegmentFilter.chunkKey(world, SegmentFilter.chunkOf(x), SegmentFilter.chunkOf(z));
                for (int index = 0; index < all.size(); index++) {
                    SegmentMembership filter = all.get(index).chunkFilter;
                    if (filter == null || filter.mightContain(key)) {
                        byFilter++;
                    }
                    if (counts.get(index).mayHoldAny(world, SegmentFilter.chunkOf(x), SegmentFilter.chunkOf(x), SegmentFilter.chunkOf(z), SegmentFilter.chunkOf(z))) {
                        byCounts++;
                    }
                }
            }
            System.out.println("block segments " + all.size() + ": " + chunks / all.size() + " chunks each; counts " + stored / all.size() + " bytes stored and " + heap / all.size()
                    + " bytes held per segment, replacing " + filterBytes / all.size() + " bytes of Bloom filter held per segment");
            System.out.println("place lookups open " + String.format("%.1f", byFilter / (double) lookups) + " segments by chunk filter, " + String.format("%.1f", byCounts / (double) lookups)
                    + " by chunk counts, of " + all.size());
        }
    }

    /**
     * Rewrites every segment of a writable copy of a production database's segments, and requires
     * lookups about real places to find exactly the same rows afterwards. Run with:
     * {@code mvn -DskipTests=false -Dcoreprotect.prod.repack=/path/to/copy.db -Dtest=ProductionSegmentBenchmark test}
     */
    @Test
    void repacksACopyOfProductionSegments() throws Exception {
        String path = System.getProperty("coreprotect.prod.repack");
        assumeTrue(path != null && !path.isEmpty(), "benchmark runs only when given a copy to rewrite");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        Config.getGlobal().BLOB_COMPRESSION = true;
        Config.getGlobal().BLOB_COMPRESSION_LEVEL = 19;
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path)) {
            SQLiteColdIndex.reload(connection);
            List<long[]> spots = new ArrayList<>();
            Random random = new Random(17);
            for (String table : new String[] { "block", "container" }) {
                List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, table, 0, null, 0, 0, null);
                SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, table);
                for (int pick = 0; pick < 100; pick++) {
                    ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, all.get(random.nextInt(all.size())));
                    Object[] row = rows.getValues(random.nextInt(rows.size()));
                    spots.add(new long[] { SQLiteColdIndex.tableId(table), ((Number) row[layout.worldColumn]).longValue(), ((Number) row[layout.xColumn]).longValue(),
                            ((Number) row[layout.zColumn]).longValue() });
                }
            }
            SegmentFrameCache.clear();

            long bytesBefore = segmentBytes(connection);
            long[] timeBefore = new long[1];
            List<List<Long>> before = lookUp(connection, spots, timeBefore);

            long began = System.nanoTime();
            long rewritten = ColdRollupTask.repackOldSegments(connection, () -> {
            }, 0);
            long repackMillis = (System.nanoTime() - began) / 1_000_000;
            SQLiteColdIndex.reload(connection);
            SegmentFrameCache.clear();

            long bytesAfter = segmentBytes(connection);
            long[] timeAfter = new long[1];
            List<List<Long>> after = lookUp(connection, spots, timeAfter);
            for (int index = 0; index < spots.size(); index++) {
                if (!before.get(index).equals(after.get(index))) {
                    throw new AssertionError("lookup " + index + " found " + after.get(index).size() + " rows after rewriting, " + before.get(index).size() + " before");
                }
            }

            System.out.println("repacked " + rewritten + " segments in " + repackMillis + " ms; segment bytes " + bytesBefore / 1048576 + " MB -> " + bytesAfter / 1048576 + " MB ("
                    + String.format("%+.1f%%", 100.0 * (bytesAfter - bytesBefore) / bytesBefore) + ")");
            System.out.println("place lookups (inspector sized), first read: " + String.format("%.2f", timeBefore[0] / 1e6 / spots.size()) + " ms before, "
                    + String.format("%.2f", timeAfter[0] / 1e6 / spots.size()) + " ms after; all " + spots.size() + " found the same rows");
        }
    }

    /**
     * Times lookups about real places on a copy, and reports how the time is spread. Run with:
     * {@code mvn -DskipTests=false -Dcoreprotect.prod.lookups=/path/to/copy.db -Dtest=ProductionSegmentBenchmark test}
     */
    @Test
    void timesLookupsAboutRealPlaces() throws Exception {
        String path = System.getProperty("coreprotect.prod.lookups");
        assumeTrue(path != null && !path.isEmpty(), "benchmark runs only when given a database");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:file:" + path + "?immutable=1")) {
            SQLiteColdIndex.reload(connection);
            for (String table : new String[] { "block", "container" }) {
                List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, table, 0, null, 0, 0, null);
                SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, table);
                Random random = new Random(29);
                List<long[]> spots = new ArrayList<>();
                for (int pick = 0; pick < 100; pick++) {
                    // The row with the lowest id, which is the same row whichever order the segment
                    // keeps its rows in.
                    ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, all.get(random.nextInt(all.size())));
                    int lowest = 0;
                    for (int row = 1; row < rows.size(); row++) {
                        if (rows.getRowId(row) < rows.getRowId(lowest)) {
                            lowest = row;
                        }
                    }
                    Object[] row = rows.getValues(lowest);
                    spots.add(new long[] { "block".equals(table) ? 0 : 1, ((Number) row[layout.worldColumn]).longValue(), ((Number) row[layout.xColumn]).longValue(),
                            ((Number) row[layout.zColumn]).longValue() });
                }
                SegmentFrameCache.clear();
                List<Long> times = new ArrayList<>();
                List<Integer> found = new ArrayList<>();
                long decodes = ColdSegmentReadCounter.reads();
                for (long[] spot : spots) {
                    long[] elapsed = new long[1];
                    List<List<Long>> result = lookUp(connection, java.util.Collections.singletonList(spot), elapsed);
                    times.add(elapsed[0]);
                    found.add(result.get(0).size());
                }
                List<Long> sorted = new ArrayList<>(times);
                java.util.Collections.sort(sorted);
                List<Integer> rowsSorted = new ArrayList<>(found);
                java.util.Collections.sort(rowsSorted);
                System.out.println(table + " place lookups: median " + sorted.get(50) / 1_000_000.0 + " ms, p90 " + sorted.get(90) / 1_000_000.0 + " ms, max " + sorted.get(99) / 1_000_000.0
                        + " ms; rows found median " + rowsSorted.get(50) + ", p90 " + rowsSorted.get(90) + ", max " + rowsSorted.get(99) + "; segments decoded per lookup "
                        + (ColdSegmentReadCounter.reads() - decodes) / 100.0 + " of " + all.size());
            }
        }
    }

    private static long segmentBytes(Connection connection) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("SELECT SUM(LENGTH(scalars) + LENGTH(COALESCE(payload,x''))) FROM co_segment");
                ResultSet results = statement.executeQuery()) {
            results.next();
            return results.getLong(1);
        }
    }

    private static List<List<Long>> lookUp(Connection connection, List<long[]> spots, long[] elapsed) throws Exception {
        String[] tables = { "block", "container" };
        List<List<Long>> found = new ArrayList<>();
        for (long[] spot : spots) {
            String table = tables[(int) spot[0]];
            int world = (int) spot[1];
            int x = (int) spot[2];
            int z = (int) spot[3];
            long began = System.nanoTime();
            SQLiteColdIndex.beginLookup(0, 0);
            List<Long> rows = new ArrayList<>();
            try {
                // Only what is packed: the copy holds the segments and none of the live rows.
                String source = SQLiteColdIndex.sourceExpression(connection, table, world, new Integer[] { 0, x, x, null, null, z, z });
                try (PreparedStatement statement = connection.prepareStatement("SELECT rowid, rolled_back FROM " + source + " WHERE wid = " + world + " AND x = " + x + " AND z = " + z + " ORDER BY rowid");
                        ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        rows.add(results.getLong(1) * 4 + results.getInt(2));
                    }
                }
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
            elapsed[0] += System.nanoTime() - began;
            found.add(rows);
        }
        return found;
    }

    private static void measure(Connection connection, String table, int sample) throws Exception {
        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, table);
        List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, table, 0, null, 0, 0, null);
        if (layout == null || all.isEmpty()) {
            return;
        }

        int[] units = { ColdSegmentCodec.UNIT_GROUPS };
        long storedScalars = 0;
        long storedPayload = 0;
        long rawBlobPayload = 0;
        long[] v2Scalars = new long[units.length];
        long withoutChunkLists = 0;
        long v1Nanos = 0;
        long[] v2Nanos = new long[units.length];
        long rows = 0;
        int lookups = 0;
        Random random = new Random(5);

        int step = Math.max(1, all.size() / sample);
        for (int index = 0; index < all.size(); index += step) {
            SQLiteColdIndex.ColdSegment segment = all.get(index);
            byte[] v1Stored;
            try (PreparedStatement statement = connection.prepareStatement("SELECT scalars, LENGTH(scalars), LENGTH(COALESCE(payload,x'')) FROM co_segment WHERE id = ?")) {
                statement.setLong(1, segment.getId());
                try (ResultSet results = statement.executeQuery()) {
                    results.next();
                    v1Stored = results.getBytes(1);
                    storedScalars += results.getLong(2);
                    storedPayload += results.getLong(3);
                }
            }

            ColdSegmentCodec.Rows decoded = SQLiteColdIndex.readRows(connection, segment);
            List<Long> rowIds = new ArrayList<>(decoded.size());
            List<Object[]> values = new ArrayList<>(decoded.size());
            ByteArrayOutputStream blobs = new ByteArrayOutputStream();
            for (int row = 0; row < decoded.size(); row++) {
                rowIds.add(decoded.getRowId(row));
                Object[] rowValues = decoded.getValues(row).clone();
                for (int column = 0; column < rowValues.length; column++) {
                    if (rowValues[column] instanceof byte[]) {
                        rowValues[column] = ColdRollupTask.unpackBlob((byte[]) rowValues[column]);
                        blobs.write((byte[]) rowValues[column]);
                    }
                    else if (rowValues[column] instanceof String) {
                        blobs.write(((String) rowValues[column]).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
                values.add(rowValues);
            }
            rows += decoded.size();
            byte[] raw = blobs.toByteArray();
            rawBlobPayload += raw.length == 0 ? 0 : Zstd.compress(raw, 19).length;

            ColdRollupTask.orderForLookup(table, layout, rowIds, values);
            long[] ids = new long[rowIds.size()];
            for (int row = 0; row < ids.length; row++) {
                ids[row] = rowIds.get(row);
            }

            byte[][] encoded = new byte[units.length][];
            for (int unit = 0; unit < units.length; unit++) {
                encoded[unit] = ColdSegmentCodec.encode(layout.types, ids, values, ColdSegmentCodec.GROUP_ROWS, units[unit], layout.chunkColumns()).getScalars();
                v2Scalars[unit] += encoded[unit].length;
            }
            withoutChunkLists += ColdSegmentCodec.encode(layout.types, ids, values, ColdSegmentCodec.GROUP_ROWS, ColdSegmentCodec.UNIT_GROUPS, null).getScalars().length;

            if (layout.xColumn < 0 || layout.zColumn < 0) {
                continue;
            }
            // Lookups about one place, at places this segment really holds.
            for (int probe = 0; probe < 5; probe++) {
                Object[] target = values.get(random.nextInt(values.size()));
                if (target[layout.xColumn] == null || target[layout.zColumn] == null) {
                    continue;
                }
                long x = ((Number) target[layout.xColumn]).longValue();
                long z = ((Number) target[layout.zColumn]).longValue();
                ColdSegmentCodec.RowFilter filter = placeFilter(layout, x, z);
                ColdSegmentCodec.RowFilter plain = filter::accept;

                long began = System.nanoTime();
                byte[] v1 = SegmentDictionary.decompress(v1Stored, segmentScalarSize(connection, segment), 0, connection);
                int v1Kept = ColdSegmentCodec.count(v1, plain);
                v1Nanos += System.nanoTime() - began;
                for (int unit = 0; unit < units.length; unit++) {
                    began = System.nanoTime();
                    int kept = ColdSegmentCodec.count(encoded[unit], filter);
                    v2Nanos[unit] += System.nanoTime() - began;
                    if (kept != v1Kept) {
                        throw new AssertionError(table + " segment " + segment.getId() + ": v2 found " + kept + " rows, v1 " + v1Kept);
                    }
                }
                lookups++;
            }
        }

        StringBuilder line = new StringBuilder(table + ": " + rows + " rows; stored scalars " + storedScalars / 1024 + " KB, payload " + storedPayload / 1024
                + " KB (plain blobs at level 19 without a dictionary: " + rawBlobPayload / 1024 + " KB)");
        for (int unit = 0; unit < units.length; unit++) {
            line.append("\n  unit ").append(units[unit]).append(": scalars ").append(v2Scalars[unit] / 1024).append(" KB (")
                    .append(String.format("%+.1f%%", 100.0 * (v2Scalars[unit] - storedScalars) / Math.max(1, storedScalars))).append(")");
            if (lookups > 0) {
                line.append(", place lookup ").append(String.format("%.3f", v2Nanos[unit] / 1e6 / lookups)).append(" ms per segment");
            }
        }
        line.append("\n  group chunk lists: ").append((v2Scalars[0] - withoutChunkLists) / 1024).append(" KB (")
                .append(String.format("%.2f%%", 100.0 * (v2Scalars[0] - withoutChunkLists) / Math.max(1, withoutChunkLists))).append(" of the scalars without them)");
        if (lookups > 0) {
            line.append("\n  first format place lookup ").append(String.format("%.3f", v1Nanos / 1e6 / lookups)).append(" ms per segment");
        }
        System.out.println(line);
    }

    private static int segmentScalarSize(Connection connection, SQLiteColdIndex.ColdSegment segment) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("SELECT scalars_size FROM co_segment WHERE id = ?")) {
            statement.setLong(1, segment.getId());
            try (ResultSet results = statement.executeQuery()) {
                results.next();
                return results.getInt(1);
            }
        }
    }

    private static ColdSegmentCodec.RowFilter placeFilter(SQLiteColdIndex.TableLayout layout, long x, long z) {
        int xColumn = layout.xColumn;
        int zColumn = layout.zColumn;
        return new ColdSegmentCodec.RowFilter() {
            @Override
            public boolean accept(long[] columns, boolean[] present) {
                return (!present[xColumn] || columns[xColumn] == x) && (!present[zColumn] || columns[zColumn] == z);
            }

            @Override
            public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                return !(known[xColumn] && (x < minimum[xColumn] || x > maximum[xColumn])) && !(known[zColumn] && (z < minimum[zColumn] || z > maximum[zColumn]));
            }
        };
    }
}
