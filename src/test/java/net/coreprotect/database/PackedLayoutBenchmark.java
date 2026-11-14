package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * Measures how packed storage performs on activity shaped like a real server's: how small it is,
 * and how long the lookups players actually run take once everything has been packed.
 *
 * <p>
 * Run with: {@code mvn -DskipTests=false -Dcoreprotect.bench=layout -Dtest=PackedLayoutBenchmark test}
 * </p>
 */
class PackedLayoutBenchmark {

    @Test
    void measuresPackedLayout(@TempDir Path directory) throws Exception {
        assumeTrue("layout".equals(System.getProperty("coreprotect.bench")), "benchmark runs only when asked for");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        int rows = Integer.getInteger("coreprotect.bench.rows", 2_000_000);
        Path database = directory.resolve("database.db");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database)) {
            RealisticActivity activity = new RealisticActivity(11, 90);
            RealisticActivity.createBlockTable(connection);
            long first = ((System.currentTimeMillis() / 1000L - 40 * RealisticActivity.DAY) / RealisticActivity.DAY) * RealisticActivity.DAY;
            activity.writeBlocks(connection, first, rows, 2);
            measure(connection, activity.spots(), "live_indexed");

            long start = System.nanoTime();
            long sealed = ColdRollupTask.rollUp(connection, () -> {
            }, System.currentTimeMillis() / 1000L + 1);
            System.out.println("pack_ms=" + (System.nanoTime() - start) / 1_000_000 + " packed=" + sealed);
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT COUNT(*), SUM(LENGTH(scalars)), SUM(LENGTH(COALESCE(payload,x''))) FROM co_segment")) {
                results.next();
                System.out.println("segments=" + results.getLong(1) + " scalar_bytes=" + results.getLong(2) + " payload_bytes=" + results.getLong(3));
            }

            describeChunkRecords(connection, activity.spots());
            measure(connection, activity.spots(), "packed_first");
            measure(connection, activity.spots(), "packed_again");

            // The same lookups with nothing in memory, first with the unit bodies stored apart as
            // they are written now, then with every segment joined back into the frame version 3
            // stored, and then split again by the repack, so the two layouts are measured on the
            // same rows and neither has the advantage of running last.
            measureCold(database, activity.spots(), "cold_split");
            joinUnitsIntoFrames(connection);
            measureCold(database, activity.spots(), "cold_inline");
            long split = ColdRollupTask.repackOldSegments(connection, () -> {
            }, 0);
            System.out.println("split_by_repack=" + split);
            measureCold(database, activity.spots(), "cold_split_again");
        }
    }

    /**
     * Measures what compacting finished days does to lookups about a place, on a month of busy
     * container activity packed into hundreds of segments a day, as a busy server packs it.
     *
     * <p>
     * The same packed database is compacted once for each target segment size, and every lookup is
     * run against the packed database and each compacted one, with nothing in memory and again with
     * everything in memory. Each compacted database is then measured once more with each segment's
     * payload joined back into one frame, the way packing stores it, to show what keeping each
     * unit's share apart is worth. Timings on a shared machine are noisy; the bytes, directories,
     * units and groups read per lookup are not.
     * </p>
     *
     * <p>
     * Run with: {@code mvn -DskipTests=false -Dcoreprotect.bench=layout -Dtest=PackedLayoutBenchmark#measuresCompaction test},
     * and {@code -Dcoreprotect.bench.days}, {@code .dayRows}, {@code .segmentRows}, {@code .targets}
     * and {@code .samples} to change its size.
     * </p>
     */
    @Test
    void measuresCompaction(@TempDir Path directory) throws Exception {
        assumeTrue("layout".equals(System.getProperty("coreprotect.bench")), "benchmark runs only when asked for");

        int days = Integer.getInteger("coreprotect.bench.days", 30);
        int dayRows = Integer.getInteger("coreprotect.bench.dayRows", 600_000);
        int segmentRows = Integer.getInteger("coreprotect.bench.segmentRows", 2048);
        int samples = Integer.getInteger("coreprotect.bench.samples", 150);
        String[] targets = System.getProperty("coreprotect.bench.targets", "65536,262144,1048576").split(",");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ColdRollupTask.setSegmentRowsForTesting(segmentRows);
        try {
            Path packed = directory.resolve("packed.db");
            RealisticActivity activity = new RealisticActivity(13, 90);
            try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + packed)) {
                RealisticActivity.createContainerTable(connection);
                long first = ((System.currentTimeMillis() / 1000L) / RealisticActivity.DAY - days - 2) * RealisticActivity.DAY;
                long began = System.nanoTime();
                for (int day = 0; day < days; day++) {
                    activity.writeContainers(connection, first + day * RealisticActivity.DAY, dayRows, RealisticActivity.DAY - 1);
                }
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (" + (System.currentTimeMillis() / 1000L - 5)
                            + ",1,1,0,64,0,1,0,1,NULL,1,0)");
                }
                System.out.println("write_ms=" + (System.nanoTime() - began) / 1_000_000 + " rows=" + (long) days * dayRows);

                began = System.nanoTime();
                long sealed = ColdRollupTask.rollUp(connection, () -> {
                });
                System.out.println("pack_ms=" + (System.nanoTime() - began) / 1_000_000 + " packed=" + sealed);
                describeStorage(connection, "uncompacted", days);
                try (Statement statement = connection.createStatement()) {
                    statement.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                }
            }

            List<RealisticActivity.Spot> spots = activity.containerSpots();
            measureLookups(packed, spots, samples, "uncompacted");

            for (String target : targets) {
                Path compacted = directory.resolve("compacted-" + target.trim() + ".db");
                java.nio.file.Files.copy(packed, compacted);
                SQLiteColdIndex.invalidate();
                SegmentDictionary.clearCache();
                SegmentCompactor.setTargetRowsForTesting(Integer.parseInt(target.trim()));
                try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + compacted)) {
                    try (Statement statement = connection.createStatement()) {
                        SQLiteSchema.createTables("co_", statement);
                    }
                    SegmentCompactor.resetPeakHeldRows();
                    long began = System.nanoTime();
                    long merged = SegmentCompactor.compactAll(connection, () -> {
                    });
                    long elapsed = (System.nanoTime() - began) / 1_000_000;
                    System.out.println("target=" + target.trim() + " compact_ms=" + elapsed + " rows_merged=" + merged + " rows_per_second=" + (merged * 1000L / Math.max(1, elapsed))
                            + " peak_decoded_rows=" + SegmentCompactor.peakHeldRows());
                    describeStorage(connection, "compacted_" + target.trim(), days);
                    try (Statement statement = connection.createStatement()) {
                        statement.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                    }
                }
                measureLookups(compacted, spots, samples, "compacted_" + target.trim());

                SQLiteColdIndex.invalidate();
                try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + compacted)) {
                    joinPayloadShares(connection);
                    describeStorage(connection, "compacted_" + target.trim() + "_one_payload", days);
                    try (Statement statement = connection.createStatement()) {
                        statement.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                    }
                }
                measureLookups(compacted, spots, samples, "compacted_" + target.trim() + "_one_payload");
                java.nio.file.Files.deleteIfExists(compacted);
            }
        }
        finally {
            ColdRollupTask.setSegmentRowsForTesting(0);
            SegmentCompactor.setTargetRowsForTesting(0);
            SQLiteColdIndex.invalidate();
            SegmentDictionary.clearCache();
        }
    }

    /** Reports how many segments there are and how their bytes divide between directories, units and payload. */
    private static void describeStorage(Connection connection, String label, int days) throws Exception {
        try (Statement statement = connection.createStatement();
                ResultSet segments = statement.executeQuery("SELECT COUNT(*), SUM(LENGTH(scalars)), MAX(LENGTH(scalars)), SUM(LENGTH(COALESCE(payload,x''))), MAX(row_count) FROM co_segment WHERE compacted >= 0")) {
            segments.next();
            long count = segments.getLong(1);
            long directories = segments.getLong(2);
            long largestDirectory = segments.getLong(3);
            long framePayload = segments.getLong(4);
            long largestSegment = segments.getLong(5);
            try (ResultSet units = statement.executeQuery("SELECT SUM(LENGTH(body)), SUM(LENGTH(COALESCE(payload,x''))), COUNT(*) FROM co_segment_unit")) {
                units.next();
                System.out.println(label + " segments=" + count + " segments_per_day=" + String.format("%.1f", count / (double) days) + " largest_segment_rows=" + largestSegment
                        + " directory_bytes=" + directories + " largest_directory_bytes=" + largestDirectory + " unit_body_bytes=" + units.getLong(1) + " units=" + units.getLong(3)
                        + " payload_bytes=" + (framePayload + units.getLong(2)) + " (in units " + units.getLong(2) + ")");
            }
        }
    }

    /**
     * Joins each compacted segment's payload shares back into one frame in the segment row, the way
     * packing stores a payload, so the two ways of storing it can be measured on the same rows.
     */
    private static void joinPayloadShares(Connection connection) throws Exception {
        List<long[]> segments = new java.util.ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT id, payload_size, dict_id FROM co_segment WHERE payload IS NULL AND payload_size > 0")) {
            while (results.next()) {
                segments.add(new long[] { results.getLong(1), results.getLong(2), results.getLong(3) });
            }
        }
        Database.inTransaction(connection, () -> {
            for (long[] segment : segments) {
                byte[] frame = SQLiteColdIndex.readWholePayloadFromShares(connection, segment[0], SegmentUnitStorageTest.directory(connection, segment[0]), (int) segment[1], (int) segment[2]);
                try (java.sql.PreparedStatement statement = connection.prepareStatement("UPDATE co_segment SET payload = ? WHERE id = ?")) {
                    statement.setBytes(1, SegmentDictionary.compress(frame, (int) segment[2], connection));
                    statement.setLong(2, segment[0]);
                    statement.executeUpdate();
                }
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("UPDATE co_segment_unit SET payload = NULL WHERE segment_id = " + segment[0]);
                }
            }
        });
    }

    /**
     * Runs the inspector on one container, a lookup of a radius of ten around it, and a page of one
     * player's newest rows, first with nothing in memory before each lookup and then with
     * everything a first pass read still in memory.
     */
    private static void measureLookups(Path database, List<RealisticActivity.Spot> spots, int samples, String label) throws Exception {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        int count = Math.min(samples, spots.size());
        String[] kinds = { "inspector", "radius10", "user_page" };
        for (boolean cold : new boolean[] { true, false }) {
            Connection shared = cold ? null : DriverManager.getConnection("jdbc:sqlite:" + database);
            try {
                if (!cold) {
                    // A first pass fills the frame cache, so the pass measured finds what it needs there.
                    for (int index = 0; index < count; index++) {
                        RealisticActivity.Spot spot = spots.get((int) ((index * 7919L) % spots.size()));
                        for (String kind : kinds) {
                            lookUpContainers(shared, kind, spot);
                        }
                    }
                }
                for (String kind : kinds) {
                    long bytes = 0;
                    long payloadBytes = 0;
                    long directories = 0;
                    long units = 0;
                    long groups = 0;
                    long decoded = 0;
                    long candidates = 0;
                    long rows = 0;
                    long materialized = 0;
                    List<Long> timings = new java.util.ArrayList<>();
                    for (int index = 0; index < count; index++) {
                        RealisticActivity.Spot spot = spots.get((int) ((index * 7919L) % spots.size()));
                        Connection connection = shared;
                        if (cold) {
                            SegmentFrameCache.clear();
                            connection = DriverManager.getConnection("jdbc:sqlite:" + database);
                        }
                        try {
                            long bytesBefore = ColdSegmentReadCounter.bytesRead();
                            long payloadBefore = ColdSegmentReadCounter.payloadBytesRead();
                            long directoriesBefore = ColdSegmentReadCounter.frameReads();
                            long unitsBefore = ColdSegmentReadCounter.unitReads();
                            long groupsBefore = ColdSegmentReadCounter.groupReads();
                            long decodedBefore = ColdSegmentReadCounter.reads();
                            long materializedBefore = ColdSegmentReadCounter.scratchRows();
                            long began = System.nanoTime();
                            rows += lookUpContainers(connection, kind, spot);
                            timings.add(System.nanoTime() - began);
                            bytes += ColdSegmentReadCounter.bytesRead() - bytesBefore;
                            payloadBytes += ColdSegmentReadCounter.payloadBytesRead() - payloadBefore;
                            directories += ColdSegmentReadCounter.frameReads() - directoriesBefore;
                            units += ColdSegmentReadCounter.unitReads() - unitsBefore;
                            groups += ColdSegmentReadCounter.groupReads() - groupsBefore;
                            decoded += ColdSegmentReadCounter.reads() - decodedBefore;
                            materialized += ColdSegmentReadCounter.scratchRows() - materializedBefore;
                            candidates += candidates(connection, kind, spot);
                        }
                        finally {
                            if (cold) {
                                connection.close();
                            }
                        }
                    }
                    java.util.Collections.sort(timings);
                    long total = 0;
                    for (long timing : timings) {
                        total += timing;
                    }
                    System.out.println(label + " " + (cold ? "cold" : "warm") + " " + kind + " avg_ms=" + String.format("%.2f", total / 1e6 / count) + " median_ms="
                            + String.format("%.2f", timings.get(count / 2) / 1e6) + " p90_ms=" + String.format("%.2f", timings.get(count * 9 / 10) / 1e6) + " max_ms="
                            + String.format("%.2f", timings.get(count - 1) / 1e6) + " bytes_read=" + bytes / count + " payload_bytes=" + payloadBytes / count + " candidates="
                            + String.format("%.1f", candidates / (double) count) + " segments_decoded=" + String.format("%.1f", decoded / (double) count) + " directories_read="
                            + String.format("%.1f", directories / (double) count) + " units_read=" + String.format("%.1f", units / (double) count) + " groups_decoded="
                            + String.format("%.1f", groups / (double) count) + " rows_materialized=" + materialized / count + " rows_found=" + String.format("%.1f", rows / (double) count));
                }
            }
            finally {
                if (shared != null) {
                    shared.close();
                }
            }
        }
    }

    private static Integer[] boundsOf(String kind, RealisticActivity.Spot spot) {
        int radius = "radius10".equals(kind) ? 10 : 0;
        return new Integer[] { radius, spot.x - radius, spot.x + radius, null, null, spot.z - radius, spot.z + radius };
    }

    /** How many segments the lookup's place, or its player, leaves to be read before any is opened. */
    private static long candidates(Connection connection, String kind, RealisticActivity.Spot spot) throws Exception {
        if ("user_page".equals(kind)) {
            long count = 0;
            for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, "container", 0, null, 0, 0, null)) {
                if (segment.userStats == null || segment.userStats.count(spot.user) > 0) {
                    count++;
                }
            }
            return count;
        }
        return SQLiteColdIndex.selectSegments(connection, "container", 1, boundsOf(kind, spot), 0, 0, null).size();
    }

    /** Runs one lookup the way the inspector, a radius lookup or a player's first page reach packed storage. */
    private static long lookUpContainers(Connection connection, String kind, RealisticActivity.Spot spot) throws Exception {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String query;
            if ("user_page".equals(kind)) {
                SQLiteColdIndex.setLookupFilters(Integer.toString(spot.user), "");
                SQLiteColdIndex.setRowBudget(10);
                String source = SQLiteColdIndex.sourceExpression(connection, "container", 0, null);
                query = "SELECT rowid,user,type,amount,metadata FROM " + source + " WHERE user IN(" + spot.user + ") ORDER BY rowid DESC LIMIT 10";
            }
            else {
                Integer[] bounds = boundsOf(kind, spot);
                String source = SQLiteColdIndex.sourceExpression(connection, "container", 1, bounds);
                String where = "inspector".equals(kind) ? "x = " + spot.x + " AND z = " + spot.z + " AND y = " + spot.y
                        : "x >= " + bounds[1] + " AND x <= " + bounds[2] + " AND z >= " + bounds[5] + " AND z <= " + bounds[6];
                query = "SELECT rowid,user,type,amount,metadata FROM " + source + " WHERE wid = 1 AND " + where + " ORDER BY time DESC, rowid DESC LIMIT 7";
            }
            long found = 0;
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
                while (results.next()) {
                    found++;
                }
            }
            return found;
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    /**
     * Rewrites every segment the way version 3 stored it: the unit bodies back inside the scalar
     * frame, and no unit rows.
     */
    private static void joinUnitsIntoFrames(Connection connection) throws Exception {
        List<Long> segments = new java.util.ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT id FROM co_segment WHERE codec_version = " + ColdSegmentCodec.CODEC_VERSION)) {
            while (results.next()) {
                segments.add(results.getLong(1));
            }
        }
        Database.inTransaction(connection, () -> {
            for (long segment : segments) {
                SegmentUnitStorageTest.joinUnits(connection, segment);
            }
        });
        SegmentFrameCache.clear();
    }

    /**
     * Runs lookups about one block and about a radius of ten with nothing held in memory: before each
     * one the frame cache is emptied and a fresh connection is opened, so SQLite's page cache is
     * empty too. Reports how many bytes each lookup read from the database, how many of them were
     * payload frames, and how long it took.
     */
    private static void measureCold(Path database, List<RealisticActivity.Spot> spots, String label) throws Exception {
        int samples = Math.min(200, spots.size());
        for (int radius : new int[] { 0, 10 }) {
            long bytes = 0;
            long units = 0;
            long frames = 0;
            long payloads = 0;
            long payloadBytes = 0;
            long matches = 0;
            List<Long> timings = new java.util.ArrayList<>();
            for (int index = 0; index < samples; index++) {
                RealisticActivity.Spot spot = spots.get((index * 7919) % spots.size());
                SegmentFrameCache.clear();
                try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database)) {
                    long bytesBefore = ColdSegmentReadCounter.bytesRead();
                    long unitsBefore = ColdSegmentReadCounter.unitReads();
                    long framesBefore = ColdSegmentReadCounter.frameReads();
                    long payloadsBefore = ColdSegmentReadCounter.payloadFetches();
                    long payloadBytesBefore = ColdSegmentReadCounter.payloadBytesRead();
                    String where = radius == 0 ? "x = " + spot.x + " AND z = " + spot.z + " AND y = " + spot.y
                            : "x >= " + (spot.x - radius) + " AND x <= " + (spot.x + radius) + " AND z >= " + (spot.z - radius) + " AND z <= " + (spot.z + radius);
                    long began = System.nanoTime();
                    matches += query(connection, 1, spot.x - radius, spot.x + radius, spot.z - radius, spot.z + radius, where);
                    timings.add(System.nanoTime() - began);
                    bytes += ColdSegmentReadCounter.bytesRead() - bytesBefore;
                    units += ColdSegmentReadCounter.unitReads() - unitsBefore;
                    frames += ColdSegmentReadCounter.frameReads() - framesBefore;
                    payloads += ColdSegmentReadCounter.payloadFetches() - payloadsBefore;
                    payloadBytes += ColdSegmentReadCounter.payloadBytesRead() - payloadBytesBefore;
                }
            }
            java.util.Collections.sort(timings);
            long total = 0;
            for (long timing : timings) {
                total += timing;
            }
            System.out.println(label + " radius=" + radius + " bytes_read_per_lookup=" + bytes / samples + " frames_per_lookup=" + String.format("%.2f", frames / (double) samples)
                    + " units_per_lookup=" + String.format("%.2f", units / (double) samples) + " payloads_per_lookup=" + String.format("%.2f", payloads / (double) samples)
                    + " payload_bytes_per_lookup=" + payloadBytes / samples + " avg_ms=" + String.format("%.3f", total / 1e6 / samples)
                    + " median_ms=" + String.format("%.3f", timings.get(samples / 2) / 1e6) + " p90_ms=" + String.format("%.3f", timings.get(samples * 9 / 10) / 1e6) + " matches=" + matches);
        }
    }

    /**
     * Reports what recording chunks costs and what it saves: how much the group chunk lists add to
     * the scalar frames, how much memory the exact counts of a segment take, and how many segments a
     * lookup about one block opens by the exact counts compared with the chunk filter alone.
     */
    private static void describeChunkRecords(Connection connection, List<RealisticActivity.Spot> spots) throws Exception {
        SQLiteColdIndex.reload(connection);
        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "block");
        List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null);
        long withLists = 0;
        long withoutLists = 0;
        long heap = 0;
        long chunks = 0;
        long storedCounts = 0;
        java.util.Map<Long, SegmentMembership> filters = new java.util.HashMap<>();
        for (SQLiteColdIndex.ColdSegment segment : all) {
            ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
            long[] ids = new long[rows.size()];
            List<Object[]> values = new java.util.ArrayList<>(rows.size());
            for (int row = 0; row < rows.size(); row++) {
                ids[row] = rows.getRowId(row);
                values.add(rows.getValues(row));
            }
            withLists += ColdSegmentCodec.encode(layout.types, ids, values, layout.chunkColumns()).getScalars().length;
            withoutLists += ColdSegmentCodec.encode(layout.types, ids, values, null).getScalars().length;
            heap += segment.getChunkCounts().heapBytes();
            chunks += segment.getChunkCounts().distinctChunks();
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT chunk_filter, LENGTH(chunk_counts) FROM co_segment WHERE id = " + segment.getId())) {
                results.next();
                filters.put(segment.getId(), SegmentMembership.decode(results.getBytes(1)));
                storedCounts += results.getLong(2);
            }
        }

        long byFilter = 0;
        long byCounts = 0;
        int samples = Math.min(200, spots.size());
        for (int index = 0; index < samples; index++) {
            RealisticActivity.Spot spot = spots.get((index * 7919) % spots.size());
            long[] keys = SQLiteColdIndex.chunkKeys(1, new Integer[] { 0, spot.x, spot.x, null, null, spot.z, spot.z });
            int chunkX = SegmentFilter.chunkOf(spot.x);
            int chunkZ = SegmentFilter.chunkOf(spot.z);
            for (SQLiteColdIndex.ColdSegment segment : all) {
                SegmentMembership filter = filters.get(segment.getId());
                if (filter == null || filter.mightContainAny(keys)) {
                    byFilter++;
                }
                if (segment.getChunkCounts().mayHoldAny(1, chunkX, chunkX, chunkZ, chunkZ)) {
                    byCounts++;
                }
            }
        }
        System.out.println("group_chunk_list_bytes=" + (withLists - withoutLists) + " (" + String.format("%.2f%%", 100.0 * (withLists - withoutLists) / withoutLists) + " of scalars)"
                + " chunk_counts_stored_bytes_per_segment=" + storedCounts / all.size() + " chunks_per_segment=" + chunks / all.size()
                + " chunk_counts_heap_bytes_per_segment=" + heap / all.size());
        System.out.println("segments_per_block_lookup by_chunk_filter=" + String.format("%.2f", byFilter / (double) samples) + " by_chunk_counts="
                + String.format("%.2f", byCounts / (double) samples) + " of " + all.size());
    }

    private static void measure(Connection connection, List<RealisticActivity.Spot> spots, String label) throws Exception {
        {
            {
                long decodes = ColdSegmentReadCounter.reads();
                long groups = ColdSegmentReadCounter.groupReads();
                long inspector = 0;
                long radius = 0;
                long matches = 0;
                int samples = Math.min(200, spots.size());
                for (int index = 0; index < samples; index++) {
                    RealisticActivity.Spot spot = spots.get((index * 7919) % spots.size());
                    long began = System.nanoTime();
                    matches += query(connection, 1, spot.x, spot.x, spot.z, spot.z, "x = " + spot.x + " AND z = " + spot.z + " AND y = " + spot.y);
                    inspector += System.nanoTime() - began;
                    began = System.nanoTime();
                    matches += query(connection, 1, spot.x - 10, spot.x + 10, spot.z - 10, spot.z + 10, "x >= " + (spot.x - 10) + " AND x <= " + (spot.x + 10) + " AND z >= " + (spot.z - 10) + " AND z <= " + (spot.z + 10));
                    radius += System.nanoTime() - began;
                }
                long users = 0;
                for (int index = 0; index < 50; index++) {
                    RealisticActivity.Spot spot = spots.get((index * 104729) % spots.size());
                    long began = System.nanoTime();
                    SQLiteColdIndex.beginLookup(0, 0);
                    try {
                        SQLiteColdIndex.setLookupFilters(Integer.toString(spot.user), "");
                        SQLiteColdIndex.setRowBudget(10);
                        String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
                        try (Statement statement = connection.createStatement();
                                ResultSet results = statement.executeQuery("SELECT rowid FROM " + source + " WHERE user IN(" + spot.user + ") ORDER BY rowid DESC LIMIT 10")) {
                            while (results.next()) {
                                matches++;
                            }
                        }
                    }
                    finally {
                        SQLiteColdIndex.endLookup(connection);
                    }
                    users += System.nanoTime() - began;
                }
                System.out.println(label + " user_page_avg_ms=" + String.format("%.2f", users / 1e6 / 50));
                System.out.println(label + " inspector_avg_ms=" + String.format("%.2f", inspector / 1e6 / samples) + " radius10_avg_ms=" + String.format("%.2f", radius / 1e6 / samples)
                        + " segment_decodes_per_lookup=" + String.format("%.1f", (ColdSegmentReadCounter.reads() - decodes) / (2.0 * samples)) + " groups_per_lookup=" + String.format("%.1f", (ColdSegmentReadCounter.groupReads() - groups) / (2.0 * samples)) + " matches=" + matches);
            }
        }
    }

    private static long query(Connection connection, int worldId, int minX, int maxX, int minZ, int maxZ, String where) throws Exception {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", worldId, new Integer[] { 0, minX, maxX, 0, 0, minZ, maxZ });
            long found = 0;
            try (Statement statement = connection.createStatement();
                    ResultSet results = statement.executeQuery("SELECT rowid,user,type FROM " + source + " WHERE wid = " + worldId + " AND " + where + " ORDER BY time DESC, rowid DESC LIMIT 7")) {
                while (results.next()) {
                    found++;
                }
            }
            return found;
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }
}
