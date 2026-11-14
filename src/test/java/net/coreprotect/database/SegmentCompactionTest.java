package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * Compaction merges the many segments of a day that is over into a few ordered by place across the
 * whole day. It may only ever make lookups cheaper: every row keeps its id and every value, what is
 * recorded about the new segments is exactly what their rows hold, every lookup finds exactly what
 * plain SQL finds before and after, and no lookup ever sees a row twice or not at all while it runs.
 */
class SegmentCompactionTest {

    private static final long DAY = 86400L;

    /** Rows per packed segment, low enough that a day of test data is packed into many. */
    private static final int SEGMENT_ROWS = 1500;

    /** Rows per compacted segment, low enough that a day is compacted into several. */
    private static final int TARGET_ROWS = 5000;

    private Path database;
    private Connection connection;
    private DatabaseType previousType;
    private RealisticActivity activity;
    private long firstDay;

    @org.junit.jupiter.api.BeforeAll
    static void prepareServer() {
        // Setting a segment aside says so on the console, which needs a server to say it on.
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = org.mockito.Mockito.mock(org.bukkit.Server.class);
            org.bukkit.command.ConsoleCommandSender console = org.mockito.Mockito.mock(org.bukkit.command.ConsoleCommandSender.class);
            org.mockito.Mockito.when(server.getConsoleSender()).thenReturn(console);
            org.mockito.Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
    }

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ColdRollupTask.setSegmentRowsForTesting(SEGMENT_ROWS);
        SegmentCompactor.setTargetRowsForTesting(TARGET_ROWS);

        database = directory.resolve("database.db");
        connection = open();
        RealisticActivity.createBlockTable(connection);
        activity = new RealisticActivity(29, 30);
        firstDay = ((now() / DAY) - 6) * DAY;
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        ColdRollupTask.setSegmentRowsForTesting(0);
        SegmentCompactor.setTargetRowsForTesting(0);
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void aDayThatCannotBeReadIsSetAsideAndTheOthersAreStillCompacted() throws Exception {
        SegmentQuarantine.clear();
        try {
            writeDays(3, 12_000);
            pack();
            Map<Long, List<Long>> byDay = segmentsByDay();
            long brokenDay = byDay.keySet().iterator().next();
            long broken = byDay.get(brokenDay).get(2);
            try (Statement statement = connection.createStatement()) {
                // A unit that no longer decompresses, as a damaged file would leave it.
                statement.executeUpdate("UPDATE co_segment_unit SET body = x'00112233445566778899' WHERE segment_id = " + broken);
            }
            SegmentFrameCache.clear();

            // The background packing takes one step at a time and must not fail on this day forever.
            for (int step = 0; step < 50; step++) {
                SegmentCompactor.compactNext(connection, () -> {
                });
            }

            assertTrue(SegmentQuarantine.contains(broken), "the day holding the damaged segment is set aside");
            for (long id : byDay.get(brokenDay)) {
                assertEquals(1, count("SELECT COUNT(*) FROM co_segment WHERE id = " + id), "the day set aside is left exactly as it was");
            }
            assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE day <> " + brokenDay + " AND compacted <> " + SegmentCompactor.COMPACTED),
                    "every other day is compacted");
        }
        finally {
            SegmentQuarantine.clear();
        }
    }

    @Test
    void compactingADayKeepsEveryRowAndRecordsExactlyWhatItsSegmentsHold() throws Exception {
        writeDays(3, 12_000);
        pack();
        Map<Long, String> before = packedRows("block");
        Map<Long, List<Long>> inputsByDay = segmentsByDay();
        Map<Long, long[]> inputRanges = new TreeMap<>();
        Map<Long, Map<Long, Long>> inputUsers = new TreeMap<>();
        for (Map.Entry<Long, List<Long>> day : inputsByDay.entrySet()) {
            assertTrue(day.getValue().size() >= 8, "a day of test data is packed into many segments");
            inputRanges.put(day.getKey(), range(day.getValue()));
            inputUsers.put(day.getKey(), userCounts(day.getValue()));
        }

        assertTrue(SegmentCompactor.compactAll(connection, () -> {
        }) > 0);

        assertEquals(before, packedRows("block"), "every row keeps its id and every value, blobs included");
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE compacted <> " + SegmentCompactor.COMPACTED), "every day that is over is compacted");
        SegmentUnitStorageTest.assertUnitsMatchSegments(connection);
        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "block");
        ColdRollupTask.LookupOrder order = ColdRollupTask.LookupOrder.of("block", layout);

        Map<Long, List<Long>> outputsByDay = segmentsByDay();
        assertEquals(inputsByDay.keySet(), outputsByDay.keySet());
        for (Map.Entry<Long, List<Long>> day : outputsByDay.entrySet()) {
            List<Long> outputs = day.getValue();
            long rows = count("SELECT SUM(row_count) FROM co_segment WHERE day = " + day.getKey());
            assertEquals((rows + TARGET_ROWS - 1) / TARGET_ROWS, outputs.size(), "a day is divided into as few segments as the target allows");
            assertTrue(count("SELECT MAX(row_count) - MIN(row_count) FROM co_segment WHERE day = " + day.getKey()) <= 1, "all of about the same size");

            long[] expectedRange = inputRanges.get(day.getKey());
            long[] previous = null;
            for (long id : outputs) {
                assertArrayEquals(expectedRange, range(List.of(id)), "each compacted segment claims the whole run's row ids");
                assertEquals(1, count("SELECT COUNT(*) FROM co_segment WHERE id = " + id + " AND payload IS NULL AND payload_size > 0"), "the payload is kept per unit");
                assertEquals(0, count("SELECT COUNT(*) FROM co_segment_unit WHERE segment_id = " + id + " AND payload IS NULL"), "every unit keeps its share");
                assertMetadataMatchesRows(id, layout);

                // One order across the whole day: each segment continues where the one before ended.
                ColdSegmentCodec.Rows segmentRows = SQLiteColdIndex.readRows(connection, SQLiteColdIndex.segmentById(connection, id));
                for (int row = 0; row < segmentRows.size(); row++) {
                    long[] key = new long[order.keyLength()];
                    order.key(segmentRows.getRowId(row), segmentRows.getValues(row), key);
                    if (previous != null) {
                        assertTrue(ColdRollupTask.LookupOrder.compare(previous, key) < 0, "rows are ordered by place across the whole day");
                    }
                    previous = key;
                }
            }
            assertEquals(inputUsers.get(day.getKey()), userCounts(outputs), "the rows per player of the day add up to what the packed segments recorded");
        }

        assertEquals(0, SegmentCompactor.compactAll(connection, () -> {
        }), "nothing is compacted twice");
    }

    @Test
    void lookupsFindWhatSqlFindsBeforeAndAfterCompaction() throws Exception {
        writeDays(3, 12_000);
        keepPlainCopy();
        pack();
        rollBackSomePackedRows();

        List<String> before = everyLookup();
        SegmentCompactor.compactAll(connection, () -> {
        });
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE compacted <> " + SegmentCompactor.COMPACTED));

        SegmentFrameCache.clear();
        assertEquals(before, everyLookup(), "every lookup finds the same rows after compaction");
    }

    @Test
    void segmentsAreNeverMergedAcrossASegmentOfAnotherDay() throws Exception {
        long day = firstDay + DAY;
        insertRows(day + 100, 6000, 1);
        // A row of the day before that arrived late, packed on its own between this day's rows.
        insertRows(day - 50, 1, 7);
        insertRows(day + 30_000, 3000, 1);
        insertRows(now() - 5, 1, 1);
        pack();

        long late = count("SELECT id FROM co_segment WHERE day = " + (day / DAY - 1));
        long[] lateRange = range(List.of(late));
        assertEquals(lateRange[0], lateRange[1], "the late row is a segment of its own");
        byte[] lateDirectory = SegmentUnitStorageTest.directory(connection, late);

        SegmentCompactor.compactAll(connection, () -> {
        });

        assertEquals(1, count("SELECT COUNT(*) FROM co_segment WHERE id = " + late), "the other day's segment is left as it was");
        assertArrayEquals(lateDirectory, SegmentUnitStorageTest.directory(connection, late));
        Set<String> ranges = new HashSet<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT start_rowid, end_rowid FROM co_segment WHERE day = " + (day / DAY))) {
            while (results.next()) {
                assertFalse(results.getLong(1) <= lateRange[0] && lateRange[0] <= results.getLong(2), "no compacted segment claims the other day's row");
                ranges.add(results.getLong(1) + "-" + results.getLong(2));
            }
        }
        assertEquals(2, ranges.size(), "the rows either side of it are compacted as two runs");
        assertEquals(9001, count("SELECT SUM(row_count) FROM co_segment"));
    }

    @Test
    void aStoppedCompactionLeavesTheSegmentsItStartedFrom() throws Exception {
        writeDays(1, 20_000);
        keepPlainCopy();
        pack();
        Map<Long, String> rows = packedRows("block");
        List<Long> inputs = segmentIds();
        List<String> lookups = everyLookup();

        AtomicInteger calls = new AtomicInteger();
        assertThrows(InterruptedException.class, () -> SegmentCompactor.compactAll(connection, () -> {
            if (calls.incrementAndGet() == 2) {
                throw new InterruptedException("the server is stopping");
            }
        }));

        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE compacted < 0"), "what was written aside is removed");
        assertEquals(inputs, segmentIds(), "the segments it started from are all still there");
        assertEquals(inputs, SQLiteColdIndex.loadedSegmentIds());
        assertEquals(rows, packedRows("block"));
        assertEquals(lookups, everyLookup());

        SegmentCompactor.compactAll(connection, () -> {
        });
        assertEquals(rows, packedRows("block"), "and compacting again finishes the job");
        assertEquals(lookups, everyLookup());
    }

    @Test
    void whatAnInterruptedCompactionLeftBehindIsNeverReadAndIsRemovedNextTime() throws Exception {
        writeDays(1, 20_000);
        keepPlainCopy();
        pack();
        Map<Long, String> rows = packedRows("block");
        List<Long> inputs = segmentIds();
        List<String> lookups = everyLookup();
        long total = count("SELECT SUM(row_count) FROM co_segment");

        // As a compaction that stopped between writing a segment aside and swapping it in leaves it:
        // a full copy of rows that are still in the segments it was being merged from.
        long copy;
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO co_segment (table_id,start_rowid,end_rowid,row_count,min_time,max_time,day,wid_set,chunk_filter,user_filter,type_filter,action_bits,dict_id,"
                    + "codec_version,scalars,scalars_size,payload,payload_size,user_stats,type_stats,action_stats,spawn_filter,chunk_counts,compacted) SELECT table_id,start_rowid,end_rowid,"
                    + "row_count,min_time,max_time,day,wid_set,chunk_filter,user_filter,type_filter,action_bits,dict_id,codec_version,scalars,scalars_size,payload,payload_size,user_stats,"
                    + "type_stats,action_stats,spawn_filter,chunk_counts," + SegmentCompactor.WRITING + " FROM co_segment WHERE id = " + inputs.get(0));
            copy = count("SELECT MAX(id) FROM co_segment");
            statement.executeUpdate("INSERT INTO co_segment_unit (segment_id,unit,body,payload) SELECT " + copy + ",unit,body,payload FROM co_segment_unit WHERE segment_id = " + inputs.get(0));
        }
        SQLiteColdIndex.invalidate();
        SQLiteColdIndex.reload(connection);

        assertEquals(inputs, SQLiteColdIndex.loadedSegmentIds(), "a lookup never sees it");
        assertEquals(rows, packedRows("block"));
        assertEquals(lookups, everyLookup(), "so no row is found twice");
        ColdStorageStats stats = ColdStorageStats.read(connection);
        assertNotNull(stats);
        assertEquals(total, stats.getColdRows(), "nor counted twice");

        SegmentCompactor.compactAll(connection, () -> {
        });
        // Its id may well have been handed to a segment compaction wrote since, so what is checked
        // is that nothing is left waiting and every unit belongs to the segment its id names.
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE compacted < 0"), "the next compaction removes it");
        assertTrue(count("SELECT COUNT(*) FROM co_segment_unit WHERE segment_id = " + copy) == 0 || count("SELECT compacted FROM co_segment WHERE id = " + copy) == SegmentCompactor.COMPACTED);
        SegmentUnitStorageTest.assertUnitsMatchSegments(connection);
        assertEquals(rows, packedRows("block"));
        assertEquals(lookups, everyLookup());
    }

    @Test
    void aCompactionWhoseSegmentsWentMissingSwapsNothingIn() throws Exception {
        writeDays(1, 20_000);
        pack();
        List<Long> inputs = segmentIds();

        // A purge dropping one of the segments while the merge is under way.
        AtomicInteger calls = new AtomicInteger();
        assertThrows(SQLException.class, () -> SegmentCompactor.compactAll(connection, () -> {
            if (calls.incrementAndGet() == 1) {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("DELETE FROM co_segment WHERE id = " + inputs.get(3));
                }
            }
        }));

        List<Long> left = new ArrayList<>(inputs);
        left.remove(3);
        assertEquals(left, segmentIds(), "the other segments are left as they were, and nothing is swapped in");
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE compacted <> " + SegmentCompactor.PACKED));
    }

    @Test
    void aLookupInProgressSeesEveryRowOnceWhileCompactionRuns() throws Exception {
        writeDays(1, 20_000);
        pack();
        long total = count("SELECT SUM(row_count) FROM co_segment") + count("SELECT COUNT(*) FROM co_block");

        CountDownLatch looking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Long> lookup = CompletableFuture.supplyAsync(() -> {
            try (Connection reader = open()) {
                SQLiteColdIndex.beginLookup(0, 0);
                try {
                    String source = SQLiteColdIndex.sourceExpression(reader, "block", 0, null);
                    looking.countDown();
                    release.await(20, TimeUnit.SECONDS);
                    long first = count(reader, "SELECT COUNT(*) FROM " + source);
                    // A second read inside the same lookup has to agree with the first.
                    String again = SQLiteColdIndex.sourceExpression(reader, "block", 1, new Integer[] { 0, -100_000, 100_000, null, null, -100_000, 100_000 });
                    return first * 1_000_000L + count(reader, "SELECT COUNT(*) FROM " + again);
                }
                finally {
                    SQLiteColdIndex.endLookup(reader);
                }
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });
        assertTrue(looking.await(10, TimeUnit.SECONDS));

        CompletableFuture<Long> compaction = CompletableFuture.supplyAsync(() -> {
            try (Connection writer = open()) {
                return SegmentCompactor.compactAll(writer, () -> {
                });
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });

        Thread.sleep(600);
        assertFalse(compaction.isDone(), "compaction does not swap segments out from under the lookup");
        release.countDown();

        assertEquals(total * 1_000_000L + total, lookup.get(20, TimeUnit.SECONDS), "the lookup saw every row, once, both times");
        assertTrue(compaction.get(30, TimeUnit.SECONDS) > 0, "compaction went ahead once the lookup finished");
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE compacted <> " + SegmentCompactor.COMPACTED));

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            assertEquals(total, count(connection, "SELECT COUNT(*) FROM " + SQLiteColdIndex.sourceExpression(connection, "block", 0, null)), "a lookup afterwards sees every row, once");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void rewritingASegmentInPlaceWaitsForALookupInProgress() throws Exception {
        writeDays(1, 6000);
        keepPlainCopy();
        pack();
        List<Long> segments = segmentIds();
        for (long id : segments) {
            SegmentUnitStorageTest.joinUnits(connection, id);
        }
        SegmentFrameCache.clear();
        SQLiteColdIndex.reload(connection);
        RealisticActivity.Spot spot = activity.spots().get(activity.spots().size() / 2);
        List<String> expected = plainPlace(spot.x, spot.z, 3);

        CountDownLatch looking = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<List<String>> lookup = CompletableFuture.supplyAsync(() -> {
            try (Connection reader = open()) {
                SQLiteColdIndex.beginLookup(0, 0);
                try {
                    // The lookup has chosen its segments and holds their directories in memory.
                    SQLiteColdIndex.sourceExpression(reader, "block", 1, new Integer[] { 3, spot.x - 3, spot.x + 3, null, null, spot.z - 3, spot.z + 3 });
                    SQLiteColdIndex.releaseMaterialized(reader, "block");
                    looking.countDown();
                    release.await(20, TimeUnit.SECONDS);
                    String source = SQLiteColdIndex.sourceExpression(reader, "block", 1, new Integer[] { 3, spot.x - 3, spot.x + 3, null, null, spot.z - 3, spot.z + 3 });
                    return placeRows(reader, source, spot.x, spot.z, 3);
                }
                finally {
                    SQLiteColdIndex.endLookup(reader);
                }
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });
        assertTrue(looking.await(10, TimeUnit.SECONDS));

        CompletableFuture<Long> repack = CompletableFuture.supplyAsync(() -> {
            try (Connection writer = open()) {
                return ColdRollupTask.repackOldSegments(writer, () -> {
                }, 0);
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });

        Thread.sleep(600);
        assertFalse(repack.isDone(), "the rewrite waits for the lookup rather than change a segment it is reading");
        release.countDown();

        assertEquals(expected, lookup.get(20, TimeUnit.SECONDS), "the lookup found every row, once");
        assertEquals(segments.size(), repack.get(20, TimeUnit.SECONDS).longValue(), "and the rewrite went ahead once it finished");
        assertEquals(expected, plainPlace(spot.x, spot.z, 3));
        SegmentFrameCache.clear();
        assertEquals(expected, place(spot.x, spot.z, 3));
    }

    @Test
    void mergingHundredsOfSegmentsNeverHoldsMoreThanTheStatedRows() throws Exception {
        ColdRollupTask.setSegmentRowsForTesting(ColdSegmentCodec.GROUP_ROWS);
        SegmentCompactor.setTargetRowsForTesting(262_144);
        int rows = 300 * ColdSegmentCodec.GROUP_ROWS;
        activity.writeBlocksOver(connection, firstDay, rows, DAY - 1);
        insertRows(now() - 5, 1, 1);
        pack();
        assertEquals(300, count("SELECT COUNT(*) FROM co_segment"), "three hundred segments to compact");
        Map<Long, String> before = packedRows("block");

        SegmentCompactor.resetPeakHeldRows();
        long merged = SegmentCompactor.compactAll(connection, () -> {
        });

        long peak = SegmentCompactor.peakHeldRows();
        assertTrue(peak <= SegmentCompactor.MAXIMUM_HELD_ROWS, "held " + peak + " decoded rows, more than " + SegmentCompactor.MAXIMUM_HELD_ROWS);
        assertTrue(peak >= (long) SegmentCompactor.MAXIMUM_INPUTS * ColdSegmentCodec.GROUP_ROWS, "the bound is reached, so it is the one being measured: " + peak);
        assertTrue(merged > rows, "more inputs than can be merged at once are merged in passes, so some rows are merged twice");
        assertEquals(1, count("SELECT COUNT(DISTINCT start_rowid || '-' || end_rowid) FROM co_segment"), "into one run");
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE compacted <> " + SegmentCompactor.COMPACTED));
        assertEquals(before, packedRows("block"));
    }

    @Test
    void chatIsCompactedInOrderOfPlayerWithEveryMessageIntact() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_chat (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, message TEXT)");
        }
        SQLiteColdIndex.invalidate();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_chat (time,user,wid,x,y,z,message) VALUES (?,?,1,?,64,?,?)")) {
            for (int row = 0; row < 9000; row++) {
                statement.setLong(1, firstDay + row * 9L);
                statement.setInt(2, 1 + (row * 7) % 23);
                statement.setInt(3, row % 300);
                statement.setInt(4, row % 170);
                statement.setString(5, row % 11 == 0 ? "" : "message " + row + " éè " + "x".repeat(row % 40));
                statement.addBatch();
            }
            statement.setLong(1, now() - 5);
            statement.setInt(2, 1);
            statement.setInt(3, 0);
            statement.setInt(4, 0);
            statement.setString(5, "latest");
            statement.addBatch();
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
        long ofPlayerFive = count("SELECT COUNT(*) FROM co_chat WHERE user = 5");
        pack();
        assertTrue(count("SELECT COUNT(*) FROM co_segment WHERE table_id = " + SQLiteColdIndex.tableId("chat")) >= 6);
        Map<Long, String> before = packedRows("chat");

        SegmentCompactor.compactAll(connection, () -> {
        });

        assertEquals(before, packedRows("chat"), "every message keeps its id and its text");
        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "chat");
        long previousUser = Long.MIN_VALUE;
        for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, "chat", 0, null, 0, 0, null)) {
            ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
            for (int row = 0; row < rows.size(); row++) {
                long user = (Long) rows.getValues(row)[layout.userColumn];
                assertTrue(user >= previousUser, "chat is ordered by player across the whole day");
                previousUser = user;
            }
        }
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setLookupFilters("5", "");
            String source = SQLiteColdIndex.sourceExpression(connection, "chat", 0, null);
            assertEquals(ofPlayerFive, ids("SELECT rowid FROM " + source + " WHERE user = 5 ORDER BY rowid").size(), "a lookup of one player finds all of their messages");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void purgeDropsTheSegmentsOfACompactedRunTogether() throws Exception {
        writeDays(2, 12_000);
        pack();
        SegmentCompactor.compactAll(connection, () -> {
        });
        rollBackSomePackedRows();
        long secondDay = firstDay / DAY + 1;
        List<Long> kept = ids("SELECT id FROM co_segment WHERE day = " + secondDay + " ORDER BY id");
        assertTrue(kept.size() >= 2);
        long flags = count("SELECT COUNT(*) FROM co_cold_flag WHERE rowid_ref BETWEEN (SELECT MIN(start_rowid) FROM co_segment WHERE day = " + secondDay
                + ") AND (SELECT MAX(end_rowid) FROM co_segment WHERE day = " + secondDay + ")");
        assertTrue(flags > 0);

        // One segment of the second day ends before the cutoff while the others do not.
        long cutoff = (secondDay + 1) * DAY - 1000;
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE co_segment SET max_time = " + (cutoff - 10) + " WHERE id = " + kept.get(0));
        }
        SQLiteColdIndex.reload(connection);

        long removed = PurgeExecutor.purgeColdSegments(connection, cutoff, Connection::prepareStatement, () -> {
        });

        assertEquals(12_000, removed, "the first day is dropped whole");
        assertEquals(kept, ids("SELECT id FROM co_segment ORDER BY id"), "the second day's segments are all kept, as they share their rows' range");
        assertEquals(flags, count("SELECT COUNT(*) FROM co_cold_flag"), "and so is the rollback state of their rows");
        SegmentUnitStorageTest.assertUnitsMatchSegments(connection);
    }

    /**
     * Every kind of lookup that plans around segments, each checked against plain SQL, as one list
     * that must come out the same before and after compaction.
     */
    private List<String> everyLookup() throws Exception {
        List<String> answers = new ArrayList<>();
        List<RealisticActivity.Spot> spots = activity.spots();
        for (int index = 0; index < 25; index++) {
            RealisticActivity.Spot spot = spots.get((int) ((index * 7919L) % spots.size()));
            for (int radius : new int[] { 0, 6 }) {
                List<String> found = place(spot.x, spot.z, radius);
                assertEquals(plainPlace(spot.x, spot.z, radius), found, "place " + spot.x + "," + spot.z + " radius " + radius);
                answers.add(found.toString());
            }
        }

        for (long user = 1; user <= 4; user++) {
            List<Long> all = ids("SELECT id FROM co_block_all WHERE user = " + user + " ORDER BY id DESC");
            for (int offset : new int[] { 0, 3, 50, all.size() / 3, all.size() / 2, all.size() - 4, all.size() + 5 }) {
                List<Long> page = userPage(user, Math.max(0, offset), 7);
                assertEquals(slice(all, Math.max(0, offset), 7), page, "page of player " + user + " at " + offset);
                answers.add(page.toString());
            }
            List<Long> byTime = ids("SELECT id FROM co_block_all WHERE user IN(" + user + ") ORDER BY time DESC, id DESC LIMIT 7 OFFSET 20");
            List<Long> ranked = rankedPage(" user IN(" + user + ")", 27);
            assertEquals(byTime, ranked, "page of player " + user + " by time");
            answers.add(ranked.toString());
        }

        for (String users : new String[] { "2", "3,4" }) {
            long counted = countFromIndex(users, null);
            assertEquals(count("SELECT COUNT(*) FROM co_block_all WHERE user IN(" + users + ")"), counted, "count of " + users);
            answers.add(Long.toString(counted));
        }
        RealisticActivity.Spot spot = spots.get(spots.size() / 3);
        int chunkX = Math.floorDiv(spot.x, 16) * 16;
        int chunkZ = Math.floorDiv(spot.z, 16) * 16;
        Integer[] chunks = { 16, chunkX - 16, chunkX + 31, null, null, chunkZ - 16, chunkZ + 31 };
        long inChunks = countFromIndex(null, chunks);
        assertEquals(count("SELECT COUNT(*) FROM co_block_all WHERE wid = 1 AND x >= " + chunks[1] + " AND x <= " + chunks[2] + " AND z >= " + chunks[5] + " AND z <= " + chunks[6]),
                inChunks, "count of whole chunks");
        answers.add(Long.toString(inChunks));

        return answers;
    }

    private List<String> place(int x, int z, int radius) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, new Integer[] { radius, x - radius, x + radius, null, null, z - radius, z + radius });
            return placeRows(connection, source, x, z, radius);
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private List<String> plainPlace(int x, int z, int radius) throws SQLException {
        return placeRows(connection, "(SELECT id AS rowid, * FROM co_block_all)", x, z, radius);
    }

    private static List<String> placeRows(Connection connection, String source, int x, int z, int radius) throws SQLException {
        List<String> rows = new ArrayList<>();
        String query = "SELECT rowid,time,user,x,y,z,type,action,rolled_back,blockdata FROM " + source + " WHERE wid = 1 AND x >= " + (x - radius) + " AND x <= " + (x + radius)
                + " AND z >= " + (z - radius) + " AND z <= " + (z + radius) + " ORDER BY rowid";
        try (Statement statement = connection.createStatement(); ResultSet found = statement.executeQuery(query)) {
            while (found.next()) {
                byte[] blockdata = found.getBytes(10);
                rows.add(found.getLong(1) + ":" + found.getLong(2) + ":" + found.getInt(3) + ":" + found.getInt(4) + ":" + found.getInt(5) + ":" + found.getInt(6) + ":" + found.getInt(7)
                        + ":" + found.getInt(8) + ":" + found.getInt(9) + ":" + (blockdata == null ? "-" : new String(blockdata)));
            }
        }
        return rows;
    }

    /** A page of one player's rows, newest first, planned the way a lookup plans it. */
    private List<Long> userPage(long user, int offset, int size) throws SQLException {
        String clause = " user = " + user;
        ColdPagePlan plan = ColdPagePlan.plan(connection, List.of(new ColdPagePlan.Branch("block", 0, clause, 1, null)), false, "user", offset, size);
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setPredicateFilters(clause, "user");
            SQLiteColdIndex.setPageWindow("block", plan.window("block"));
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
            return ids("SELECT rowid FROM " + source + " WHERE" + clause + " ORDER BY rowid DESC LIMIT " + size);
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    /** The last seven rows of a budget, ordered by time, kept by the reader's own ranking. */
    private List<Long> rankedPage(String clause, long budget) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setRowBudget(budget);
            SQLiteColdIndex.setPageOrder("time DESC, id DESC");
            SQLiteColdIndex.setPredicateFilters(clause, "user");
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            return ids("SELECT rowid FROM " + source + " WHERE" + clause + " ORDER BY time DESC, rowid DESC LIMIT 7 OFFSET " + (budget - 7));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private long countFromIndex(String users, Integer[] bounds) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            StringBuilder where = new StringBuilder(" 1");
            if (users != null) {
                SQLiteColdIndex.setLookupFilters(users, "");
                where.append(" AND user IN(").append(users).append(')');
            }
            if (bounds != null) {
                where.append(" AND wid = 1 AND x >= ").append(bounds[1]).append(" AND x <= ").append(bounds[2]).append(" AND z >= ").append(bounds[5]).append(" AND z <= ").append(bounds[6]);
                SQLiteColdIndex.setPredicateFilters(where.toString(), "user");
            }
            SQLiteColdIndex.setCounting(true);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", bounds == null ? 0 : 1, bounds);
            return count("SELECT COUNT(*) FROM " + source + " WHERE" + where) + SQLiteColdIndex.countedRows("block");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    /** Checks each thing recorded about a segment against what its rows hold. */
    private void assertMetadataMatchesRows(long id, SQLiteColdIndex.TableLayout layout) throws SQLException {
        ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, SQLiteColdIndex.segmentById(connection, id));
        ColdRollupTask.Summary summary = new ColdRollupTask.Summary();
        List<Object[]> values = new ArrayList<>();
        for (int row = 0; row < rows.size(); row++) {
            summary.add(rows.getRowId(row), rows.getValues(row), layout);
            values.add(rows.getValues(row));
        }
        try (Statement statement = connection.createStatement();
                ResultSet stored = statement.executeQuery("SELECT row_count,min_time,max_time,day,wid_set,chunk_filter,user_filter,type_filter,action_bits,user_stats,type_stats,"
                        + "action_stats,spawn_filter,chunk_counts FROM co_segment WHERE id = " + id)) {
            assertTrue(stored.next());
            assertEquals(summary.rowCount, stored.getInt("row_count"));
            assertEquals(summary.minTime, stored.getLong("min_time"));
            assertEquals(summary.maxTime, stored.getLong("max_time"));
            assertEquals(summary.minTime / DAY, stored.getLong("day"));
            assertEquals(summary.worldIds, worlds(stored.getBytes("wid_set")));
            assertArrayEquals(SegmentMembership.encode(longs(summary.chunkKeys), SegmentFilter.CHUNK_BYTES), stored.getBytes("chunk_filter"));
            assertArrayEquals(SegmentMembership.encode(longs(summary.userIds)), stored.getBytes("user_filter"));
            assertArrayEquals(SegmentMembership.encode(longs(summary.typeIds)), stored.getBytes("type_filter"));
            assertEquals(summary.actionBits, stored.getLong("action_bits"));
            assertArrayEquals(SegmentStatistics.encode(summary.userCounts), stored.getBytes("user_stats"));
            assertArrayEquals(SegmentStatistics.encode(summary.typeCounts), stored.getBytes("type_stats"));
            assertArrayEquals(SegmentStatistics.encode(summary.actionCounts), stored.getBytes("action_stats"));
            assertArrayEquals(SegmentMembership.encode(longs(summary.spawnIds)), stored.getBytes("spawn_filter"));
            assertArrayEquals(ColdSegmentCodec.chunkCounts(values, layout.chunkColumns()), stored.getBytes("chunk_counts"));
        }
    }

    private Map<Long, Long> userCounts(List<Long> segments) throws SQLException {
        Map<Long, Long> counts = new TreeMap<>();
        for (long id : segments) {
            SegmentStatistics stats;
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT user_stats FROM co_segment WHERE id = " + id)) {
                results.next();
                stats = SegmentStatistics.decode(results.getBytes(1));
            }
            assertNotNull(stats);
            for (long user = 1; user <= 40; user++) {
                counts.merge(user, (long) stats.count(user), Long::sum);
            }
        }
        return counts;
    }

    private static Set<Integer> worlds(byte[] encoded) {
        Set<Integer> worlds = new HashSet<>();
        for (int offset = 0; encoded != null && offset + 3 < encoded.length; offset += 4) {
            worlds.add(((encoded[offset] & 0xFF) << 24) | ((encoded[offset + 1] & 0xFF) << 16) | ((encoded[offset + 2] & 0xFF) << 8) | (encoded[offset + 3] & 0xFF));
        }
        return worlds;
    }

    private static long[] longs(Set<Long> values) {
        long[] array = new long[values.size()];
        int index = 0;
        for (long value : values) {
            array[index++] = value;
        }
        return array;
    }

    /** Every packed row of a table, by row id, with every value spelled out, blob bytes included. */
    private Map<Long, String> packedRows(String table) throws SQLException {
        SQLiteColdIndex.reload(connection);
        Map<Long, String> rows = new TreeMap<>();
        for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, table, 0, null, 0, 0, null)) {
            ColdSegmentCodec.Rows decoded = SQLiteColdIndex.readRows(connection, segment);
            for (int row = 0; row < decoded.size(); row++) {
                Object[] values = decoded.getValues(row);
                StringBuilder spelled = new StringBuilder("[");
                for (int column = 0; column < values.length; column++) {
                    spelled.append(column > 0 ? ", " : "").append(values[column] instanceof byte[] ? Arrays.toString((byte[]) values[column]) : String.valueOf(values[column]));
                }
                assertTrue(rows.put(decoded.getRowId(row), spelled.append(']').toString()) == null, "row " + decoded.getRowId(row) + " is in one segment only");
            }
        }
        return rows;
    }

    private Map<Long, List<Long>> segmentsByDay() throws SQLException {
        Map<Long, List<Long>> days = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT day, id FROM co_segment WHERE compacted >= 0 ORDER BY day, id")) {
            while (results.next()) {
                days.computeIfAbsent(results.getLong(1), key -> new ArrayList<>()).add(results.getLong(2));
            }
        }
        return days;
    }

    private long[] range(List<Long> segments) throws SQLException {
        String ids = segments.toString().replace('[', '(').replace(']', ')');
        return new long[] { count("SELECT MIN(start_rowid) FROM co_segment WHERE id IN " + ids), count("SELECT MAX(end_rowid) FROM co_segment WHERE id IN " + ids) };
    }

    private List<Long> segmentIds() throws SQLException {
        return ids("SELECT id FROM co_segment ORDER BY id");
    }

    /** Several days of activity, then one row of now so that every row of those days is packed. */
    private void writeDays(int days, int rowsPerDay) throws SQLException {
        for (int day = 0; day < days; day++) {
            activity.writeBlocksOver(connection, firstDay + day * DAY, rowsPerDay, DAY - 1);
        }
        insertRows(now() - 5, 1, 1);
    }

    private void insertRows(long firstTime, int rows, int user) throws SQLException {
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,5,0,NULL,?,1,0)")) {
            for (int row = 0; row < rows; row++) {
                statement.setLong(1, firstTime + row);
                statement.setInt(2, user);
                statement.setInt(3, row % 200);
                statement.setInt(4, (row * 3) % 200);
                statement.setBytes(5, row % 4 == 0 ? ("minecraft:stone[" + row + "]").getBytes() : null);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void keepPlainCopy() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block_all AS SELECT rowid AS id, * FROM co_block");
            statement.executeUpdate("CREATE INDEX co_block_all_place ON co_block_all(wid,x,z)");
            statement.executeUpdate("CREATE INDEX co_block_all_user ON co_block_all(user,id)");
        }
    }

    private void pack() throws Exception {
        ColdRollupTask.rollUp(connection, () -> {
        });
        SQLiteColdIndex.reload(connection);
    }

    /** Rolls back every thirteenth packed row, which records the state beside its segment. */
    private void rollBackSomePackedRows() throws SQLException {
        long highWater = SQLiteColdIndex.coldHighWaterMark("block");
        List<Long> rowIds = new ArrayList<>();
        for (long rowId = 13; rowId <= highWater; rowId += 13) {
            rowIds.add(rowId);
        }
        try (Statement statement = connection.createStatement()) {
            Database.performRolledBackUpdateChecked(statement, 1, rowIds, 0);
            if (count("SELECT COUNT(*) FROM sqlite_master WHERE name = 'co_block_all'") > 0) {
                statement.executeUpdate("UPDATE co_block_all SET rolled_back = 1 WHERE id % 13 = 0 AND id <= " + highWater);
            }
        }
        assertTrue(count("SELECT COUNT(*) FROM co_cold_flag") > 0, "the state is recorded beside the segments");
    }

    private static <T> List<T> slice(List<T> list, int from, int size) {
        if (from >= list.size()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(list.subList(from, Math.min(list.size(), from + size)));
    }

    private Connection open() throws SQLException {
        Connection opened = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = opened.createStatement()) {
            statement.executeUpdate("PRAGMA busy_timeout=10000");
        }
        return opened;
    }

    private static long now() {
        return System.currentTimeMillis() / 1000L;
    }

    private List<Long> ids(String query) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                ids.add(results.getLong(1));
            }
        }
        return ids;
    }

    private long count(String query) throws SQLException {
        return count(connection, query);
    }

    private static long count(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
