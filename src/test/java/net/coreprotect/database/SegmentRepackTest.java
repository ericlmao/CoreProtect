package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * Segments written by older builds are rewritten in the current format, and nothing a lookup can
 * see changes when they are.
 */
class SegmentRepackTest {

    private Connection connection;
    private DatabaseType previousType;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        RealisticActivity.createBlockTable(connection);
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void oldSegmentsAreRewrittenWithoutChangingWhatLookupsSee() throws Exception {
        RealisticActivity activity = new RealisticActivity(3, 30);
        long first = ((System.currentTimeMillis() / 1000L) / 86400L - 5) * 86400L;
        activity.writeBlocks(connection, first, 150_000, 1);
        ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        int segments = (int) count("SELECT COUNT(*) FROM co_segment");
        assertTrue(segments >= 2);

        downgradeToFirstFormat();
        SQLiteColdIndex.reload(connection);
        try (Statement statement = connection.createStatement()) {
            Database.performRolledBackUpdateChecked(statement, 1, java.util.Arrays.asList(5L, 70_000L), 0);
        }

        List<String> spots = new ArrayList<>();
        for (RealisticActivity.Spot spot : activity.spots().subList(0, 30)) {
            spots.add(spot.x + "," + spot.z);
        }
        List<List<String>> before = lookUp(spots);
        long everythingBefore = countAll();

        long rewritten = ColdRollupTask.repackOldSegments(connection, () -> {
        }, 0);

        assertEquals(segments, rewritten, "every old segment is rewritten");
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE codec_version < " + ColdSegmentCodec.CODEC_VERSION));
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE chunk_counts IS NULL"), "every rewritten segment records its rows per chunk");
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT scalars FROM co_segment")) {
            while (results.next()) {
                assertEquals(ColdSegmentCodec.CODEC_VERSION, results.getBytes(1)[2], "the scalar frame is written in the current format");
            }
        }
        for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null)) {
            assertTrue(segment.getChunkCounts() != null, "lookups use the counts of a rewritten segment straight away");
        }
        assertEquals(before, lookUp(spots), "lookups about a place see exactly the same rows, in the same state");
        assertEquals(everythingBefore, countAll());
        SegmentUnitStorageTest.assertUnitsMatchSegments(connection);
        assertEquals(0, ColdRollupTask.repackOldSegments(connection, () -> {
        }, 0), "nothing is rewritten twice");
    }

    @Test
    void segmentsWithTheirUnitsInLineAreSplitWithoutChangingWhatLookupsSee() throws Exception {
        RealisticActivity activity = new RealisticActivity(5, 30);
        long first = ((System.currentTimeMillis() / 1000L) / 86400L - 5) * 86400L;
        activity.writeBlocks(connection, first, 150_000, 1);
        ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        SQLiteColdIndex.reload(connection);
        List<Long> ids = new ArrayList<>();
        Map<Long, byte[]> payloads = new HashMap<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT id, payload FROM co_segment")) {
            while (results.next()) {
                ids.add(results.getLong(1));
                payloads.put(results.getLong(1), results.getBytes(2));
            }
        }
        assertTrue(ids.size() >= 2);
        for (long id : ids) {
            SegmentUnitStorageTest.joinUnits(connection, id);
        }
        SegmentFrameCache.clear();
        SQLiteColdIndex.reload(connection);
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment_unit"), "every segment holds its units in line, as version 3 wrote it");

        List<String> spots = new ArrayList<>();
        for (RealisticActivity.Spot spot : activity.spots().subList(0, 30)) {
            spots.add(spot.x + "," + spot.z);
        }
        long unitReads = ColdSegmentReadCounter.unitReads();
        List<List<String>> before = lookUp(spots);
        assertEquals(unitReads, ColdSegmentReadCounter.unitReads(), "a segment with its units in line reads no unit rows");
        long everythingBefore = countAll();

        long rewritten = ColdRollupTask.repackOldSegments(connection, () -> {
        }, 0);

        assertEquals(ids.size(), rewritten, "every segment with its units in line is split");
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE codec_version < " + ColdSegmentCodec.CODEC_VERSION));
        SegmentUnitStorageTest.assertUnitsMatchSegments(connection);
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT id, scalars, payload FROM co_segment")) {
            while (results.next()) {
                assertTrue(ColdSegmentCodec.isSplit(results.getBytes(2)), "the segment row keeps the directory alone");
                assertArrayEquals(payloads.get(results.getLong(1)), results.getBytes(3), "the payload is left as it was");
            }
        }
        SegmentFrameCache.clear();
        unitReads = ColdSegmentReadCounter.unitReads();
        assertEquals(before, lookUp(spots), "lookups about a place see exactly the same rows, in the same state");
        assertTrue(ColdSegmentReadCounter.unitReads() > unitReads, "and read them from the unit rows");
        assertEquals(everythingBefore, countAll());
        assertEquals(0, ColdRollupTask.repackOldSegments(connection, () -> {
        }, 0), "nothing is split twice");
    }

    /** Rewrites every segment the way builds before groups wrote them. */
    private void downgradeToFirstFormat() throws Exception {
        SQLiteColdIndex.reload(connection);
        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "block");
        for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null)) {
            SegmentUnitStorageTest.rewriteAsVersionOne(connection, layout, segment);
        }
        SegmentFrameCache.clear();
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment_unit"));
    }

    private List<List<String>> lookUp(List<String> spots) throws SQLException {
        List<List<String>> results = new ArrayList<>();
        for (String spot : spots) {
            int x = Integer.parseInt(spot.split(",")[0]);
            int z = Integer.parseInt(spot.split(",")[1]);
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, new Integer[] { 0, x - 3, x + 3, null, null, z - 3, z + 3 });
                List<String> rows = new ArrayList<>();
                try (Statement statement = connection.createStatement();
                        ResultSet found = statement.executeQuery("SELECT rowid,time,user,x,y,z,type,action,rolled_back,blockdata FROM " + source
                                + " WHERE wid = 1 AND x >= " + (x - 3) + " AND x <= " + (x + 3) + " AND z >= " + (z - 3) + " AND z <= " + (z + 3) + " ORDER BY rowid")) {
                    while (found.next()) {
                        byte[] blockdata = found.getBytes(10);
                        rows.add(found.getLong(1) + ":" + found.getLong(2) + ":" + found.getInt(3) + ":" + found.getInt(4) + ":" + found.getInt(5) + ":" + found.getInt(6) + ":"
                                + found.getInt(7) + ":" + found.getInt(8) + ":" + found.getInt(9) + ":" + (blockdata == null ? "-" : new String(blockdata)));
                    }
                }
                results.add(rows);
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
        }
        return results;
    }

    private long countAll() throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            return count("SELECT COUNT(*) FROM " + SQLiteColdIndex.sourceExpression(connection, "block", 0, null) + " WHERE rolled_back = 1");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
