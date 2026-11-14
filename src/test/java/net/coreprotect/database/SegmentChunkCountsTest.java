package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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

import com.github.luben.zstd.Zstd;

import net.coreprotect.config.ConfigHandler;

/**
 * The exact rows per chunk each segment records: they skip segments the chunk filter cannot rule
 * out, they count lookups over whole chunks without reading a row, and segments written before
 * them gain them without anything a lookup sees changing.
 */
class SegmentChunkCountsTest {

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
    void aSegmentTheChunkFilterWouldOpenIsSkippedByItsCounts() throws Exception {
        // A chunk to every row, twenty thousand of them, so the segment's chunk filter is a Bloom
        // filter full enough to say "maybe" about most chunks it does not hold.
        long base = ((System.currentTimeMillis() / 1000L) / 86400L - 5) * 86400L;
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,1,1,?,64,?,10,0,NULL,NULL,0,0)")) {
            for (int row = 0; row < 20_000; row++) {
                statement.setLong(1, base + row);
                statement.setInt(2, row * 16);
                statement.setInt(3, row * 16);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
        ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        SQLiteColdIndex.reload(connection);
        assertEquals(1, count("SELECT COUNT(*) FROM co_segment"));

        SegmentMembership filter;
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT chunk_filter FROM co_segment")) {
            results.next();
            filter = SegmentMembership.decode(results.getBytes(1));
        }
        assertFalse(filter.isExact(), "the segment covers too many chunks to list them");

        // A chunk the segment holds no row in, that its filter still lets through.
        int absent = -1;
        for (int chunk = 0; chunk < 20_000 && absent < 0; chunk++) {
            if (filter.mightContain(SegmentFilter.chunkKey(1, chunk, chunk + 1))) {
                absent = chunk;
            }
        }
        assertTrue(absent >= 0, "a full filter says maybe about some chunk it does not hold");
        Integer[] nowhere = { 0, absent * 16, absent * 16 + 3, null, null, (absent + 1) * 16, (absent + 1) * 16 + 3 };
        assertTrue(SQLiteColdIndex.selectSegments(connection, "block", 1, nowhere, 0, 0, null).isEmpty(), "the counts know the chunk is empty");
        Integer[] somewhere = { 0, 500 * 16, 500 * 16, null, null, 500 * 16, 500 * 16 };
        assertEquals(1, SQLiteColdIndex.selectSegments(connection, "block", 1, somewhere, 0, 0, null).size());

        long reads = ColdSegmentReadCounter.frameReads();
        assertEquals(0, lookUp(1, nowhere, "x >= " + nowhere[1] + " AND x <= " + nowhere[2] + " AND z >= " + nowhere[5] + " AND z <= " + nowhere[6]));
        assertEquals(reads, ColdSegmentReadCounter.frameReads(), "a lookup about an empty chunk reads no segment");
        assertEquals(1, lookUp(1, somewhere, "x = " + (500 * 16) + " AND z = " + (500 * 16)));

        // A segment written before the counts existed still has its filter to go on.
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE co_segment SET chunk_counts = NULL");
        }
        SQLiteColdIndex.reload(connection);
        assertEquals(1, SQLiteColdIndex.selectSegments(connection, "block", 1, nowhere, 0, 0, null).size(), "without counts the filter decides, and says maybe");
        assertEquals(0, lookUp(1, nowhere, "x >= " + nowhere[1] + " AND x <= " + nowhere[2] + " AND z >= " + nowhere[5] + " AND z <= " + nowhere[6]));
    }

    @Test
    void aCountOverWholeChunksIsAnsweredWithoutReadingAndMatchesTheRows() throws Exception {
        RealisticActivity activity = sealRealisticActivity();

        int checked = 0;
        for (RealisticActivity.Spot spot : activity.spots().subList(0, 40)) {
            // Three chunks by three around the spot, edge to edge.
            int minX = (SegmentFilter.chunkOf(spot.x) - 1) * 16;
            int minZ = (SegmentFilter.chunkOf(spot.z) - 1) * 16;
            Integer[] bounds = { 0, minX, minX + 47, null, null, minZ, minZ + 47 };
            String where = "wid = 1 AND x >= " + minX + " AND x <= " + (minX + 47) + " AND z >= " + minZ + " AND z <= " + (minZ + 47);
            long expected = count("SELECT COUNT(*) FROM co_block_all WHERE " + where);

            long reads = ColdSegmentReadCounter.reads();
            assertEquals(expected, countThroughIndex(bounds, where), "the count over whole chunks at " + minX + "," + minZ);
            assertEquals(reads, ColdSegmentReadCounter.reads(), "a count over whole chunks decodes nothing");

            // Bounds that cut across chunks cannot be counted from the chunks, and still count right.
            Integer[] partial = { 0, spot.x - 3, spot.x + 3, null, null, spot.z - 3, spot.z + 3 };
            String partialWhere = "wid = 1 AND x >= " + (spot.x - 3) + " AND x <= " + (spot.x + 3) + " AND z >= " + (spot.z - 3) + " AND z <= " + (spot.z + 3);
            assertEquals(count("SELECT COUNT(*) FROM co_block_all WHERE " + partialWhere), countThroughIndex(partial, partialWhere));
            checked++;
        }
        assertEquals(40, checked);
    }

    @Test
    void aPageDeepIntoWholeChunksSkipsSegmentsByTheirCounts() throws Exception {
        RealisticActivity activity = sealRealisticActivity();

        // The area around a spot whose rows are spread over the most segments, so a page among its
        // oldest rows lies below several segments that can be skipped.
        List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null);
        int minX = 0;
        int minZ = 0;
        int widest = -1;
        for (RealisticActivity.Spot spot : activity.spots()) {
            int chunkX = SegmentFilter.chunkOf(spot.x) - 2;
            int chunkZ = SegmentFilter.chunkOf(spot.z) - 2;
            int holding = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                if (segment.getChunkCounts().rowsIn(1, chunkX, chunkX + 4, chunkZ, chunkZ + 4) > 0) {
                    holding++;
                }
            }
            if (holding > widest) {
                widest = holding;
                minX = chunkX * 16;
                minZ = chunkZ * 16;
            }
        }
        assertTrue(widest >= 2, "some area has rows in several segments");
        long oldest = 0;
        for (SQLiteColdIndex.ColdSegment segment : all) {
            long rows = segment.getChunkCounts().rowsIn(1, SegmentFilter.chunkOf(minX), SegmentFilter.chunkOf(minX) + 4, SegmentFilter.chunkOf(minZ), SegmentFilter.chunkOf(minZ) + 4);
            if (rows > 0) {
                oldest = rows;
                break;
            }
        }
        String where = "wid = 1 AND x >= " + minX + " AND x <= " + (minX + 79) + " AND z >= " + minZ + " AND z <= " + (minZ + 79);
        Integer[] bounds = { 0, minX, minX + 79, null, null, minZ, minZ + 79 };
        long total = count("SELECT COUNT(*) FROM co_block_all WHERE " + where);

        // A page within the oldest segment holding rows there.
        int offset = (int) (total - Math.max(1, oldest / 2));
        List<Long> expected = ids("SELECT id FROM co_block_all WHERE " + where + " ORDER BY id DESC LIMIT 10 OFFSET " + offset);
        long readsBefore = ColdSegmentReadCounter.reads();
        ColdPagePlan plan = ColdPagePlan.plan(connection, List.of(new ColdPagePlan.Branch("block", 0, " " + where, 1, bounds)), false, "user", offset, 10);
        long planningReads = ColdSegmentReadCounter.reads() - readsBefore;
        SQLiteColdIndex.beginLookup(0, 0);
        List<Long> actual;
        try {
            SQLiteColdIndex.setPredicateFilters(" " + where, "user");
            SQLiteColdIndex.setPageWindow("block", plan.window("block"));
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, bounds);
            actual = ids("SELECT rowid FROM " + source + " WHERE " + where + " ORDER BY rowid DESC LIMIT 10");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
        assertTrue(planningReads < widest, "segments above the page are counted from their records, not read: read " + planningReads + " of " + widest);
        assertEquals(expected, actual);
    }

    @Test
    void segmentsWithoutCountsGainThemFromTheBackfill() throws Exception {
        RealisticActivity activity = new RealisticActivity(3, 30);
        long first = ((System.currentTimeMillis() / 1000L) / 86400L - 5) * 86400L;
        activity.writeBlocks(connection, first, 150_000, 1);
        ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        SQLiteColdIndex.reload(connection);

        Map<Long, byte[]> written = new HashMap<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT id, chunk_counts FROM co_segment")) {
            while (results.next()) {
                assertNotNull(results.getBytes(2), "a segment records its chunks when it is written");
                written.put(results.getLong(1), results.getBytes(2));
            }
        }
        assertTrue(written.size() >= 2);

        // One segment as the first format wrote it, and none with counts, as builds before them left
        // them.
        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "block");
        SQLiteColdIndex.ColdSegment oldest = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null).get(0);
        ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, oldest);
        long[] ids = new long[rows.size()];
        List<Object[]> values = new ArrayList<>();
        for (int row = 0; row < rows.size(); row++) {
            ids[row] = rows.getRowId(row);
            values.add(rows.getValues(row));
        }
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encodeVersionOne(layout.types, ids, values);
        try (PreparedStatement statement = connection.prepareStatement("UPDATE co_segment SET scalars = ?, scalars_size = ?, payload = ?, payload_size = ?, dict_id = 0, codec_version = 1 WHERE id = ?")) {
            statement.setBytes(1, Zstd.compress(frames.getScalars(), 3));
            statement.setInt(2, frames.getScalars().length);
            statement.setBytes(3, frames.getPayload().length == 0 ? null : Zstd.compress(frames.getPayload(), 3));
            statement.setInt(4, frames.getPayload().length);
            statement.setLong(5, oldest.getId());
            statement.executeUpdate();
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_segment_unit WHERE segment_id = " + oldest.getId());
            statement.executeUpdate("UPDATE co_segment SET chunk_counts = NULL");
        }
        SegmentFrameCache.clear();
        SQLiteColdIndex.reload(connection);

        long filled = ColdRollupTask.backfillStatistics(connection, () -> {
        });
        assertEquals(written.size(), filled, "every segment without counts is counted");
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT id, chunk_counts FROM co_segment")) {
            while (results.next()) {
                assertArrayEquals(written.get(results.getLong(1)), results.getBytes(2), "segment " + results.getLong(1) + " counts the same chunks it was written with");
            }
        }
        for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null)) {
            assertNotNull(segment.getChunkCounts(), "lookups use the counts as soon as they are filled");
        }
        assertEquals(0, ColdRollupTask.backfillStatistics(connection, () -> {
        }), "nothing is counted twice");
    }

    /** Writes a realistic history, keeps an unpacked copy of it, and packs all of it. */
    private RealisticActivity sealRealisticActivity() throws Exception {
        RealisticActivity activity = new RealisticActivity(7, 30);
        long first = ((System.currentTimeMillis() / 1000L) / 86400L - 6) * 86400L;
        activity.writeBlocks(connection, first, 200_000, 1);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block_all AS SELECT rowid AS id, * FROM co_block");
            statement.executeUpdate("CREATE INDEX co_block_all_place ON co_block_all(wid,x,z)");
        }
        ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        SQLiteColdIndex.reload(connection);
        assertTrue(count("SELECT COUNT(*) FROM co_segment") >= 3);
        return activity;
    }

    /** Counts a lookup the way a lookup reports it: the packed rows counted, plus the live rows. */
    private long countThroughIndex(Integer[] bounds, String where) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            SQLiteColdIndex.setPredicateFilters(where, "user");
            SQLiteColdIndex.setCounting(true);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, bounds);
            return count("SELECT COUNT(*) FROM " + source + " WHERE " + where) + SQLiteColdIndex.countedRows("block");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private long lookUp(int worldId, Integer[] bounds, String where) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", worldId, bounds);
            return count("SELECT COUNT(*) FROM " + source + " WHERE wid = " + worldId + " AND " + where);
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

    private List<Long> ids(String query) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                ids.add(results.getLong(1));
            }
        }
        return ids;
    }
}
