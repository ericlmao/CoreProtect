package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.luben.zstd.Zstd;

import net.coreprotect.config.ConfigHandler;

/**
 * Segments store their unit bodies apart from their directory, and a lookup reads only the units
 * holding what it wants, while every lookup still finds exactly the rows plain SQL finds.
 */
class SegmentUnitStorageTest {

    private static final long DAY = 86400L;

    private Connection connection;
    private DatabaseType previousType;
    private RealisticActivity activity;
    private long firstDay;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws Exception {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        RealisticActivity.createBlockTable(connection);

        activity = new RealisticActivity(7, 30);
        firstDay = ((System.currentTimeMillis() / 1000L) / DAY - 5) * DAY;
        activity.writeBlocks(connection, firstDay, 150_000, 1);
        try (Statement statement = connection.createStatement()) {
            // Every row as written, unpacked, for plain SQL to answer from.
            statement.executeUpdate("CREATE TABLE co_block_all AS SELECT rowid AS id, * FROM co_block");
            statement.executeUpdate("CREATE INDEX co_block_all_place ON co_block_all(wid,x,z)");
        }
        ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        SQLiteColdIndex.reload(connection);
        assertTrue(count("SELECT COUNT(*) FROM co_segment") >= 3);
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
    void everySegmentIsWrittenAsADirectoryWithItsUnitsBesideIt() throws Exception {
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE codec_version <> " + ColdSegmentCodec.CODEC_VERSION));
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT scalars, scalars_size FROM co_segment")) {
            while (results.next()) {
                assertTrue(ColdSegmentCodec.isSplit(results.getBytes(1)), "the segment row holds the directory alone");
                assertEquals(results.getBytes(1).length, results.getInt(2));
            }
        }
        assertUnitsMatchSegments(connection);
    }

    @Test
    void aPlaceLookupReadsOnlyTheUnitsHoldingItsGroupsAndFindsWhatSqlFinds() throws Exception {
        long unitsRead = 0;
        long segmentsRead = 0;
        long found = 0;
        for (RealisticActivity.Spot spot : spread(40)) {
            SegmentFrameCache.clear();
            long units = ColdSegmentReadCounter.unitReads();
            long opened = ColdSegmentReadCounter.unitsOpened();
            long frames = ColdSegmentReadCounter.frameReads();

            List<String> actual = lookUp(connection, spot.x, spot.z, 0);
            assertEquals(expected(spot.x, spot.z, 0), actual, "lookup at " + spot.x + "," + spot.z);

            assertEquals(ColdSegmentReadCounter.unitsOpened() - opened, ColdSegmentReadCounter.unitReads() - units,
                    "the units read are exactly the units the decoder opens, at " + spot.x + "," + spot.z);
            unitsRead += ColdSegmentReadCounter.unitReads() - units;
            segmentsRead += ColdSegmentReadCounter.frameReads() - frames;
            found += actual.size();
        }
        assertTrue(found > 0);
        assertTrue(unitsRead > 0, "the lookups read units");
        long unitsPerSegment = count("SELECT MAX(units) FROM (SELECT COUNT(*) AS units FROM co_segment_unit GROUP BY segment_id)");
        assertTrue(unitsRead * 3 < segmentsRead * unitsPerSegment,
                "a lookup about one block reads " + unitsRead + " units of the " + segmentsRead * unitsPerSegment + " in the segments it opens");
    }

    @Test
    void aSecondIdenticalLookupReadsNothingFromTheDatabase() throws Exception {
        RealisticActivity.Spot spot = activity.spots().get(activity.spots().size() / 3);
        SegmentFrameCache.clear();
        long units = ColdSegmentReadCounter.unitReads();
        List<String> first = lookUp(connection, spot.x, spot.z, 3);
        assertTrue(ColdSegmentReadCounter.unitReads() > units, "the first lookup reads units");

        units = ColdSegmentReadCounter.unitReads();
        long frames = ColdSegmentReadCounter.frameReads();
        long bytes = ColdSegmentReadCounter.bytesRead();
        List<String> second = lookUp(connection, spot.x, spot.z, 3);
        assertEquals(first, second);
        assertEquals(units, ColdSegmentReadCounter.unitReads(), "the units are already in memory");
        assertEquals(frames, ColdSegmentReadCounter.frameReads(), "and so are the directories");
        assertEquals(bytes, ColdSegmentReadCounter.bytesRead(), "nothing is read from the database");
    }

    @Test
    void aFullReadFetchesEveryUnit() throws Exception {
        for (SQLiteColdIndex.ColdSegment segment : SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null)) {
            int unitCount = ColdSegmentCodec.unitCount(directory(connection, segment.getId()));

            long units = ColdSegmentReadCounter.unitReads();
            ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
            assertEquals(segment.getRowCount(), rows.size());
            assertEquals(unitCount, ColdSegmentReadCounter.unitReads() - units, "reading a whole segment reads every unit");

            units = ColdSegmentReadCounter.unitReads();
            int[] walked = new int[1];
            SQLiteColdIndex.scanRows(connection, segment, (columns, present) -> {
                walked[0]++;
                return false;
            });
            assertEquals(segment.getRowCount(), walked[0]);
            assertEquals(unitCount, ColdSegmentReadCounter.unitReads() - units, "and so does walking it");
        }
    }

    @Test
    void segmentsOfEveryLayoutAreReadSideBySide() throws Exception {
        List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null);
        SQLiteColdIndex.TableLayout layout = SQLiteColdIndex.layout(connection, "block");
        rewriteAsVersionOne(connection, layout, all.get(0));
        joinUnits(connection, all.get(1).getId());
        SegmentFrameCache.clear();
        SQLiteColdIndex.reload(connection);
        assertEquals(1, count("SELECT COUNT(*) FROM co_segment WHERE codec_version = 1"));
        assertEquals(1, count("SELECT COUNT(*) FROM co_segment WHERE codec_version = 3"));
        assertUnitsMatchSegments(connection);

        for (RealisticActivity.Spot spot : spread(40)) {
            assertEquals(expected(spot.x, spot.z, 3), lookUp(connection, spot.x, spot.z, 3), "lookup at " + spot.x + "," + spot.z);
        }
    }

    @Test
    void purgingADayRemovesItsUnitRows() throws Exception {
        long cutoff = firstDay + DAY;
        List<Long> expired = ids("SELECT id FROM co_segment WHERE max_time < " + cutoff);
        assertTrue(!expired.isEmpty(), "the first day is packed");
        long kept = count("SELECT COUNT(*) FROM co_segment") - expired.size();
        assertTrue(kept > 0, "later days are packed too");
        String expiredIds = expired.toString().replace('[', '(').replace(']', ')');
        assertTrue(count("SELECT COUNT(*) FROM co_segment_unit WHERE segment_id IN " + expiredIds) > 0);

        long removed = PurgeExecutor.purgeColdSegments(connection, cutoff, Connection::prepareStatement, () -> {
        });

        assertTrue(removed > 0);
        assertEquals(kept, count("SELECT COUNT(*) FROM co_segment"));
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment_unit WHERE segment_id IN " + expiredIds), "the dropped segments' units go with them");
        assertUnitsMatchSegments(connection);
        for (RealisticActivity.Spot spot : spread(10)) {
            assertEquals(expected(spot.x, spot.z, 3, cutoff), lookUp(connection, spot.x, spot.z, 3), "what is left still reads");
        }
    }

    @Test
    void storageStatisticsCountTheUnitRows() throws Exception {
        ColdStorageStats stats = ColdStorageStats.read(connection);
        assertNotNull(stats);
        long segmentRows = count("SELECT SUM(LENGTH(scalars) + LENGTH(COALESCE(payload, x''))) FROM co_segment");
        long unitRows = count("SELECT SUM(LENGTH(body)) FROM co_segment_unit");
        long blobGroups = count("SELECT COALESCE(SUM(LENGTH(data)),0) FROM co_blob_group");
        assertTrue(unitRows > count("SELECT SUM(LENGTH(scalars)) FROM co_segment"), "the unit rows hold most of the scalar data");
        assertEquals(segmentRows + unitRows + blobGroups, stats.getColdBytes());
    }

    /**
     * Checks that every segment stored with its units apart has exactly the units its directory
     * describes, numbered from zero, that a segment with its bodies in line has none, and that no
     * unit row belongs to a segment that is gone.
     */
    static void assertUnitsMatchSegments(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM co_segment_unit WHERE segment_id NOT IN (SELECT id FROM co_segment)")) {
            results.next();
            assertEquals(0, results.getLong(1), "no unit row outlives its segment");
        }
        List<long[]> segments = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT id FROM co_segment")) {
            while (results.next()) {
                segments.add(new long[] { results.getLong(1) });
            }
        }
        for (long[] segment : segments) {
            byte[] directory = directory(connection, segment[0]);
            List<Long> units = new ArrayList<>();
            try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT unit FROM co_segment_unit WHERE segment_id = " + segment[0] + " ORDER BY unit")) {
                while (results.next()) {
                    units.add(results.getLong(1));
                }
            }
            int expected = ColdSegmentCodec.isSplit(directory) ? ColdSegmentCodec.unitCount(directory) : 0;
            assertEquals(expected, units.size(), "segment " + segment[0] + " has the units its directory describes");
            for (int unit = 0; unit < units.size(); unit++) {
                assertEquals(unit, units.get(unit).intValue());
            }
        }
    }

    /**
     * Stores a segment the way version 3 did: its unit bodies back inside the scalar frame, and no
     * unit rows.
     */
    static void joinUnits(Connection connection, long segmentId) throws SQLException {
        byte[] directory = directory(connection, segmentId);
        byte[][] units = new byte[ColdSegmentCodec.unitCount(directory)][];
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT unit, body FROM co_segment_unit WHERE segment_id = " + segmentId)) {
            while (results.next()) {
                units[results.getInt(1)] = results.getBytes(2);
            }
        }
        byte[] frame = ColdSegmentCodec.inline(directory, units);
        try (PreparedStatement statement = connection.prepareStatement("UPDATE co_segment SET scalars = ?, scalars_size = ?, codec_version = 3 WHERE id = ?")) {
            statement.setBytes(1, frame);
            statement.setInt(2, frame.length);
            statement.setLong(3, segmentId);
            statement.executeUpdate();
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_segment_unit WHERE segment_id = " + segmentId);
        }
    }

    /** Stores a segment the way builds before groups did, compressed as a whole, with no unit rows. */
    static void rewriteAsVersionOne(Connection connection, SQLiteColdIndex.TableLayout layout, SQLiteColdIndex.ColdSegment segment) throws SQLException {
        ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
        long[] ids = new long[rows.size()];
        List<Object[]> values = new ArrayList<>();
        for (int row = 0; row < rows.size(); row++) {
            ids[row] = rows.getRowId(row);
            values.add(rows.getValues(row));
        }
        ColdSegmentCodec.Frames frames = ColdSegmentCodec.encodeVersionOne(layout.types, ids, values);
        try (PreparedStatement statement = connection.prepareStatement("UPDATE co_segment SET scalars = ?, scalars_size = ?, payload = ?, payload_size = ?, dict_id = 0, codec_version = 1, chunk_counts = NULL WHERE id = ?")) {
            statement.setBytes(1, Zstd.compress(frames.getScalars(), 3));
            statement.setInt(2, frames.getScalars().length);
            statement.setBytes(3, frames.getPayload().length == 0 ? null : Zstd.compress(frames.getPayload(), 3));
            statement.setInt(4, frames.getPayload().length);
            statement.setLong(5, segment.getId());
            statement.executeUpdate();
        }
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM co_segment_unit WHERE segment_id = " + segment.getId());
        }
    }

    static byte[] directory(Connection connection, long segmentId) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT scalars FROM co_segment WHERE id = " + segmentId)) {
            assertTrue(results.next(), "segment " + segmentId + " is stored");
            return results.getBytes(1);
        }
    }

    /**
     * Runs a lookup about the blocks within a radius of a place through the index, the way the
     * inspector and radius lookups reach it.
     */
    static List<String> lookUp(Connection connection, int x, int z, int radius) throws SQLException {
        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, new Integer[] { radius, x - radius, x + radius, null, null, z - radius, z + radius });
            return rows(connection, "SELECT rowid,time,user,x,y,z,type,action,rolled_back,blockdata FROM " + source + " WHERE " + place(x, z, radius) + " ORDER BY rowid");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private List<String> expected(int x, int z, int radius) throws SQLException {
        return rows(connection, "SELECT id,time,user,x,y,z,type,action,rolled_back,blockdata FROM co_block_all WHERE " + place(x, z, radius) + " ORDER BY id");
    }

    private List<String> expected(int x, int z, int radius, long notBefore) throws SQLException {
        return rows(connection, "SELECT id,time,user,x,y,z,type,action,rolled_back,blockdata FROM co_block_all WHERE " + place(x, z, radius) + " AND time >= " + notBefore + " ORDER BY id");
    }

    private static String place(int x, int z, int radius) {
        return "wid = 1 AND x >= " + (x - radius) + " AND x <= " + (x + radius) + " AND z >= " + (z - radius) + " AND z <= " + (z + radius);
    }

    private static List<String> rows(Connection connection, String query) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet found = statement.executeQuery(query)) {
            while (found.next()) {
                byte[] blockdata = found.getBytes(10);
                rows.add(found.getLong(1) + ":" + found.getLong(2) + ":" + found.getInt(3) + ":" + found.getInt(4) + ":" + found.getInt(5) + ":" + found.getInt(6) + ":"
                        + found.getInt(7) + ":" + found.getInt(8) + ":" + found.getInt(9) + ":" + (blockdata == null ? "-" : new String(blockdata)));
            }
        }
        return rows;
    }

    private List<RealisticActivity.Spot> spread(int count) {
        List<RealisticActivity.Spot> spots = activity.spots();
        List<RealisticActivity.Spot> chosen = new ArrayList<>();
        for (int index = 0; index < count && index < spots.size(); index++) {
            chosen.add(spots.get((int) ((index * 7919L) % spots.size())));
        }
        return chosen;
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
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
