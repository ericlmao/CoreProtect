package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobCompression;

/**
 * Activity is packed as soon as there is a segment's worth of it, and a day's remainder once the day
 * is over. There is no window of recent history kept apart: the live table only holds what has not
 * filled a segment yet.
 */
class ContinuousPackingTest {

    private static final long DAY = 86400L;

    private Connection connection;
    private DatabaseType previousType;
    private boolean previousCompression;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        previousCompression = Config.getGlobal().BLOB_COMPRESSION;
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
        Config.getGlobal().BLOB_COMPRESSION = previousCompression;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void aFinishedDayIsPackedEvenWhenItIsSmall() throws Exception {
        assumeNotJustAfterMidnight();
        long yesterday = today() - DAY;
        insert(yesterday + 100, 500, null);
        insert(now() - 60, 20, null); // today, which keeps the numbering up and is still filling

        assertTrue(ColdRollupTask.hasWork(connection), "a finished day is ready to pack");
        long packed = ColdRollupTask.rollUp(connection, () -> {
        });

        assertEquals(500, packed, "the whole of yesterday is packed, however recent it is");
        assertEquals(20, liveRows(), "today's rows wait until they fill a segment");
        assertFalse(ColdRollupTask.hasWork(connection), "nothing else is ready");
    }

    @Test
    void todayIsPackedAsSoonAsASegmentIsFull() throws Exception {
        assumeNotJustAfterMidnight();
        long start = today() + 1;
        insert(start, ColdRollupTask.SEGMENT_ROWS + 300, null);

        assertTrue(ColdRollupTask.hasWork(connection));
        long packed = ColdRollupTask.rollUp(connection, () -> {
        });

        assertEquals(ColdRollupTask.SEGMENT_ROWS, packed, "one full segment of today is packed straight away");
        assertEquals(300, liveRows(), "the remainder waits");
        assertFalse(ColdRollupTask.hasWork(connection));
    }

    @Test
    void aRowOfAnEarlierDayBehindTodaysRowsDoesNotHoldPackingUp() throws Exception {
        assumeNotJustAfterMidnight();
        // A few of today's rows, then a row stamped yesterday that arrived late, then more of today.
        insert(today() + 5, 10, null);
        insert(today() - 60, 1, null);
        insert(today() + 30, 200, null);
        insert(now() - 5, 1, null);

        assertTrue(ColdRollupTask.hasWork(connection), "the day change inside the waiting rows is seen");
        long packed = ColdRollupTask.rollUp(connection, () -> {
        });
        assertEquals(11, packed, "the rows up to the day change are packed, and the late row with them, so today's rows behind it can be");
        assertEquals(201, liveRows());
    }

    @Test
    void lookupsSeeEveryRowWhereverItIs() throws Exception {
        assumeNotJustAfterMidnight();
        insert(today() - DAY + 50, 1200, null);
        insert(now() - 30, 40, null);
        ColdRollupTask.rollUp(connection, () -> {
        });
        assertEquals(40, liveRows());

        SQLiteColdIndex.beginLookup(0, 0);
        try {
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            assertEquals(1240, count("SELECT COUNT(*) FROM " + source));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    @Test
    void aRowTooNewToPackStopsTheBlockInsteadOfBreakingIt() throws Exception {
        long yesterday = today() - DAY;
        insert(yesterday + 10, 100, null);
        // Written in between, stamped later than the cutoff the compact is asked for.
        insert(now() + 3600, 1, null);
        insert(yesterday + 20, 100, null);
        insert(now() - 5, 1, null);

        long packed = ColdRollupTask.rollUp(connection, () -> {
        }, now());
        assertEquals(100, packed, "the rows before the one that is too new are packed");
        assertEquals(102, liveRows(), "the rest wait, and nothing failed");
    }

    @Test
    void blobsArePackedPlainAndReadBackUnchanged() throws Exception {
        assumeNotJustAfterMidnight();
        Config.getGlobal().BLOB_COMPRESSION = true;
        byte[] original = new byte[400];
        Arrays.fill(original, (byte) 'q');
        byte[] logged = BlobCompression.compress(original);
        assertTrue(BlobCompression.isCompressed(logged), "the blob is compressed as it is logged");

        insert(today() - DAY + 5, 10, logged);
        insert(now() - 5, 1, null);
        ColdRollupTask.rollUp(connection, () -> {
        });

        SQLiteColdIndex.ColdSegment segment = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null).get(0);
        ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
        int blockdata = SQLiteColdIndex.layout(connection, "block").columns.length - 3;
        for (int row = 0; row < rows.size(); row++) {
            byte[] stored = (byte[]) rows.getValues(row)[blockdata];
            assertFalse(BlobCompression.isCompressed(stored), "inside a segment the blob is left for the segment to compress");
            assertArrayEquals(original, BlobCompression.decompress(stored), "and reads back as it was logged");
        }
    }

    private void assumeNotJustAfterMidnight() {
        // A day only counts as over a few minutes after midnight, so just after midnight yesterday
        // is still open and today's rows could still be written for it.
        org.junit.jupiter.api.Assumptions.assumeTrue(now() - today() > ColdRollupTask.DAY_CLOSE_GRACE_SECONDS + 120);
    }

    private static long now() {
        return System.currentTimeMillis() / 1000L;
    }

    private static long today() {
        return (now() / DAY) * DAY;
    }

    private void insert(long firstTime, int rows, byte[] blockdata) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,5,0,NULL,?,1,0)")) {
            for (int row = 0; row < rows; row++) {
                statement.setLong(1, firstTime + row / 50);
                statement.setInt(2, 1 + row % 5);
                statement.setInt(3, row % 300);
                statement.setInt(4, (row * 7) % 300);
                statement.setBytes(5, blockdata);
                statement.addBatch();
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(autoCommit);
    }

    private long liveRows() throws SQLException {
        return count("SELECT COUNT(*) FROM co_block");
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }
}
