package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * What a read of packed rows fetches from the database, and that it still returns exactly what plain
 * SQL over the same rows unpacked would.
 *
 * <p>
 * A segment's payload frame holds every text and blob value it stores, and is most of its size. It
 * is read only once a lookup keeps a row of the segment that has such a value, so a count, a page
 * whose rows have none, and a segment the lookup keeps nothing of never read it. And a count the
 * metadata recorded at seal time can answer reads nothing of the segment at all.
 * </p>
 *
 * <p>
 * The history is four days, one segment each. Only the rows of player 1 on days 1 and 3, and of
 * player 3 on days 0 and 3, carry blob values, so each day but day 2 has a payload, while player 2
 * has no blob value anywhere. Day 0 holds one block type only, days 0 and 1 lie in world 1, day 2
 * in world 2, and day 3 is split between worlds 2 and 3.
 * </p>
 */
class ColdPayloadReadTest {

    private static final long DAY = 86400L;
    private static final int ROWS_PER_DAY = 2000;
    private static final int DAYS = 4;
    private static final String COLUMNS = "time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back";

    private Connection connection;
    private DatabaseType previousType;

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws Exception {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        SegmentFrameCache.clear();

        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
        }
        seal();
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        SegmentFrameCache.clear();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void aCountThatDecodesSegmentsReadsNoPayload() throws Exception {
        // A player and a block type together, and windows starting inside day 1 and inside day 0:
        // none can be answered from the recorded counts, so segments holding blob values are
        // decoded.
        long reads = ColdSegmentReadCounter.reads();
        long payloads = ColdSegmentReadCounter.payloadFetches();
        long payloadBytes = ColdSegmentReadCounter.payloadBytesRead();
        long decompressed = ColdSegmentReadCounter.payloadReads();

        assertEquals(countPlain("user IN(1) AND type IN(11)"), countThroughIndex("1", "11", 0));
        assertEquals(countPlain("user IN(1) AND time > " + (base() + DAY + 500)), countThroughIndex("1", "", base() + DAY + 500));
        assertEquals(countPlain("user IN(3) AND time > " + (base() + 100)), countThroughIndex("3", "", base() + 100));

        assertTrue(ColdSegmentReadCounter.reads() - reads >= 4, "the counts decoded the segments holding blob values");
        assertEquals(payloads, ColdSegmentReadCounter.payloadFetches(), "counting reads no payload");
        assertEquals(payloadBytes, ColdSegmentReadCounter.payloadBytesRead(), "counting reads no payload bytes");
        assertEquals(decompressed, ColdSegmentReadCounter.payloadReads(), "counting decompresses no payload");
    }

    @Test
    void aPageWhoseRowsHaveNoBlobValuesReadsNoPayload() throws Exception {
        long payloads = ColdSegmentReadCounter.payloadFetches();
        long payloadBytes = ColdSegmentReadCounter.payloadBytesRead();

        // Every row of player 2, from segments that do hold payloads for other players.
        assertEquals(plainRows("user IN(2)", "rowid DESC", 0), readRows("2", "user IN(2)", "rowid DESC", 0, 0));
        // A short page of them, ranked by the reader.
        assertEquals(plainRows("user IN(2)", "rowid DESC", 7), readRows("2", "user IN(2)", "rowid DESC", 7, 0));
        // Player 1's rows of day 0, where none of them has a blob value though player 3's do.
        long endOfFirstDay = base() + DAY - 1;
        String window = "user IN(1) AND time <= " + endOfFirstDay;
        assertEquals(plainRows(window, "rowid DESC", 0), readRows("1", window, "rowid DESC", 0, endOfFirstDay));

        assertEquals(payloads, ColdSegmentReadCounter.payloadFetches(), "no kept row has a blob value, so no payload is read");
        assertEquals(payloadBytes, ColdSegmentReadCounter.payloadBytesRead());
    }

    @Test
    void aPageWhoseRowsHaveBlobValuesReadsOnlyThePayloadsItNeeds() throws Exception {
        long payloads = ColdSegmentReadCounter.payloadFetches();
        long payloadBytes = ColdSegmentReadCounter.payloadBytesRead();
        long reads = ColdSegmentReadCounter.reads();

        // Player 1 has rows in all four segments, but blob values in days 1 and 3 only.
        assertEquals(plainRows("user IN(1)", "rowid DESC", 0), readRows("1", "user IN(1)", "rowid DESC", 0, 0));
        assertEquals(4, ColdSegmentReadCounter.reads() - reads, "every segment holding player 1's rows is decoded");
        assertEquals(2, ColdSegmentReadCounter.payloadFetches() - payloads, "only the two segments whose kept rows have blob values read a payload");
        assertEquals(storedPayloadBytes(1, 3), ColdSegmentReadCounter.payloadBytesRead() - payloadBytes, "and exactly their payloads are read");

        // A second read of the same rows finds the payloads in memory.
        long again = ColdSegmentReadCounter.payloadFetches();
        assertEquals(plainRows("user IN(1)", "rowid DESC", 0), readRows("1", "user IN(1)", "rowid DESC", 0, 0));
        assertEquals(again, ColdSegmentReadCounter.payloadFetches(), "a payload already read is not read again");

        // A short page ranked by the reader reaches only the newest segment, whose rows need its payload.
        SegmentFrameCache.clear();
        long page = ColdSegmentReadCounter.payloadFetches();
        assertEquals(plainRows("user IN(3)", "rowid DESC", 7), readRows("3", "user IN(3)", "rowid DESC", 7, 0));
        assertEquals(1, ColdSegmentReadCounter.payloadFetches() - page, "the page reads the payload of the one segment it takes rows from");
    }

    @Test
    void countsTheRecordedMetadataAnswersReadNothing() throws Exception {
        long base = base();
        // A window covering days 1 and 2 whole, and nothing else.
        assertAnsweredFromMetadata("time only", "time > " + (base + DAY - 1) + " AND time <= " + (base + (3 * DAY) - 1), "", "", "", base + DAY - 1, base + (3 * DAY) - 1, 0,
                null);
        // Worlds: days 0 and 1 hold world 1 alone, day 3 mixes worlds 2 and 3 and is answered by its
        // chunk counts.
        assertAnsweredFromMetadata("world 1", "wid = 1", "", "", "", 0, 0, 1, null);
        assertAnsweredFromMetadata("world 2", "wid = 2", "", "", "", 0, 0, 2, null);
        assertAnsweredFromMetadata("world 3", "wid = 3", "", "", "", 0, 0, 3, null);
        // Bounds taking in whole chunks.
        assertAnsweredFromMetadata("whole chunks", "wid = 1 AND x >= 0 AND x <= 47 AND z >= 16 AND z <= 63", "", "", "", 0, 0, 1, new Integer[] { 0, 0, 47, null, null, 16, 63 });
        // One column at a time.
        assertAnsweredFromMetadata("user", "user IN(1)", "1", "", "", 0, 0, 0, null);
        assertAnsweredFromMetadata("type", "type IN(12)", "", "12", "", 0, 0, 0, null);
        assertAnsweredFromMetadata("action", "action IN(1)", "", "", "1", 0, 0, 0, null);
        // Two columns, where in every segment one of them keeps every row or none: day 0 holds
        // block type 10 alone, and no other day has it.
        assertAnsweredFromMetadata("user and type", "user IN(2) AND type IN(10)", "2", "10", "", 0, 0, 0, null);
        // Days 0 and 1 are all world 1, and days 2 and 3 have none of it.
        assertAnsweredFromMetadata("user and world", "wid = 1 AND user IN(2)", "2", "", "", 0, 0, 1, null);
    }

    /**
     * Counts through the index and requires the count to agree with plain SQL, and nothing of any
     * segment to have been read or decoded to get it.
     */
    private void assertAnsweredFromMetadata(String label, String where, String users, String types, String actions, long startTime, long endTime, int worldId, Integer[] bounds)
            throws Exception {
        long frames = ColdSegmentReadCounter.frameReads();
        long units = ColdSegmentReadCounter.unitReads();
        long groups = ColdSegmentReadCounter.groupReads();
        long reads = ColdSegmentReadCounter.reads();
        long bytes = ColdSegmentReadCounter.bytesRead();

        long expected = countPlain(where);
        long counted;
        SQLiteColdIndex.beginLookup(startTime, endTime);
        try {
            SQLiteColdIndex.setLookupFilters(users, types, actions);
            if (bounds != null) {
                SQLiteColdIndex.setPredicateFilters(" " + where, "user");
            }
            SQLiteColdIndex.setCounting(true);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", worldId, bounds);
            counted = count("SELECT COUNT(*) FROM " + source + " WHERE " + where) + SQLiteColdIndex.countedRows("block");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }

        assertTrue(expected > 0, label + ": the count has something to find");
        assertEquals(expected, counted, label);
        assertEquals(frames, ColdSegmentReadCounter.frameReads(), label + ": no segment frame is read");
        assertEquals(units, ColdSegmentReadCounter.unitReads(), label + ": no unit is read");
        assertEquals(groups, ColdSegmentReadCounter.groupReads(), label + ": no group is decoded");
        assertEquals(reads, ColdSegmentReadCounter.reads(), label + ": no segment is decoded");
        assertEquals(bytes, ColdSegmentReadCounter.bytesRead(), label + ": nothing is read from the database");
    }

    /** Counts packed rows through the index the way a lookup does, plus the live rows. */
    private long countThroughIndex(String users, String types, long windowStart) throws Exception {
        StringBuilder where = new StringBuilder("1");
        if (!users.isEmpty()) {
            where.append(" AND user IN(").append(users).append(')');
        }
        if (!types.isEmpty()) {
            where.append(" AND type IN(").append(types).append(')');
        }
        if (windowStart > 0) {
            where.append(" AND time > ").append(windowStart);
        }
        SQLiteColdIndex.beginLookup(windowStart, 0);
        try {
            SQLiteColdIndex.setLookupFilters(users, types);
            SQLiteColdIndex.setCounting(true);
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            return count("SELECT COUNT(*) FROM " + source + " WHERE " + where) + SQLiteColdIndex.countedRows("block");
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    /**
     * Reads packed rows through the index, the whole of them when the budget is zero, or a page the
     * reader ranks itself otherwise, and returns every column of the rows the query then shows.
     */
    private List<String> readRows(String users, String where, String order, int page, long endTime) throws Exception {
        SQLiteColdIndex.beginLookup(0, endTime);
        try {
            SQLiteColdIndex.setLookupFilters(users, "");
            if (page > 0) {
                SQLiteColdIndex.setRowBudget(page);
                SQLiteColdIndex.setPageOrder(order);
                SQLiteColdIndex.setPredicateFilters(" " + where, "user");
            }
            String source = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
            return rows("SELECT rowid," + COLUMNS + " FROM " + source + " WHERE " + where + " ORDER BY " + order + (page > 0 ? " LIMIT " + page : ""));
        }
        finally {
            SQLiteColdIndex.endLookup(connection);
        }
    }

    private List<String> plainRows(String where, String order, int page) throws SQLException {
        return rows("SELECT id," + COLUMNS + " FROM co_block_all WHERE " + where + " ORDER BY " + order.replace("rowid", "id") + (page > 0 ? " LIMIT " + page : ""));
    }

    private long countPlain(String where) throws SQLException {
        return count("SELECT COUNT(*) FROM co_block_all WHERE " + where);
    }

    /** The stored size of the payloads of the segments packed from the given days of the history. */
    private long storedPayloadBytes(int... days) throws SQLException {
        long first = base() / DAY;
        StringBuilder list = new StringBuilder();
        for (int day : days) {
            list.append(list.length() == 0 ? "" : ",").append(first + day);
        }
        return count("SELECT SUM(LENGTH(payload)) FROM co_segment WHERE day IN(" + list + ")");
    }

    /** Every column of every row a query returns, blob values written out byte by byte. */
    private List<String> rows(String query) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            int columns = results.getMetaData().getColumnCount();
            while (results.next()) {
                StringBuilder row = new StringBuilder();
                for (int column = 1; column <= columns; column++) {
                    Object value = results.getObject(column);
                    row.append(value instanceof byte[] ? Arrays.toString((byte[]) value) : String.valueOf(value)).append('|');
                }
                rows.add(row.toString());
            }
        }
        assertTrue(!rows.isEmpty(), "the query has rows to compare: " + query);
        return rows;
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private long base() {
        long now = System.currentTimeMillis() / 1000L;
        return ((now - (40 * DAY)) / DAY) * DAY;
    }

    private void seal() throws Exception {
        long base = base();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (" + COLUMNS + ") VALUES (?,?,?,?,64,?,?,0,?,?,?,0)")) {
            for (int day = 0; day < DAYS; day++) {
                for (int index = 0; index < ROWS_PER_DAY; index++) {
                    int user = 1 + (index % 10);
                    int world = day < 2 ? 1 : (day == 2 || index % 2 == 0 ? 2 : 3);
                    boolean blockData = user == 1 && (day == 1 || day == 3);
                    boolean meta = user == 3 && (day == 0 || day == 3);
                    statement.setLong(1, base + (day * DAY) + index);
                    statement.setInt(2, user);
                    statement.setInt(3, world);
                    statement.setInt(4, index % 100);
                    statement.setInt(5, (index / 7) % 100);
                    statement.setInt(6, day == 0 ? 10 : 11 + (index % 4));
                    statement.setBytes(7, meta ? ("meta of row " + index + " on day " + day).getBytes() : null);
                    statement.setBytes(8, blockData ? ("minecraft:block_" + index + "[day=" + day + "]").getBytes() : null);
                    statement.setInt(9, index % 2);
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block_all AS SELECT rowid AS id, * FROM co_block");
        }
        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });
        assertTrue(sealed > 0, "the history has to reach compressed storage");
        SQLiteColdIndex.reload(connection);
        assertEquals(DAYS, count("SELECT COUNT(*) FROM co_segment"), "one segment a day");
        assertEquals(0, count("SELECT COUNT(*) FROM co_segment WHERE day = " + (base / DAY + 2) + " AND payload IS NOT NULL"), "day 2 has no blob values");
    }
}
