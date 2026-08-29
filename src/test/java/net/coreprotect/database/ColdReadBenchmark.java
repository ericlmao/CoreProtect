package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * Measures where the time goes when a lookup reads cold segments, phase by phase, so the read path
 * can be made faster based on numbers rather than guesses.
 *
 * <p>
 * Run with: {@code mvn -DskipTests=false -Dcoreprotect.bench=cold -Dtest=ColdReadBenchmark test}
 * </p>
 */
class ColdReadBenchmark {

    private static final int ROWS = 1_000_000;
    private static final long DAY = 86400L;

    @Test
    void measuresColdReadPhases(@TempDir Path directory) throws Exception {
        assumeTrue("cold".equals(System.getProperty("coreprotect.bench")), "benchmark runs only when asked for");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        Path database = directory.resolve("database.db");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database)) {
            build(connection);
            SQLiteColdIndex.reload(connection);

            List<SQLiteColdIndex.ColdSegment> all = SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null);
            System.out.println("segments=" + all.size());

            // Phase 1: read the compressed frames from the database.
            long start = System.nanoTime();
            long compressedBytes = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                try (PreparedStatement statement = connection.prepareStatement("SELECT scalars,payload FROM co_segment WHERE id = ?")) {
                    statement.setLong(1, segment.getId());
                    try (ResultSet results = statement.executeQuery()) {
                        results.next();
                        compressedBytes += results.getBytes(1).length;
                        byte[] payload = results.getBytes(2);
                        compressedBytes += payload == null ? 0 : payload.length;
                    }
                }
            }
            System.out.println("phase_read_blobs_ms=" + (System.nanoTime() - start) / 1_000_000 + " compressed_kb=" + compressedBytes / 1024);

            // Phase 2: decompress and decode every row.
            start = System.nanoTime();
            long decoded = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
                decoded += rows.size();
            }
            System.out.println("phase_decompress_decode_ms=" + (System.nanoTime() - start) / 1_000_000 + " rows=" + decoded);

            // Phase 3: the full path a lookup takes, including the temporary table.
            SQLiteColdIndex.beginLookup(0, 0);
            String source;
            try {
                start = System.nanoTime();
                source = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
                System.out.println("phase_full_materialize_ms=" + (System.nanoTime() - start) / 1_000_000);

                // Phase 4: the SQL that runs over the materialized rows.
                start = System.nanoTime();
                try (Statement statement = connection.createStatement();
                        ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + source + " WHERE wid = 1 AND x >= 100 AND x <= 150 AND z >= 100 AND z <= 150")) {
                    results.next();
                    System.out.println("phase_sql_over_temp_ms=" + (System.nanoTime() - start) / 1_000_000 + " matches=" + results.getLong(1));
                }
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }

            // A user lookup: no location bounds, so segment pruning has nothing to work with.
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                SQLiteColdIndex.setLookupFilters("7", "");
                long userStart = System.nanoTime();
                String userSource = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
                try (Statement statement = connection.createStatement();
                        ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + userSource + " WHERE user IN(7)")) {
                    results.next();
                    System.out.println("user_lookup_ms=" + (System.nanoTime() - userStart) / 1_000_000 + " matches=" + results.getLong(1));
                }
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }

            // What the same lookup would cost if the user filter were applied while decoding.
            SQLiteColdIndex.invalidate();
            SQLiteColdIndex.reload(connection);
            long filterStart = System.nanoTime();
            long kept = 0;
            for (SQLiteColdIndex.ColdSegment segment : all) {
                ColdSegmentCodec.Rows rows = SQLiteColdIndex.readRows(connection, segment);
                for (int row = 0; row < rows.size(); row++) {
                    Object user = rows.getValues(row)[1];
                    if (user != null && ((Number) user).intValue() == 7) {
                        kept++;
                    }
                }
            }
            System.out.println("decode_with_user_filter_ms=" + (System.nanoTime() - filterStart) / 1_000_000 + " kept=" + kept);

            // A page of a user lookup, which is what the command actually asks for.
            for (int pageSize : new int[] { 10, 100 }) {
                SQLiteColdIndex.beginLookup(0, 0);
                try {
                    long pageStart = System.nanoTime();
                    SQLiteColdIndex.setLookupFilters("7", "");
                    SQLiteColdIndex.setRowBudget(pageSize);
                    String pageSource = SQLiteColdIndex.sourceExpression(connection, "block", 0, null);
                    long decodedRows = 0;
                    try (Statement statement = connection.createStatement();
                            ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM temp.cp_cold_block")) {
                        results.next();
                        decodedRows = results.getLong(1);
                    }
                    try (Statement statement = connection.createStatement();
                            ResultSet results = statement.executeQuery("SELECT rowid FROM " + pageSource + " WHERE user IN(7) ORDER BY rowid DESC LIMIT " + pageSize)) {
                        long rows = 0;
                        while (results.next()) {
                            rows++;
                        }
                        System.out.println("user_page_" + pageSize + "_ms=" + (System.nanoTime() - pageStart) / 1_000_000 + " rows=" + rows + " decoded=" + decodedRows);
                    }
                }
                finally {
                    SQLiteColdIndex.endLookup(connection);
                }
            }

            // A pruned lookup, the way a radius query actually arrives: one chunk of the world.
            SQLiteColdIndex.beginLookup(0, 0);
            try {
                start = System.nanoTime();
                String pruned = SQLiteColdIndex.sourceExpression(connection, "block", 1, new Integer[] { 0, 100, 150, 0, 0, 100, 150 });
                try (Statement statement = connection.createStatement();
                        ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + pruned + " WHERE wid = 1 AND x >= 100 AND x <= 150 AND z >= 100 AND z <= 150")) {
                    results.next();
                    System.out.println("pruned_radius_lookup_ms=" + (System.nanoTime() - start) / 1_000_000 + " matches=" + results.getLong(1));
                }
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
        }
    }

    private void build(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
        }

        long base = ((System.currentTimeMillis() / 1000L - (30 * DAY)) / DAY) * DAY;
        java.util.Random random = new java.util.Random(7);
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,NULL,?,?,0)")) {
            for (int i = 0; i < ROWS; i++) {
                statement.setLong(1, base + (i / 4000)); // ~4000 events per second of game time
                statement.setInt(2, random.nextInt(12) + 1);
                statement.setInt(3, 1);
                statement.setInt(4, random.nextInt(2000) - 1000);
                statement.setInt(5, random.nextInt(256) - 64);
                statement.setInt(6, random.nextInt(2000) - 1000);
                statement.setInt(7, random.nextInt(64) + 1);
                statement.setInt(8, 0);
                statement.setBytes(9, random.nextInt(10) == 0 ? ("4," + random.nextInt(30)).getBytes() : null);
                statement.setInt(10, random.nextInt(2));
                statement.addBatch();
                if (i % 50000 == 0) {
                    statement.executeBatch();
                    connection.commit();
                }
            }
            statement.executeBatch();
            connection.commit();
        }
        connection.setAutoCommit(true);

        long start = System.nanoTime();
        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });
        System.out.println("rollup_ms=" + (System.nanoTime() - start) / 1_000_000 + " sealed=" + sealed);
    }
}
