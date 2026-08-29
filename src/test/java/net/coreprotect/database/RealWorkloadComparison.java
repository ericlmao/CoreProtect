package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * Replays the block log of a real DuckDB database through both SQLite storage layouts and reports
 * the resulting file sizes.
 *
 * <p>
 * Run with the database to read from:
 * {@code mvn -DskipTests=false -Dcoreprotect.compare=/path/to/database.duckdb -Dtest=RealWorkloadComparison test}
 * </p>
 */
class RealWorkloadComparison {

    private static final long DAY = 86400L;

    @Test
    void comparesStorageLayoutsOnRealData(@TempDir Path directory) throws Exception {
        String source = System.getProperty("coreprotect.compare");
        assumeTrue(source != null, "comparison only runs when a source database is given");

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        Path legacy = directory.resolve("legacy.db");
        Path modern = directory.resolve("modern.db");

        // Shift the log back in time so it sits outside the hot window, which is what the roll-up
        // would find on a server that had been running for a while.
        long shift = 40 * DAY;

        long rows;
        try (Connection duck = openSource(source)) {
            rows = copyBlocks(duck, legacy, shift, false);
            copyBlocks(duck, modern, shift, true);
        }

        long legacySize = Files.size(legacy);
        long modernHot = Files.size(modern);

        long sealed;
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + modern)) {
            sealed = ColdRollupTask.rollUp(connection, () -> {
            });
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA incremental_vacuum");
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }
        }
        long modernCold = Files.size(modern);

        // Prove the compressed rows still read back exactly, before quoting any size win.
        try (Connection duck = openSource(source);
                Connection connection = DriverManager.getConnection("jdbc:sqlite:" + modern)) {
            long[] expected = checksum(duck, "SELECT COUNT(*), SUM(x), SUM(z), SUM(type), SUM(action) FROM co_block");
            SQLiteColdIndex.reload(connection);
            SQLiteColdIndex.beginLookup(0, 0);
            String sourceTable;
            long[] actual;
            try {
                sourceTable = SQLiteColdIndex.sourceExpression(connection, "block", 1, null);
                actual = checksum(connection, "SELECT COUNT(*), SUM(x), SUM(z), SUM(type), SUM(action) FROM " + sourceTable);
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }

            System.out.println("read_back_source=" + (sourceTable.startsWith("(") ? "compressed+live" : "live only"));
            for (int index = 0; index < expected.length; index++) {
                org.junit.jupiter.api.Assertions.assertEquals(expected[index], actual[index], "column checksum " + index + " differs after compression");
            }
            System.out.println("read_back_verified_rows=" + actual[0]);
        }

        System.out.println("block_rows=" + rows);
        System.out.println("sealed_rows=" + sealed);
        System.out.println("sqlite_v1_bytes=" + legacySize);
        System.out.println("sqlite_v2_hot_bytes=" + modernHot);
        System.out.println("sqlite_v2_cold_bytes=" + modernCold);
        System.out.printf("bytes_per_row_v1=%.2f%n", legacySize / (double) rows);
        System.out.printf("bytes_per_row_v2=%.2f%n", modernCold / (double) rows);
        System.out.printf("shrink_factor=%.2fx%n", legacySize / (double) modernCold);
    }

    /**
     * Opens the database to replay, which may be either backend.
     *
     * @param source
     *            the path of the database to read
     * @return an open connection
     * @throws SQLException
     *             if the database cannot be opened
     */
    private static Connection openSource(String source) throws SQLException {
        String path = Paths.get(source).toAbsolutePath().toString();
        if (path.endsWith(".duckdb")) {
            return DriverManager.getConnection("jdbc:duckdb:" + path);
        }
        return DriverManager.getConnection("jdbc:sqlite:" + path + "?open_mode=1");
    }

    private static long[] checksum(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            results.next();
            return new long[] { results.getLong(1), results.getLong(2), results.getLong(3), results.getLong(4), results.getLong(5) };
        }
    }

    private long copyBlocks(Connection duck, Path target, long shift, boolean modern) throws Exception {
        if (modern) {
            ConfigHandler.path = target.getParent().toString() + "/";
            ConfigHandler.sqlite = target.getFileName().toString();
            SQLiteSchema.prepareDatabaseFile();
        }

        long copied = 0;
        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite:" + target)) {
            try (Statement statement = sqlite.createStatement()) {
                statement.executeUpdate("PRAGMA journal_mode=WAL");
                statement.executeUpdate("PRAGMA synchronous=NORMAL");
                statement.executeUpdate("CREATE TABLE co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
                if (modern) {
                    SQLiteSchema.createTables("co_", statement);
                }
            }

            sqlite.setAutoCommit(false);
            String insert = "INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
            try (PreparedStatement writer = sqlite.prepareStatement(insert);
                    Statement reader = duck.createStatement();
                    ResultSet results = reader.executeQuery("SELECT time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back FROM co_block ORDER BY rowid")) {
                while (results.next()) {
                    writer.setLong(1, results.getLong(1) - shift);
                    for (int column = 2; column <= 8; column++) {
                        writer.setInt(column, results.getInt(column));
                    }
                    writer.setBytes(9, results.getBytes(9));
                    writer.setBytes(10, results.getBytes(10));
                    writer.setInt(11, results.getInt(11));
                    writer.setInt(12, results.getInt(12));
                    writer.addBatch();
                    copied++;
                    if (copied % 50000 == 0) {
                        writer.executeBatch();
                        sqlite.commit();
                    }
                }
                writer.executeBatch();
                sqlite.commit();
            }

            sqlite.setAutoCommit(true);

            // Both layouts carry the same indexes on live rows.
            try (Statement statement = sqlite.createStatement()) {
                statement.executeUpdate("CREATE INDEX block_index ON co_block(wid,x,z,time)");
                statement.executeUpdate("CREATE INDEX block_user_index ON co_block(user,time)");
                statement.executeUpdate("CREATE INDEX block_type_index ON co_block(type,time)");
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }
        }
        catch (SQLException exception) {
            throw new SQLException("Unable to replay the block log into " + target, exception);
        }

        return copied;
    }
}
