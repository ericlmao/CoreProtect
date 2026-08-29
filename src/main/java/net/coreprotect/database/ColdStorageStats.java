package net.coreprotect.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import net.coreprotect.config.ConfigHandler;

/**
 * Reports how much of the database is live activity and how much is compressed storage.
 *
 * <p>
 * The compressed size is read from the segment table directly. The live size is what remains of the
 * file once the compressed segments and the pages waiting to be reused are accounted for, which is
 * the live rows together with their indexes. Both come from counters SQLite already maintains, so
 * asking for them costs nothing noticeable even on a large database.
 * </p>
 */
public final class ColdStorageStats {

    private final long hotBytes;
    private final long coldBytes;
    private final long freeBytes;
    private final long coldRows;
    private final long blobBytes;
    private final long blobSavedBytes;

    private ColdStorageStats(long hotBytes, long coldBytes, long freeBytes, long coldRows, long blobBytes, long blobSavedBytes) {
        this.hotBytes = hotBytes;
        this.coldBytes = coldBytes;
        this.freeBytes = freeBytes;
        this.coldRows = coldRows;
        this.blobBytes = blobBytes;
        this.blobSavedBytes = blobSavedBytes;
    }

    /**
     * @return the bytes held by live rows and their indexes
     */
    public long getHotBytes() {
        return hotBytes;
    }

    /**
     * @return the bytes held by compressed segments
     */
    public long getColdBytes() {
        return coldBytes;
    }

    /**
     * @return the bytes freed by compacting that are waiting to be reused
     */
    public long getFreeBytes() {
        return freeBytes;
    }

    /**
     * @return the number of rows stored in compressed segments
     */
    public long getColdRows() {
        return coldRows;
    }

    /**
     * @return the bytes the packed entity data occupies, which is part of the compressed total
     */
    public long getBlobBytes() {
        return blobBytes;
    }

    /**
     * Entity data is compressed into groups rather than segments, because it is read a row at a time.
     * Reporting what that saved makes it clear that the live size falling is compression rather than
     * anything going missing.
     *
     * @return the bytes compressing those blobs has saved
     */
    public long getBlobSavedBytes() {
        return blobSavedBytes;
    }

    /**
     * Measures the current split between live and compressed storage.
     *
     * @param connection
     *            an open connection
     * @return the measurement, or null when the database has no compressed storage
     * @throws SQLException
     *             if the database cannot be read
     */
    public static ColdStorageStats read(Connection connection) throws SQLException {
        if (!ConfigHandler.databaseType.isSQLite()) {
            return null;
        }

        try (Statement statement = connection.createStatement()) {
            long pageSize = value(statement, "PRAGMA page_size");
            long pageCount = value(statement, "PRAGMA page_count");
            long freeList = value(statement, "PRAGMA freelist_count");
            if (pageSize <= 0 || pageCount <= 0) {
                return null;
            }

            long coldBytes = 0;
            long coldRows = 0;
            try (ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(LENGTH(scalars) + LENGTH(COALESCE(payload, x''))),0), COALESCE(SUM(row_count),0) FROM " + ConfigHandler.prefix + "segment")) {
                if (results.next()) {
                    coldBytes = results.getLong(1);
                    coldRows = results.getLong(2);
                }
            }

            // Entity data is compressed into groups rather than segments, because it is read a row at
            // a time. It is compressed storage all the same, and counting it anywhere else would
            // leave it looking as though packing it away achieved nothing.
            long blobBytes = 0;
            try (ResultSet results = statement.executeQuery("SELECT COALESCE(SUM(LENGTH(data)),0) FROM " + ConfigHandler.prefix + "blob_group")) {
                if (results.next()) {
                    blobBytes = results.getLong(1);
                }
            }
            coldBytes = coldBytes + blobBytes;

            long totalBytes = pageCount * pageSize;
            long freeBytes = freeList * pageSize;
            long hotBytes = Math.max(0, totalBytes - coldBytes - freeBytes);
            return new ColdStorageStats(hotBytes, coldBytes, freeBytes, coldRows, blobBytes, BlobRecompressTask.savedBytes(connection));
        }
    }

    private static long value(Statement statement, String pragma) throws SQLException {
        try (ResultSet results = statement.executeQuery(pragma)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    /**
     * Formats a byte count the way an operator reads it.
     *
     * @param bytes
     *            the byte count
     * @return the count as KB, MB or GB
     */
    public static String format(long bytes) {
        if (bytes >= 1024L * 1024 * 1024) {
            return String.format(Locale.ROOT, "%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
        if (bytes >= 1024L * 1024) {
            return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
    }
}
