package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.serialize.BlobCompression;

/**
 * Trains, stores and applies the shared compression dictionaries used by cold segment payloads.
 *
 * <p>
 * Segment payloads are made of many small values that individually give a compressor almost nothing
 * to work with. A dictionary trained on real payloads from the same server supplies the repeated
 * structure up front, which is worth far more on this kind of data than a higher compression level.
 * Every compressed frame records which dictionary produced it, so dictionaries can be added over
 * time and old segments stay readable.
 * </p>
 */
public final class SegmentDictionary {

    /** Size of a trained dictionary. Large enough to hold the repeated structure, small to store. */
    private static final int DICTIONARY_BYTES = 65536;

    /** Sample bytes needed before a dictionary is trained. */
    private static final int MINIMUM_SAMPLE_BYTES = 1024 * 1024;

    private static final Map<Integer, ZstdDictCompress> COMPRESSORS = new ConcurrentHashMap<>();
    private static final Map<Integer, ZstdDictDecompress> DECOMPRESSORS = new ConcurrentHashMap<>();

    private SegmentDictionary() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Forgets the cached dictionaries, which is required when the database is reopened.
     */
    public static void clearCache() {
        COMPRESSORS.clear();
        DECOMPRESSORS.clear();
    }

    /**
     * @param connection
     *            an open connection
     * @param tableId
     *            the segment table id
     * @return the newest dictionary id for that table, or 0 when it has none yet
     * @throws SQLException
     *             if the dictionary table cannot be read
     */
    public static int currentDictionary(Connection connection, int tableId) throws SQLException {
        String query = "SELECT dict_id FROM " + ConfigHandler.prefix + "segment_dict WHERE table_id = ? ORDER BY dict_id DESC LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, tableId);
            try (ResultSet results = statement.executeQuery()) {
                return results.next() ? results.getInt(1) : 0;
            }
        }
    }

    /**
     * Trains a dictionary from real payload values and stores it.
     *
     * @param connection
     *            an open connection
     * @param tableId
     *            the segment table id
     * @param samples
     *            payload values to train on
     * @return the new dictionary id, or 0 when there was not enough material to train on
     * @throws SQLException
     *             if the dictionary cannot be stored
     */
    public static int train(Connection connection, int tableId, List<byte[]> samples) throws SQLException {
        long sampleBytes = 0;
        for (byte[] sample : samples) {
            sampleBytes = sampleBytes + sample.length;
        }
        if (sampleBytes < MINIMUM_SAMPLE_BYTES || samples.size() < 64 || !BlobCompression.isAvailable()) {
            return 0;
        }

        byte[] dictionary;
        try {
            ZstdDictTrainer trainer = new ZstdDictTrainer((int) Math.min(sampleBytes, 64L * 1024 * 1024), DICTIONARY_BYTES);
            for (byte[] sample : samples) {
                if (!trainer.addSample(sample)) {
                    break;
                }
            }
            dictionary = trainer.trainSamples();
        }
        catch (Throwable failure) {
            // Training is an optimisation; a failure just means payloads compress without one.
            return 0;
        }

        if (dictionary == null || dictionary.length == 0) {
            return 0;
        }

        int dictionaryId;
        try (PreparedStatement statement = connection.prepareStatement("SELECT COALESCE(MAX(dict_id),0) + 1 FROM " + ConfigHandler.prefix + "segment_dict")) {
            try (ResultSet results = statement.executeQuery()) {
                results.next();
                dictionaryId = results.getInt(1);
            }
        }

        String insert = "INSERT INTO " + ConfigHandler.prefix + "segment_dict (dict_id,table_id,time,sample_bytes,data) VALUES (?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            statement.setInt(1, dictionaryId);
            statement.setInt(2, tableId);
            statement.setLong(3, System.currentTimeMillis() / 1000L);
            statement.setLong(4, sampleBytes);
            statement.setBytes(5, dictionary);
            statement.executeUpdate();
        }

        COMPRESSORS.put(dictionaryId, new ZstdDictCompress(dictionary, BlobCompression.storageLevel()));
        DECOMPRESSORS.put(dictionaryId, new ZstdDictDecompress(dictionary));
        return dictionaryId;
    }

    /**
     * Loads a dictionary into the cache so later use needs no database access, which lets segments
     * be decoded on threads that hold no connection.
     *
     * @param dictionaryId
     *            the dictionary to load, or 0 for none
     * @param connection
     *            an open connection
     * @throws SQLException
     *             if the dictionary cannot be read
     */
    public static void preload(int dictionaryId, Connection connection) throws SQLException {
        if (dictionaryId > 0) {
            decompressor(dictionaryId, connection);
        }
    }

    /**
     * Loads a dictionary for writing, so it can then be used from threads that hold no connection.
     *
     * @param dictionaryId
     *            the dictionary to load, or 0 for none
     * @param connection
     *            an open connection
     * @throws SQLException
     *             if the dictionary cannot be read
     */
    public static void warm(int dictionaryId, Connection connection) throws SQLException {
        if (dictionaryId > 0) {
            compressor(dictionaryId, connection);
        }
    }

    /**
     * Compresses with a dictionary that has already been loaded.
     *
     * <p>
     * Compressing is the slow part of packing and is what a compact spreads across cores, so it has
     * to be possible without a connection: a connection belongs to one thread at a time, and reading
     * a dictionary through it from several would corrupt whatever else it was doing.
     * </p>
     *
     * @param data
     *            the payload to compress
     * @param dictionaryId
     *            the dictionary to use, or 0 for none
     * @return the compressed payload
     * @throws SQLException
     *             if the dictionary was never loaded
     */
    public static byte[] compressWith(byte[] data, int dictionaryId) throws SQLException {
        if (data == null || data.length == 0 || !BlobCompression.isAvailable()) {
            return data;
        }
        if (dictionaryId <= 0) {
            return Zstd.compress(data, BlobCompression.storageLevel());
        }
        ZstdDictCompress cached = COMPRESSORS.get(dictionaryId);
        if (cached == null) {
            throw new SQLException("Compression dictionary " + dictionaryId + " was not loaded before use");
        }
        return Zstd.compress(data, cached);
    }

    /**
     * Compresses a segment payload with the given dictionary.
     *
     * @param data
     *            the payload to compress
     * @param dictionaryId
     *            the dictionary to use, or 0 for none
     * @param connection
     *            an open connection, used to load the dictionary if it is not cached
     * @return the compressed payload
     * @throws SQLException
     *             if the dictionary cannot be loaded
     */
    public static byte[] compress(byte[] data, int dictionaryId, Connection connection) throws SQLException {
        if (data == null || data.length == 0 || !BlobCompression.isAvailable()) {
            return data;
        }
        if (dictionaryId <= 0) {
            return Zstd.compress(data, BlobCompression.storageLevel());
        }
        return Zstd.compress(data, compressor(dictionaryId, connection));
    }

    /**
     * Decompresses a segment frame.
     *
     * @param data
     *            the stored frame
     * @param originalSize
     *            the size the frame decompresses to
     * @param dictionaryId
     *            the dictionary the frame was written with, or 0 for none
     * @param connection
     *            an open connection, used to load the dictionary if it is not cached
     * @return the decompressed frame
     * @throws SQLException
     *             if the dictionary cannot be loaded
     */
    public static byte[] decompress(byte[] data, int originalSize, int dictionaryId, Connection connection) throws SQLException {
        if (data == null) {
            return new byte[0];
        }
        if (originalSize == data.length && !isCompressed(data)) {
            return data;
        }
        if (dictionaryId <= 0) {
            return Zstd.decompress(data, originalSize);
        }
        return Zstd.decompress(data, decompressor(dictionaryId, connection), originalSize);
    }

    private static boolean isCompressed(byte[] data) {
        return data.length >= 4 && (data[0] & 0xFF) == 0x28 && (data[1] & 0xFF) == 0xB5 && (data[2] & 0xFF) == 0x2F && (data[3] & 0xFF) == 0xFD;
    }

    private static ZstdDictCompress compressor(int dictionaryId, Connection connection) throws SQLException {
        ZstdDictCompress cached = COMPRESSORS.get(dictionaryId);
        if (cached != null) {
            return cached;
        }
        ZstdDictCompress created = new ZstdDictCompress(load(dictionaryId, connection), BlobCompression.storageLevel());
        COMPRESSORS.put(dictionaryId, created);
        return created;
    }

    private static ZstdDictDecompress decompressor(int dictionaryId, Connection connection) throws SQLException {
        ZstdDictDecompress cached = DECOMPRESSORS.get(dictionaryId);
        if (cached != null) {
            return cached;
        }
        ZstdDictDecompress created = new ZstdDictDecompress(load(dictionaryId, connection));
        DECOMPRESSORS.put(dictionaryId, created);
        return created;
    }

    private static byte[] load(int dictionaryId, Connection connection) throws SQLException {
        String query = "SELECT data FROM " + ConfigHandler.prefix + "segment_dict WHERE dict_id = ? LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, dictionaryId);
            try (ResultSet results = statement.executeQuery()) {
                if (!results.next()) {
                    throw new SQLException("Compression dictionary " + dictionaryId + " is missing");
                }
                return results.getBytes(1);
            }
        }
    }
}
