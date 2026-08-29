package net.coreprotect.utility.serialize;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;

/**
 * The shared dictionaries used to compress the blobs stored in the relational tables.
 *
 * <p>
 * Entity data is stored one blob to a row, and each blob is only a couple of kilobytes. Compressed
 * on its own a blob of that size gives a compressor almost nothing to work with, because what
 * repeats in this data repeats <em>between</em> rows rather than inside any one of them: the same
 * entity types, the same attribute names, the same class descriptions, over and over. Measured on a
 * real server's data, compressing each blob by itself saves a factor of three, while the same blobs
 * compressed against a dictionary trained on a sample of them save a factor of thirty nine.
 * </p>
 *
 * <p>
 * A dictionary supplies that shared structure up front, so each blob stays independently readable
 * and a lookup can still fetch one row without touching any other. Zstandard records in every frame
 * which dictionary produced it, so blobs written before a dictionary existed, or with an earlier
 * one, keep reading back normally. Dictionaries are never removed for that reason.
 * </p>
 */
public final class BlobDictionary {

    /** Digested dictionaries for reading, by the identifier Zstandard records in each frame. */
    private static final Map<Long, ZstdDictDecompress> READERS = new ConcurrentHashMap<>();

    /** Digested dictionaries for writing, by compression level. */
    private static final Map<Integer, ZstdDictCompress> WRITERS = new ConcurrentHashMap<>();

    /** The dictionary new blobs are written with, or null when none has been trained yet. */
    private static volatile byte[] current;

    private BlobDictionary() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Forgets every dictionary, which is required when the database is reopened.
     */
    public static void clear() {
        READERS.clear();
        WRITERS.clear();
        current = null;
    }

    /**
     * Makes a dictionary available.
     *
     * @param dictionary
     *            the trained dictionary
     * @param writeWithIt
     *            whether new blobs should be compressed with it; a dictionary registered only for
     *            reading still lets blobs written with it earlier be read back
     */
    public static void register(byte[] dictionary, boolean writeWithIt) {
        if (dictionary == null || dictionary.length == 0 || !BlobCompression.isAvailable()) {
            return;
        }

        try {
            long id = Zstd.getDictIdFromDict(dictionary);
            if (id != 0) {
                READERS.computeIfAbsent(id, ignored -> new ZstdDictDecompress(dictionary));
            }
            if (writeWithIt) {
                WRITERS.clear();
                current = dictionary;
            }
        }
        catch (Throwable failure) {
            // A dictionary that cannot be digested is one blobs simply are not written against.
        }
    }

    /**
     * @return true when new blobs are compressed against a dictionary
     */
    public static boolean hasDictionary() {
        return current != null;
    }

    /**
     * @param frame
     *            a compressed blob
     * @return true when the blob was already written with the dictionary now in use, so recompressing
     *         it would only produce the same thing again
     */
    public static boolean isCurrent(byte[] frame) {
        byte[] dictionary = current;
        if (dictionary == null || frame == null) {
            return false;
        }
        try {
            long frameId = Zstd.getDictIdFromFrame(frame);
            return frameId != 0 && frameId == Zstd.getDictIdFromDict(dictionary);
        }
        catch (Throwable failure) {
            return false;
        }
    }

    /**
     * @param level
     *            the compression level to write at
     * @return the dictionary to compress with, or null when there is none
     */
    static ZstdDictCompress writer(int level) {
        byte[] dictionary = current;
        if (dictionary == null) {
            return null;
        }
        return WRITERS.computeIfAbsent(level, value -> new ZstdDictCompress(dictionary, value));
    }

    /**
     * @param frame
     *            a compressed blob
     * @return the dictionary it was written with, or null when it needs none
     */
    static ZstdDictDecompress reader(byte[] frame) {
        long id;
        try {
            id = Zstd.getDictIdFromFrame(frame);
        }
        catch (Throwable failure) {
            return null;
        }
        if (id == 0) {
            return null;
        }

        ZstdDictDecompress reader = READERS.get(id);
        if (reader == null) {
            // The blob names a dictionary this database does not have. Saying so is far better than
            // returning whatever decompressing without it happens to produce.
            throw new IllegalStateException("A database blob was written with compression dictionary " + id + ", which is missing");
        }
        return reader;
    }
}
