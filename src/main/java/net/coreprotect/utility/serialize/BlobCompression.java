package net.coreprotect.utility.serialize;

import java.util.logging.Logger;

import com.github.luben.zstd.Zstd;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;

/**
 * Transparent Zstandard compression for the binary blobs stored in the relational
 * (SQLite and MySQL) tables.
 *
 * <p>
 * Compressed blobs are stored as a standard Zstandard frame, which always begins with the
 * frame magic {@code 28 B5 2F FD}. Legacy blobs are Java serialized objects, which always
 * begin with {@code AC ED}, and the columnar codecs emit their own leading markers, so a
 * stored blob can always be identified without a schema change or a data migration. Data
 * written before compression was enabled therefore continues to read back normally.
 * </p>
 *
 * <p>
 * Columnar backends (DuckDB and ClickHouse) compress their own storage, so blobs written for
 * those backends are left untouched.
 * </p>
 */
public final class BlobCompression {

    /** Zstandard frame magic number, in the order it appears at the start of a frame. */
    private static final byte[] FRAME_MAGIC = { (byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD };

    /** Blobs smaller than this rarely compress well enough to justify the frame overhead. */
    private static final int MINIMUM_COMPRESSED_LENGTH = 64;

    /** Upper bound on a decompressed blob, mirroring {@link BinaryCodecSupport#MAX_ENCODED_LENGTH}. */
    private static final int MAXIMUM_DECOMPRESSED_LENGTH = 32 * 1024 * 1024;

    private static final Logger LOGGER = Logger.getLogger("CoreProtect");

    private static volatile Boolean nativeAvailable;

    private BlobCompression() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Compresses a blob that is about to be written to a relational table.
     *
     * @param data
     *            the serialized blob, may be null
     * @return the compressed blob, or the original blob when compression is disabled,
     *         unavailable, or not worthwhile
     */
    public static byte[] compress(byte[] data) {
        if (data == null || data.length < MINIMUM_COMPRESSED_LENGTH) {
            return data;
        }
        if (ConfigHandler.databaseType != null && ConfigHandler.databaseType.isColumnar()) {
            return data;
        }

        Config config = Config.getGlobal();
        if (config == null || !config.BLOB_COMPRESSION || !isAvailable()) {
            return data;
        }

        try {
            byte[] compressed = Zstd.compress(data, clampLevel(config.HOT_BLOB_COMPRESSION_LEVEL));
            if (compressed.length >= data.length) {
                return data;
            }
            return compressed;
        }
        catch (Throwable failure) {
            if (isMissingNative(failure)) {
                disableAfterNativeFailure(failure);
                return data;
            }
            LOGGER.warning("CoreProtect was unable to compress a database blob, storing it uncompressed: " + failure.getMessage());
            return data;
        }
    }

    /**
     * Decompresses a blob that was read from the database, if it is a Zstandard frame.
     *
     * @param data
     *            the stored blob, may be null
     * @return the original blob contents
     */
    public static byte[] decompress(byte[] data) {
        if (!isCompressed(data)) {
            return data;
        }
        if (!isAvailable()) {
            throw new IllegalStateException("A compressed blob was read, but Zstandard support is unavailable");
        }

        long size = Zstd.decompressedSize(data);
        if (size <= 0 || size > MAXIMUM_DECOMPRESSED_LENGTH) {
            throw new IllegalArgumentException("Compressed blob declares an unusable length of " + size + " bytes");
        }

        return Zstd.decompress(data, (int) size);
    }

    /**
     * @param data
     *            the stored blob, may be null
     * @return true if the blob is a Zstandard frame written by CoreProtect
     */
    public static boolean isCompressed(byte[] data) {
        if (data == null || data.length < FRAME_MAGIC.length) {
            return false;
        }
        for (int i = 0; i < FRAME_MAGIC.length; i++) {
            if (data[i] != FRAME_MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if the Zstandard native library loaded successfully
     */
    public static boolean isAvailable() {
        Boolean available = nativeAvailable;
        if (available != null) {
            return available;
        }

        synchronized (BlobCompression.class) {
            if (nativeAvailable == null) {
                try {
                    Zstd.compressBound(1L);
                    nativeAvailable = Boolean.TRUE;
                }
                catch (Throwable failure) {
                    nativeAvailable = Boolean.FALSE;
                    LOGGER.warning("CoreProtect could not load Zstandard on this system; database blobs will be stored uncompressed: " + failure);
                }
            }
            return nativeAvailable;
        }
    }

    /**
     * Compresses a blob for long term storage, at the level configured for rolled up data.
     *
     * @param data
     *            the payload to compress
     * @return the compressed payload, or the original when compression is unavailable
     */
    public static byte[] compressForStorage(byte[] data) {
        if (data == null || !isAvailable()) {
            return data;
        }

        try {
            byte[] compressed = Zstd.compress(data, clampLevel(Config.getGlobal().BLOB_COMPRESSION_LEVEL));
            return compressed.length >= data.length ? data : compressed;
        }
        catch (Throwable failure) {
            if (isMissingNative(failure)) {
                disableAfterNativeFailure(failure);
            }
            return data;
        }
    }

    /**
     * @return the storage compression level, clamped to the range Zstandard supports
     */
    public static int storageLevel() {
        return clampLevel(Config.getGlobal().BLOB_COMPRESSION_LEVEL);
    }

    private static int clampLevel(int configured) {
        int level = configured;
        if (level < Zstd.minCompressionLevel()) {
            return Zstd.minCompressionLevel();
        }
        if (level > Zstd.maxCompressionLevel()) {
            return Zstd.maxCompressionLevel();
        }
        return level;
    }

    private static boolean isMissingNative(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof UnsatisfiedLinkError || current instanceof NoClassDefFoundError || current instanceof ExceptionInInitializerError) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static void disableAfterNativeFailure(Throwable failure) {
        synchronized (BlobCompression.class) {
            if (!Boolean.FALSE.equals(nativeAvailable)) {
                nativeAvailable = Boolean.FALSE;
                LOGGER.warning("CoreProtect disabled database blob compression after a Zstandard failure: " + failure);
            }
        }
    }
}
