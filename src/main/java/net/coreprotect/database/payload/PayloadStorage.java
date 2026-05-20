package net.coreprotect.database.payload;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import net.coreprotect.config.Config;

public class PayloadStorage {

    public static final String TABLE = "co_payload";

    private PayloadStorage() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean shouldWritePayloads() {
        Config config = Config.getGlobal();
        return !config.MYSQL && config.SQLITE_PAYLOAD_COMPRESSION_ENABLED;
    }

    public static long store(Connection connection, byte[] raw) throws Exception {
        if (raw == null) {
            return 0L;
        }

        Config config = Config.getGlobal();
        byte[] hash = hash(raw);
        if (config.SQLITE_PAYLOAD_DEDUPLICATE) {
            long existing = findExisting(connection, hash, raw);
            if (existing > 0) {
                return existing;
            }
        }

        StoredPayload stored = encode(raw);
        return insertPayload(connection, hash, raw.length, stored);
    }

    public static byte[] resolve(Connection connection, long payloadId, byte[] legacyInline) {
        if (payloadId <= 0) {
            return legacyInline;
        }

        int codec = -1;
        try (PreparedStatement statement = connection.prepareStatement("SELECT codec,uncompressed_size,data FROM " + TABLE + " WHERE id = ? LIMIT 1")) {
            statement.setLong(1, payloadId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    codec = resultSet.getInt("codec");
                    int uncompressedSize = resultSet.getInt("uncompressed_size");
                    byte[] data = resultSet.getBytes("data");
                    return codec(codec).decompress(data, uncompressedSize);
                }
            }
        }
        catch (Exception e) {
            System.err.println("[CoreProtect] Failed to resolve compressed SQLite payload id " + payloadId + " with codec " + codec + ": " + e.getMessage());
        }

        return legacyInline;
    }

    private static StoredPayload encode(byte[] raw) {
        Config config = Config.getGlobal();
        if (!config.SQLITE_PAYLOAD_COMPRESSION_ENABLED) {
            return new StoredPayload(RawPayloadCodec.ID, raw);
        }
        if (raw.length < Math.max(1, config.SQLITE_PAYLOAD_COMPRESSION_MIN_BYTES)) {
            return new StoredPayload(RawPayloadCodec.ID, raw);
        }

        PayloadCodec codec = configuredCodec();
        if (codec.id() == RawPayloadCodec.ID) {
            return new StoredPayload(RawPayloadCodec.ID, raw);
        }

        try {
            byte[] compressed = codec.compress(raw);
            if (config.SQLITE_PAYLOAD_STORE_RAW_IF_LARGER && compressed.length >= raw.length) {
                return new StoredPayload(RawPayloadCodec.ID, raw);
            }
            return new StoredPayload(codec.id(), compressed);
        }
        catch (Exception e) {
            System.err.println("[CoreProtect] Failed to compress SQLite payload; storing raw payload instead: " + e.getMessage());
            return new StoredPayload(RawPayloadCodec.ID, raw);
        }
    }

    private static long findExisting(Connection connection, byte[] hash, byte[] raw) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("SELECT id,codec,uncompressed_size,data FROM " + TABLE + " WHERE hash = ? AND uncompressed_size = ?")) {
            statement.setBytes(1, hash);
            statement.setInt(2, raw.length);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    long id = resultSet.getLong("id");
                    int codec = resultSet.getInt("codec");
                    int uncompressedSize = resultSet.getInt("uncompressed_size");
                    byte[] stored = resultSet.getBytes("data");
                    byte[] decoded = codec(codec).decompress(stored, uncompressedSize);
                    if (Arrays.equals(raw, decoded)) {
                        return id;
                    }

                    System.err.println("[CoreProtect] Suspected SQLite payload hash collision for payload id " + id + "; not reusing it.");
                }
            }
        }

        return 0L;
    }

    private static long insertPayload(Connection connection, byte[] hash, int uncompressedSize, StoredPayload stored) throws Exception {
        String query = "INSERT INTO " + TABLE + " (hash, codec, uncompressed_size, data, created_at) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            statement.setBytes(1, hash);
            statement.setInt(2, stored.codec);
            statement.setInt(3, uncompressedSize);
            statement.setBytes(4, stored.data);
            statement.setInt(5, (int) (System.currentTimeMillis() / 1000L));
            statement.executeUpdate();
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                }
            }
        }
        catch (Exception e) {
            long existing = findExisting(connection, hash, decodeStored(stored, uncompressedSize));
            if (existing > 0) {
                return existing;
            }

            byte[] collisionHash = hashWithCollisionSalt(hash);
            StoredPayload rawStored = new StoredPayload(RawPayloadCodec.ID, decodeStored(stored, uncompressedSize));
            try (PreparedStatement retry = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
                retry.setBytes(1, collisionHash);
                retry.setInt(2, RawPayloadCodec.ID);
                retry.setInt(3, uncompressedSize);
                retry.setBytes(4, rawStored.data);
                retry.setInt(5, (int) (System.currentTimeMillis() / 1000L));
                retry.executeUpdate();
                try (ResultSet generatedKeys = retry.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        System.err.println("[CoreProtect] Stored SQLite payload with collision-safe hash after insert conflict.");
                        return generatedKeys.getLong(1);
                    }
                }
            }
        }

        throw new IllegalStateException("Unable to insert SQLite payload");
    }

    private static byte[] decodeStored(StoredPayload stored, int uncompressedSize) throws Exception {
        return codec(stored.codec).decompress(stored.data, uncompressedSize);
    }

    private static byte[] hash(byte[] raw) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(raw);
    }

    private static byte[] hashWithCollisionSalt(byte[] hash) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(hash);
        digest.update((byte) 0x01);
        digest.update(Long.toString(System.nanoTime()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return digest.digest();
    }

    private static PayloadCodec configuredCodec() {
        String codec = Config.getGlobal().SQLITE_PAYLOAD_COMPRESSION_CODEC;
        if ("zstd".equalsIgnoreCase(codec)) {
            return new ZstdPayloadCodec(Config.getGlobal().SQLITE_PAYLOAD_COMPRESSION_ZSTD_LEVEL);
        }
        return new RawPayloadCodec();
    }

    private static PayloadCodec codec(int codec) {
        if (codec == ZstdPayloadCodec.ID) {
            return new ZstdPayloadCodec(Config.getGlobal().SQLITE_PAYLOAD_COMPRESSION_ZSTD_LEVEL);
        }
        return new RawPayloadCodec();
    }

    public static byte[] hashForTesting(byte[] raw) throws Exception {
        return hash(raw);
    }

    private static final class StoredPayload {
        private final int codec;
        private final byte[] data;

        private StoredPayload(int codec, byte[] data) {
            this.codec = codec;
            this.data = data;
        }
    }
}
