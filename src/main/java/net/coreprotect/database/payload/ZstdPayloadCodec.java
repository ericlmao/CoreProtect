package net.coreprotect.database.payload;

import com.github.luben.zstd.Zstd;

public class ZstdPayloadCodec implements PayloadCodec {

    public static final int ID = 1;

    private final int level;

    public ZstdPayloadCodec(int level) {
        this.level = level;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public byte[] compress(byte[] raw) throws Exception {
        return Zstd.compress(raw, level);
    }

    @Override
    public byte[] decompress(byte[] stored, int uncompressedSize) throws Exception {
        byte[] decompressed = Zstd.decompress(stored, uncompressedSize);
        if (decompressed.length != uncompressedSize) {
            throw new IllegalStateException("Zstd decompressed size mismatch: expected " + uncompressedSize + " bytes, got " + decompressed.length);
        }
        return decompressed;
    }
}
