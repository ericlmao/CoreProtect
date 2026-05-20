package net.coreprotect.database.payload;

public class RawPayloadCodec implements PayloadCodec {

    public static final int ID = 0;

    @Override
    public int id() {
        return ID;
    }

    @Override
    public byte[] compress(byte[] raw) {
        return raw;
    }

    @Override
    public byte[] decompress(byte[] stored, int uncompressedSize) {
        return stored;
    }
}
