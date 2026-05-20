package net.coreprotect.database.payload;

public interface PayloadCodec {

    int id();

    byte[] compress(byte[] raw) throws Exception;

    byte[] decompress(byte[] stored, int uncompressedSize) throws Exception;
}
