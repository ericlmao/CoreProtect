package net.coreprotect.utility.serialize;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.github.luben.zstd.Zstd;

class BlobCompressionTest {

    @Test
    void decompressesFramesAndPassesEverythingElseThrough() {
        byte[] original = new byte[4096];
        Arrays.fill(original, (byte) 'a');
        byte[] frame = Zstd.compress(original, 19);

        assertTrue(BlobCompression.isCompressed(frame));
        assertArrayEquals(original, BlobCompression.decompress(frame));
        assertArrayEquals(original, BlobCompression.decompress(BlobCompression.decompress(frame)));
    }

    @Test
    void leavesLegacyAndCodecBlobsAlone() {
        byte[] javaSerialized = { (byte) 0xAC, (byte) 0xED, 0x00, 0x05, 0x73, 0x72 };
        byte[] blockCodec = "CB".getBytes(StandardCharsets.US_ASCII);
        byte[] entityCodec = "CP".getBytes(StandardCharsets.US_ASCII);

        assertFalse(BlobCompression.isCompressed(javaSerialized));
        assertFalse(BlobCompression.isCompressed(blockCodec));
        assertFalse(BlobCompression.isCompressed(entityCodec));
        assertFalse(BlobCompression.isCompressed(null));
        assertArrayEquals(javaSerialized, BlobCompression.decompress(javaSerialized));
        assertArrayEquals(blockCodec, BlobCompression.decompress(blockCodec));
    }
}
