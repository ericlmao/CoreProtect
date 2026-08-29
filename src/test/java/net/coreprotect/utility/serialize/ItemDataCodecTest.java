package net.coreprotect.utility.serialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Covers the compact encoding used for item and container payloads, which is the shape
 * {@code ItemMetaHandler} produces.
 */
class ItemDataCodecTest {

    @Test
    void roundTripsItemMetadata() {
        Object metadata = sampleMetadata(7);
        byte[] encoded = ItemDataCodec.encode(metadata);

        assertTrue(ItemDataCodec.isEncoded(encoded));
        assertEquals(metadata, ItemDataCodec.decode(encoded));
    }

    @Test
    void isToldApartFromTheOtherStoredFormats() {
        byte[] encoded = ItemDataCodec.encode(sampleMetadata(1));
        byte[] javaSerialized = { (byte) 0xAC, (byte) 0xED, 0x00, 0x05 };

        assertFalse(BlockMetaCodec.isEncoded(encoded), "item payloads are not read as block metadata");
        assertFalse(ItemDataCodec.isEncoded(javaSerialized));
        assertFalse(BlobCompression.isCompressed(encoded));
    }

    @Test
    void storesItemMetadataInFarFewerBytesThanJavaSerialization() throws Exception {
        long compact = 0;
        long portable = 0;
        for (int index = 0; index < 200; index++) {
            Object metadata = sampleMetadata(index);
            compact = compact + ItemDataCodec.encode(metadata).length;
            portable = portable + javaSerialize(metadata).length;
        }

        assertTrue(compact * 2 < portable, "compact encoding should more than halve the size, was " + compact + " against " + portable);
    }

    private static byte[] javaSerialize(Object value) throws Exception {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(); ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
            output.flush();
            return bytes.toByteArray();
        }
    }

    /** Builds a payload with the nesting and key names real item metadata has. */
    private static Object sampleMetadata(int index) {
        List<Object> metadata = new ArrayList<>();
        List<Object> stack = new ArrayList<>();
        Map<String, Object> meta = new LinkedHashMap<>();

        meta.put("meta-type", "UNSPECIFIC");
        meta.put("display-name", "Item " + (index % 50));

        Map<String, Object> enchants = new LinkedHashMap<>();
        enchants.put("minecraft:sharpness", index % 5);
        enchants.put("minecraft:unbreaking", 3);
        meta.put("enchants", enchants);

        meta.put("lore", Arrays.asList("Forged in the nether", "A relic of the old world"));
        meta.put("Damage", index % 250);
        meta.put("repair-cost", index % 40);

        stack.add(meta);
        metadata.add(stack);
        return metadata;
    }
}
