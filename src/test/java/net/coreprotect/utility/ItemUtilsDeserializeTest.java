package net.coreprotect.utility;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import net.coreprotect.utility.serialize.BlobCompression;
import net.coreprotect.utility.serialize.BlockMetaCodec;
import net.coreprotect.utility.serialize.ItemDataCodec;

/**
 * Inventory lookups merge block placements in, so the payload reader has to accept block metadata
 * as well as item metadata.
 */
class ItemUtilsDeserializeTest {

    @Test
    void readsBlockEncodedMetadata() throws Exception {
        List<Object> metadata = blockMetadata();
        byte[] encoded = BlockMetaCodec.encode(metadata);

        assertEquals(BlockMetaCodec.decode(encoded), ItemUtils.deserializeByteData(encoded));
    }

    @Test
    void readsCompressedBlockEncodedMetadata() throws Exception {
        List<Object> metadata = blockMetadata();
        byte[] encoded = BlockMetaCodec.encode(metadata);

        assertEquals(BlockMetaCodec.decode(encoded), ItemUtils.deserializeByteData(BlobCompression.compress(encoded)));
    }

    @Test
    void stillReadsItemEncodedMetadata() throws Exception {
        List<Object> metadata = blockMetadata();

        assertEquals(metadata, ItemUtils.deserializeByteData(ItemDataCodec.encode(metadata)));
    }

    private static List<Object> blockMetadata() {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("v", 3953);
        item.put("type", "DIAMOND_SWORD");
        item.put("amount", 1);
        List<Object> metadata = new ArrayList<>();
        metadata.add(item);
        return metadata;
    }
}
