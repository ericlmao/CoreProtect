package net.coreprotect.utility.serialize;

/**
 * Compact binary encoding for the item and container payloads stored in
 * {@code container.metadata}, {@code entity_container.metadata} and {@code item.data}.
 *
 * <p>
 * Those payloads are trees of lists, maps, strings and numbers produced by
 * {@link ItemMetaHandler#serialize}, which is exactly the value universe the block metadata codec
 * already encodes. This class is a facade over that encoder with its own header, so an item payload
 * and a block metadata payload can always be told apart by their leading bytes.
 * </p>
 */
public final class ItemDataCodec {

    private ItemDataCodec() {
        throw new IllegalStateException("Codec class");
    }

    /**
     * @param data
     *            the payload to encode
     * @return the encoded payload
     */
    public static byte[] encode(Object data) {
        return BlockMetaCodec.encodeItemData(data);
    }

    /**
     * @param encoded
     *            the encoded payload
     * @return the decoded payload
     */
    public static Object decode(byte[] encoded) {
        return BlockMetaCodec.decodeItemData(encoded);
    }

    /**
     * @param data
     *            the stored payload, may be null
     * @return true if the payload uses this codec
     */
    public static boolean isEncoded(byte[] data) {
        return BlockMetaCodec.isItemDataEncoded(data);
    }
}
