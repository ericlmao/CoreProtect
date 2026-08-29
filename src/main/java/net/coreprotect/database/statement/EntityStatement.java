package net.coreprotect.database.statement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.bukkit.attribute.Attribute;
import org.bukkit.block.BlockState;
import org.bukkit.util.io.BukkitObjectInputStream;
import org.bukkit.util.io.BukkitObjectOutputStream;

import net.coreprotect.bukkit.BukkitAdapter;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.ColdBlobStore;
import net.coreprotect.database.ConsumerWriteBatch;
import net.coreprotect.database.Database;
import net.coreprotect.database.DatabaseType;
import net.coreprotect.utility.DatabaseUtils;
import net.coreprotect.utility.ErrorReporter;
import net.coreprotect.utility.serialize.BlobCompression;
import net.coreprotect.utility.serialize.EntityDataCodec;
import net.coreprotect.utility.serialize.EntityDataCodec.Kind;

public class EntityStatement {

    private static final int SELECT_BATCH_SIZE = 500;

    private EntityStatement() {
        throw new IllegalStateException("Database class");
    }

    public static int insert(ConsumerWriteBatch batch, int time, List<Object> data) {
        try {
            byte[] serializedData = serializeData(data, Kind.ENTITY);
            if (serializedData == null) {
                return 0;
            }
            return batch.addEntity(time, serializedData);
        }
        catch (Exception e) {
            Database.handleWriteFailure(e);
        }

        return 0;
    }

    public static byte[] serializeData(List<Object> data) {
        return serializeData(data, Kind.ENTITY_SPAWN);
    }

    public static byte[] serializeData(List<Object> data, Kind kind) {
        return serializeData(data, kind, ConfigHandler.databaseType);
    }

    public static byte[] serializeData(List<Object> data, Kind kind, DatabaseType databaseType) {
        if (data == null) {
            return null;
        }

        try {
            return serializeDataStrict(data, kind, databaseType);
        }
        catch (Exception e) {
            ErrorReporter.report(e, ConfigHandler.EDITION_BRANCH.contains("-dev"));
            return null;
        }
    }

    public static byte[] transcodeData(byte[] data, Kind kind, DatabaseType targetType) throws Exception {
        if (data == null) {
            return null;
        }
        if (EntityDataCodec.isEncoded(data)) {
            if (targetType.isColumnar()) {
                return EntityDataCodec.canonicalize(kind, data);
            }
            return serializeLegacyData(sanitizeData(EntityDataCodec.decode(kind, data)));
        }
        return serializeDataStrict(deserializeDataStrict(data, kind), kind, targetType);
    }

    private static byte[] serializeDataStrict(List<Object> data, Kind kind, DatabaseType databaseType) throws Exception {
        if (databaseType.isColumnar()) {
            return EntityDataCodec.encode(kind, data);
        }
        if (databaseType.isSQLite()) {
            return BlobCompression.compress(EntityDataCodec.encode(kind, data));
        }
        return serializeLegacyData(sanitizeData(data));
    }

    private static byte[] serializeLegacyData(List<Object> data) throws Exception {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream(); BukkitObjectOutputStream objectOutput = new BukkitObjectOutputStream(output)) {
            objectOutput.writeObject(data);
            objectOutput.flush();
            return BlobCompression.compress(output.toByteArray());
        }
    }

    private static List<Object> sanitizeData(List<Object> data) {
        List<Object> result = new ArrayList<>(data.size());
        for (Object value : data) {
            result.add(sanitizeValue(value));
        }

        return result;
    }

    private static Object sanitizeValue(Object value) {
        if (value instanceof Attribute) {
            return BukkitAdapter.ADAPTER.getRegistryKey(value);
        }
        else if (value instanceof List<?>) {
            List<?> list = (List<?>) value;
            List<Object> result = new ArrayList<>(list.size());
            for (Object item : list) {
                result.add(sanitizeValue(item));
            }

            return result;
        }
        else if (value instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) value;
            Map<Object, Object> result = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(sanitizeValue(entry.getKey()), sanitizeValue(entry.getValue()));
            }

            return result;
        }

        return value;
    }

    public static List<Object> getData(Statement statement, BlockState block, String query) {
        List<Object> result = new ArrayList<>();

        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                result = readData(resultSet, "data", Kind.ENTITY);
            }

            resultSet.close();
        }
        catch (Exception e) { // only print exception on development branch
            ErrorReporter.report(e, ConfigHandler.EDITION_BRANCH.contains("-dev"));
        }

        return result;
    }

    public static Map<Integer, List<Object>> loadData(Connection connection, Collection<Integer> rowIds) throws SQLException {
        Map<Integer, List<Object>> result = new HashMap<>();
        if (rowIds.isEmpty()) {
            return result;
        }

        List<Integer> ids = new ArrayList<>(rowIds);
        for (int offset = 0; offset < ids.size(); offset += SELECT_BATCH_SIZE) {
            int end = Math.min(offset + SELECT_BATCH_SIZE, ids.size());
            StringJoiner placeholders = new StringJoiner(",");
            for (int ignored = offset; ignored < end; ignored++) {
                placeholders.add("?");
            }

            String query = "SELECT rowid AS id,data FROM " + ConfigHandler.prefix + "entity WHERE rowid IN(" + placeholders + ")";
            List<Long> packedAway = new ArrayList<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                for (int index = offset; index < end; index++) {
                    preparedStatement.setInt(index - offset + 1, ids.get(index));
                }
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        byte[] stored = DatabaseUtils.getBlobBytes(resultSet, "data");
                        if (stored == null || stored.length == 0) {
                            // The row is still here but its blob has been packed away with those of
                            // the rows around it.
                            packedAway.add((long) resultSet.getInt("id"));
                            continue;
                        }
                        List<Object> data = deserializeData(stored, Kind.ENTITY);
                        if (!data.isEmpty()) {
                            result.put(resultSet.getInt("id"), data);
                        }
                    }
                }
            }

            for (Map.Entry<Long, byte[]> entry : ColdBlobStore.load(connection, "entity", packedAway).entrySet()) {
                List<Object> data = deserializeData(entry.getValue(), Kind.ENTITY);
                if (!data.isEmpty()) {
                    result.put(entry.getKey().intValue(), data);
                }
            }
        }
        return result;
    }

    /**
     * Reads one entity's data by row id, wherever it is held.
     *
     * @param statement
     *            an open statement
     * @param rowId
     *            the entity row
     * @return the entity data, or an empty list when there is none
     */
    public static List<Object> getData(Statement statement, int rowId) {
        try {
            String query = "SELECT data FROM " + ConfigHandler.prefix + "entity WHERE rowid=" + rowId + " LIMIT 1 OFFSET 0";
            byte[] stored = null;
            try (ResultSet resultSet = statement.executeQuery(query)) {
                if (resultSet.next()) {
                    stored = DatabaseUtils.getBlobBytes(resultSet, "data");
                }
            }
            if (stored == null || stored.length == 0) {
                stored = ColdBlobStore.load(statement.getConnection(), "entity", rowId);
            }
            return deserializeData(stored, Kind.ENTITY);
        }
        catch (Exception e) {
            ErrorReporter.report(e, ConfigHandler.EDITION_BRANCH.contains("-dev"));
            return new ArrayList<>();
        }
    }

    public static List<Object> deserializeData(byte[] data) {
        return deserializeData(data, Kind.ENTITY_SPAWN);
    }

    public static List<Object> deserializeData(byte[] data, Kind kind) {
        List<Object> result = new ArrayList<>();
        if (data == null) {
            return result;
        }

        try {
            return deserializeDataStrict(data, kind);
        }
        catch (Exception e) {
            ErrorReporter.report(e, ConfigHandler.EDITION_BRANCH.contains("-dev"));
            return result;
        }
    }

    public static List<Object> readData(ResultSet resultSet, String column, Kind kind) throws SQLException {
        return deserializeData(DatabaseUtils.getBlobBytes(resultSet, column), kind);
    }

    /**
     * Reads a blob from a result set, looking in the packed groups when the row no longer carries it.
     *
     * @param resultSet
     *            positioned on the row
     * @param column
     *            the blob column
     * @param kind
     *            what the blob holds
     * @param connection
     *            an open connection, used only when the blob has been packed away
     * @param table
     *            the unprefixed table the row came from
     * @param rowId
     *            the row id
     * @return the data, or an empty list when there is none
     */
    public static List<Object> readData(ResultSet resultSet, String column, Kind kind, Connection connection, String table, long rowId) throws SQLException {
        byte[] stored = DatabaseUtils.getBlobBytes(resultSet, column);
        if (stored == null || stored.length == 0) {
            stored = ColdBlobStore.load(connection, table, rowId);
        }
        return deserializeData(stored, kind);
    }

    private static List<Object> deserializeDataStrict(byte[] storedData, Kind kind) throws Exception {
        byte[] data = BlobCompression.decompress(storedData);
        if (EntityDataCodec.isEncoded(data)) {
            return EntityDataCodec.decode(kind, data);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data); BukkitObjectInputStream input = new BukkitObjectInputStream(bais)) {
            Object value = input.readObject();
            if (!(value instanceof List<?>)) {
                throw new IllegalArgumentException("Entity data root is not a list");
            }
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) value;
            return values;
        }
    }
}
