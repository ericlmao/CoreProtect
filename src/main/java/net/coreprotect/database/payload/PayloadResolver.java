package net.coreprotect.database.payload;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class PayloadResolver {

    private PayloadResolver() {
        throw new IllegalStateException("Utility class");
    }

    public static byte[] getBytes(Connection connection, ResultSet resultSet, String inlineColumn, String payloadIdColumn) throws SQLException {
        byte[] legacyInline = resultSet.getBytes(inlineColumn);
        long payloadId = getPayloadId(resultSet, payloadIdColumn);
        return PayloadStorage.resolve(connection, payloadId, legacyInline);
    }

    public static byte[] getBytes(Connection connection, ResultSet resultSet, PayloadColumn payloadColumn) throws SQLException {
        return getBytes(connection, resultSet, payloadColumn.inlineColumn(), payloadColumn.payloadIdColumn());
    }

    private static long getPayloadId(ResultSet resultSet, String payloadIdColumn) throws SQLException {
        if (!hasColumn(resultSet, payloadIdColumn)) {
            return 0L;
        }

        long payloadId = resultSet.getLong(payloadIdColumn);
        return resultSet.wasNull() ? 0L : payloadId;
    }

    private static boolean hasColumn(ResultSet resultSet, String column) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            if (column.equalsIgnoreCase(metaData.getColumnLabel(i))) {
                return true;
            }
        }
        return false;
    }
}
