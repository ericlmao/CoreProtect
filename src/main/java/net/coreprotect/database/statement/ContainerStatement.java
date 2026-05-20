package net.coreprotect.database.statement;

import java.sql.PreparedStatement;
import java.sql.Types;

import net.coreprotect.config.Config;
import net.coreprotect.database.payload.PayloadStorage;
import net.coreprotect.utility.ItemUtils;

public class ContainerStatement {

    private ContainerStatement() {
        throw new IllegalStateException("Database class");
    }

    public static void insert(PreparedStatement preparedStmt, int batchCount, int time, int id, int wid, int x, int y, int z, int type, int data, int amount, Object metadata, int action, int rolledBack) {
        try {
            byte[] byteData = ItemUtils.convertByteData(metadata);
            long payloadId = 0L;
            if (PayloadStorage.shouldWritePayloads()) {
                payloadId = PayloadStorage.store(preparedStmt.getConnection(), byteData);
                if (!Config.getGlobal().SQLITE_PAYLOAD_KEEP_LEGACY_INLINE_VALUES) {
                    byteData = null;
                }
            }
            preparedStmt.setInt(1, time);
            preparedStmt.setInt(2, id);
            preparedStmt.setInt(3, wid);
            preparedStmt.setInt(4, x);
            preparedStmt.setInt(5, y);
            preparedStmt.setInt(6, z);
            preparedStmt.setInt(7, type);
            preparedStmt.setInt(8, data);
            preparedStmt.setInt(9, amount);
            preparedStmt.setObject(10, byteData);
            preparedStmt.setInt(11, action);
            preparedStmt.setInt(12, rolledBack);
            if (!Config.getGlobal().MYSQL) {
                if (payloadId > 0) {
                    preparedStmt.setLong(13, payloadId);
                }
                else {
                    preparedStmt.setNull(13, Types.INTEGER);
                }
            }
            preparedStmt.addBatch();

            if (batchCount > 0 && batchCount % 1000 == 0) {
                preparedStmt.executeBatch();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
