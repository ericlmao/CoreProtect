package net.coreprotect.database.statement;

import java.sql.PreparedStatement;
import java.sql.Types;

import net.coreprotect.config.Config;
import net.coreprotect.database.payload.PayloadStorage;
import net.coreprotect.utility.ItemUtils;

public class ItemStatement {

    private ItemStatement() {
        throw new IllegalStateException("Database class");
    }

    public static void insert(PreparedStatement preparedStmt, int batchCount, int time, int id, int wid, int x, int y, int z, int type, Object data, int amount, int action) {
        try {
            byte[] byteData = ItemUtils.convertByteData(data);
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
            preparedStmt.setObject(8, byteData);
            preparedStmt.setInt(9, amount);
            preparedStmt.setInt(10, action);
            preparedStmt.setInt(11, 0); // rolled_back
            if (!Config.getGlobal().MYSQL) {
                if (payloadId > 0) {
                    preparedStmt.setLong(12, payloadId);
                }
                else {
                    preparedStmt.setNull(12, Types.INTEGER);
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
