package net.coreprotect.database.statement;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.List;

import net.coreprotect.config.Config;
import net.coreprotect.database.payload.PayloadStorage;
import net.coreprotect.utility.BlockUtils;
import net.coreprotect.utility.ItemUtils;

public class BlockStatement {

    private BlockStatement() {
        throw new IllegalStateException("Database class");
    }

    public static void insert(PreparedStatement preparedStmt, int batchCount, int time, int id, int wid, int x, int y, int z, int type, int data, List<Object> meta, String blockData, int action, int rolledBack) {
        try {
            byte[] bBlockData = BlockUtils.stringToByteData(blockData, type);
            byte[] byteData = null;

            if (meta != null) {
                byteData = ItemUtils.convertByteData(meta);
            }
            long metaPayloadId = 0L;
            long blockDataPayloadId = 0L;
            if (PayloadStorage.shouldWritePayloads()) {
                metaPayloadId = PayloadStorage.store(preparedStmt.getConnection(), byteData);
                blockDataPayloadId = PayloadStorage.store(preparedStmt.getConnection(), bBlockData);
                if (!Config.getGlobal().SQLITE_PAYLOAD_KEEP_LEGACY_INLINE_VALUES) {
                    byteData = null;
                    bBlockData = null;
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
            preparedStmt.setObject(9, byteData);
            preparedStmt.setObject(10, bBlockData);
            preparedStmt.setInt(11, action);
            preparedStmt.setInt(12, rolledBack);
            if (!Config.getGlobal().MYSQL) {
                if (metaPayloadId > 0) {
                    preparedStmt.setLong(13, metaPayloadId);
                }
                else {
                    preparedStmt.setNull(13, Types.INTEGER);
                }
                if (blockDataPayloadId > 0) {
                    preparedStmt.setLong(14, blockDataPayloadId);
                }
                else {
                    preparedStmt.setNull(14, Types.INTEGER);
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
