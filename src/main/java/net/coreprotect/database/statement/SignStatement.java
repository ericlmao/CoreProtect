package net.coreprotect.database.statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bukkit.block.BlockState;
import org.bukkit.block.Sign;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.ConsumerWriteBatch;
import net.coreprotect.database.Database;
import net.coreprotect.database.LocationQuery;
import net.coreprotect.database.lookup.InspectorSource;
import net.coreprotect.model.SignState;
import net.coreprotect.model.action.SignActions;
import net.coreprotect.model.rollback.SignRollbackChange;
import net.coreprotect.utility.ErrorReporter;

public class SignStatement {

    private SignStatement() {
        throw new IllegalStateException("Database class");
    }

    public static void insert(ConsumerWriteBatch batch, int batchCount, int time, int id, int wid, int x, int y, int z, int action, int color, int colorSecondary, int data, int waxed, int face, String line1, String line2, String line3, String line4, String line5, String line6, String line7, String line8) {
        try {
            batch.addSign(batchCount, time, id, wid, x, y, z, action, color, colorSecondary, data, waxed, face, new String[] { line1, line2, line3, line4, line5, line6, line7, line8 });
        }
        catch (Exception e) {
            Database.handleWriteFailure(e);
        }
    }

    /**
     * Puts a logged sign's colors, glow, text and wax back on the sign, from the rows given.
     *
     * @param resultSet
     *            the logged state to restore, which is closed once read
     * @param block
     *            the sign
     */
    public static void getData(ResultSet resultSet, BlockState block) {
        try {
            if (!(block instanceof Sign)) {
                resultSet.close();
                return;
            }

            try (ResultSet rows = resultSet) {
                while (rows.next()) {
                    SignState.read(rows).apply((Sign) block);
                }
            }
        }
        catch (Exception e) {
            ErrorReporter.report(e);
        }
    }

    public static void loadRollbackStates(Statement statement, List<Object[]> rows) throws SQLException {
        if (ConfigHandler.databaseType.isSQLite()) {
            loadPackedRollbackStates(statement, rows);
            return;
        }
        for (int start = 0; start < rows.size(); start += 64) {
            int end = Math.min(start + 64, rows.size());
            Map<Long, Object[]> selected = new HashMap<>();
            StringBuilder query = new StringBuilder();
            for (int index = start; index < end; index++) {
                Object[] row = rows.get(index);
                long id = ((Number) row[0]).longValue();
                selected.put(id, row);
                if (query.length() > 0) {
                    query.append(" UNION ALL ");
                }
                query.append("SELECT previous_sign.*, ").append(id).append(" AS after_id FROM (SELECT rowid, time, ")
                        .append(ConfigHandler.databaseType.getUserColumn()).append(" AS actor, action, face, color, color_secondary, data, waxed, line_1, line_2, line_3, line_4, line_5, line_6, line_7, line_8 FROM ")
                        .append(ConfigHandler.prefix).append("sign WHERE ").append(LocationQuery.predicate("wid", "=" + row[10]))
                        .append(" AND ").append(LocationQuery.predicate("x", "=" + row[3])).append(" AND y=").append(row[4])
                        .append(" AND ").append(LocationQuery.predicate("z", "=" + row[5]))
                        .append(" AND rowid<").append(id).append(" ORDER BY rowid DESC LIMIT 1) previous_sign");
            }
            try (ResultSet result = statement.executeQuery(query.toString())) {
                while (result.next()) {
                    Object[] row = selected.get(result.getLong("after_id"));
                    applyPreviousState(row, result);
                }
            }
        }
    }

    /**
     * Reads the sign row logged before each rolled back one, for SQLite. Sign rows are packed into
     * compressed storage minutes after they are logged, so the previous row is read through an
     * {@link InspectorSource} at the sign's position, which reads the live and the packed rows
     * together, rather than from the live table alone.
     */
    private static void loadPackedRollbackStates(Statement statement, List<Object[]> rows) throws SQLException {
        String columns = "rowid, time, " + ConfigHandler.databaseType.getUserColumn() + " AS actor, action, face, color, color_secondary, data, waxed, line_1, line_2, line_3, line_4, line_5, line_6, line_7, line_8";
        for (Object[] row : rows) {
            long id = ((Number) row[0]).longValue();
            int worldId = ((Number) row[10]).intValue();
            int x = ((Number) row[3]).intValue();
            int z = ((Number) row[5]).intValue();
            String where = "wid = " + worldId + " AND x = " + x + " AND z = " + z + " AND y = " + row[4] + " AND rowid < " + id;
            try (InspectorSource source = InspectorSource.open(statement.getConnection(), "sign", worldId, x, x, z, z, 0);
                    ResultSet result = source.page(columns, where, "rowid DESC", 0, 1)) {
                if (result.next()) {
                    applyPreviousState(row, result);
                }
            }
            catch (SQLException e) {
                throw e;
            }
            catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    private static void applyPreviousState(Object[] row, ResultSet result) throws SQLException {
        SignRollbackChange change = (SignRollbackChange) row[14];
        int action = result.getInt("action");
        int elapsed = (Integer) row[1] - result.getInt("time");
        if ((action == SignActions.BEFORE || action == SignActions.BREAK && elapsed == 1)
                && result.getInt("actor") == (Integer) row[2] && (result.getInt("face") == 0) == change.isFront()) {
            change.setBefore(SignState.read(result));
        }
    }
}
