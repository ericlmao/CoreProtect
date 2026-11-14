package net.coreprotect.consumer.process;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import org.bukkit.block.BlockState;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.lookup.InspectorSource;
import net.coreprotect.database.statement.SignStatement;
import net.coreprotect.utility.BlockUtils;
import net.coreprotect.utility.WorldUtils;

class SignUpdateProcess {

    static void process(Statement statement, Object object, String user, int action, int time) {
        /*
         * We're switching blocks around quickly.
         * This block could already be removed again by the time the server tries to modify it.
         * Ignore any errors.
         */
        if (object instanceof BlockState) {
            BlockState block = (BlockState) object;
            int x = block.getX();
            int y = block.getY();
            int z = block.getZ();
            int wid = WorldUtils.getWorldId(block.getWorld().getName());
            int userid = ConfigHandler.playerIdCache.get(user.toLowerCase(Locale.ROOT));
            // The sign's earlier text has usually been packed into compressed storage by now, so it
            // is read from there as well as from the live rows.
            try (InspectorSource source = InspectorSource.open(statement.getConnection(), "sign", wid, x, x, z, z, 0)) {
                SignStatement.getData(loggedText(source, userid, wid, x, y, z, action, time), block);
            }
            catch (Exception e) {
                // As above: the block may already have changed again, and nothing is lost by skipping it.
            }
            BlockUtils.updateBlock(block);
        }
    }

    /**
     * Reads the logged sign text an update restores: for a rollback, the player's last text at the
     * sign before the time, and otherwise the first at or after it. It is one row, and only that row
     * is read out of compressed storage, however much the sign has logged.
     *
     * @return the row, which the caller closes
     */
    static ResultSet loggedText(InspectorSource source, int userId, int worldId, int x, int y, int z, int action, int time) throws Exception {
        String columns = "color, color_secondary, data, waxed, face, line_1, line_2, line_3, line_4, line_5, line_6, line_7, line_8";
        String place = ConfigHandler.databaseType.getUserColumn() + "=" + userId + " AND wid=" + worldId + " AND x=" + x + " AND z=" + z + " AND y=" + y;
        if (action == 0) {
            return source.page(columns, place + " AND time < " + time, ConfigHandler.getDescendingEventOrder(), 0, 1);
        }
        return source.page(columns, place + " AND time >= " + time, ConfigHandler.getAscendingEventOrder(), 0, 1);
    }
}
