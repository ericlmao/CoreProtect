package net.coreprotect.consumer.process;

import java.sql.ResultSet;

import net.coreprotect.database.lookup.InspectorSource;

/**
 * Lets tests elsewhere run the query a sign update restores its text from, which is package private.
 */
public final class SignUpdateAccess {

    private SignUpdateAccess() {
        throw new IllegalStateException("Test access class");
    }

    public static ResultSet loggedText(InspectorSource source, int userId, int worldId, int x, int y, int z, int action, int time) throws Exception {
        return SignUpdateProcess.loggedText(source, userId, worldId, x, y, z, action, time);
    }
}
