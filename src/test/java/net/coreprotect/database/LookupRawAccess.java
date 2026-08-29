package net.coreprotect.database;

import java.lang.reflect.Method;
import java.util.List;

/** Reaches the package private planning helpers so tests can check them directly. */
final class LookupRawAccess {

    private LookupRawAccess() {
        throw new IllegalStateException("Utility class");
    }

    static String[] unionTables(List<Integer> actionList, boolean lookup, String queryTable) throws Exception {
        Method method = LookupRaw.class.getDeclaredMethod("unionTables", List.class, boolean.class, String.class);
        method.setAccessible(true);
        return (String[]) method.invoke(null, actionList, lookup, queryTable);
    }
}
