package net.coreprotect.thread;

import net.coreprotect.language.Language;

/**
 * Placeholder for the upstream network thread.
 *
 * <p>
 * This build is self contained: it never contacts an update server, a licence server or a
 * translation service, so nothing here does any work. The class remains so that the startup code
 * and the commands that ask about updates keep compiling and simply find nothing to report.
 * </p>
 */
public class NetworkHandler extends Language implements Runnable {

    public NetworkHandler(boolean startup, boolean background) {
        // Kept so existing call sites still compile; both settings are ignored.
    }

    /**
     * @return null, since this build never asks anyone what the latest version is
     */
    public static String latestVersion() {
        return null;
    }

    /**
     * @return null, since this build has no separate editions
     */
    public static String latestEdgeVersion() {
        return null;
    }

    /**
     * @return null, since this build does not use licence keys
     */
    public static String donationKey() {
        return null;
    }

    @Override
    public void run() {
        // This build never contacts an update, licence or translation server. Language files are
        // read from the plugin folder, and there is nothing to check for or report.
    }
}
