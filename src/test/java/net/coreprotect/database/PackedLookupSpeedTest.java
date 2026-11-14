package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.BlockState;
import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.lookup.BlockLookup;
import net.coreprotect.listener.channel.PluginChannelHandshakeListener;
import net.coreprotect.listener.channel.PluginChannelListener;
import net.coreprotect.model.action.EntityActionFilter;
import net.coreprotect.model.lookup.EntityLookupContext;
import net.coreprotect.model.lookup.LookupRollbackState;

/**
 * Lookups stay fast once everything is packed.
 *
 * <p>
 * Activity is packed minutes after it is logged, so nearly every lookup reads packed rows. This
 * builds a history shaped like a real server's, packs all of it, and runs the lookups players run
 * most through the code they run through: the inspector as a click reaches it, a player's
 * history a page at a time, and the count that pages it. Every answer is checked against the same
 * rows unpacked, and each lookup has a time budget far above what it needs on an ordinary machine,
 * so the budgets catch a lookup that has started reading everything rather than a slow test runner.
 * How much each lookup reads is checked exactly, since that is what the time follows.
 * </p>
 *
 * <p>
 * The server is mocked with Mockito, as the other lookup tests do. MockBukkit is on the test class
 * path, but the version the build pins cannot start against the server API it is built with.
 * </p>
 */
class PackedLookupSpeedTest {

    private static final long DAY = 86400L;
    private static final int ROWS = 600_000;

    @TempDir
    static Path directory;

    private static World world;
    private static CommandSender console;
    private static Connection connection;
    private static RealisticActivity activity;
    private static DatabaseType previousType;
    private static long startTime;

    @BeforeAll
    static void buildHistory() throws Exception {
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = Mockito.mock(org.bukkit.Server.class);
            ConsoleCommandSender serverConsole = Mockito.mock(ConsoleCommandSender.class);
            Mockito.when(server.getConsoleSender()).thenReturn(serverConsole);
            Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
        world = Mockito.mock(World.class);
        Mockito.when(world.getName()).thenReturn("world");
        console = Mockito.mock(CommandSender.class);
        Mockito.when(console.getName()).thenReturn("console");
        Mockito.when(console.hasPermission(Mockito.anyString())).thenReturn(true);
        new PluginChannelListener();
        new PluginChannelHandshakeListener();

        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.worlds.put("world", 1);
        ConfigHandler.worldsReversed.put(1, "world");
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        Path database = directory.resolve("database.db");
        // The real schema, with the real indexes, so the live side is measured as it really is.
        Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + database), DatabaseType.SQLITE, false);
        connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            SQLiteSchema.createTables("co_", statement);
        }

        activity = new RealisticActivity(23, 60);
        for (int player = 1; player <= activity.players(); player++) {
            ConfigHandler.playerIdCache.put("player" + player, player);
            ConfigHandler.playerIdCacheReversed.put(player, "player" + player);
        }
        long first = ((System.currentTimeMillis() / 1000L - 20 * DAY) / DAY) * DAY;
        startTime = first - 1;
        activity.writeBlocks(connection, first, ROWS, 2);

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_block_all AS SELECT rowid AS id, * FROM co_block");
            statement.executeUpdate("CREATE INDEX co_block_all_place ON co_block_all(wid,x,z,y)");
            statement.executeUpdate("CREATE INDEX co_block_all_user ON co_block_all(user,time)");
        }

        long packed = ColdRollupTask.rollUp(connection, () -> {
        }, System.currentTimeMillis() / 1000L);
        assertEquals(ROWS - 1, packed, "everything but the newest row is packed");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void inspectorClicksAreFastAndShowTheWholeHistory() throws Exception {
        List<Long> timings = new ArrayList<>();
        long groupsBefore = ColdSegmentReadCounter.groupReads();
        int clicks = 0;
        try (Statement statement = connection.createStatement()) {
            for (RealisticActivity.Spot spot : spread(activity.spots(), 80)) {
                long expected = count("SELECT COUNT(*) FROM co_block_all WHERE wid = 1 AND x = " + spot.x + " AND z = " + spot.z + " AND y = " + spot.y + " AND action IN(0,1)");
                BlockState block = blockAt(spot);

                long began = System.nanoTime();
                String result = BlockLookup.performLookup("co", statement, block, console, 0, 1, 7);
                timings.add(System.nanoTime() - began);
                clicks++;

                int shown = lines(result);
                assertEquals(Math.min(7, expected), shown, "click " + clicks + " at " + spot.x + "," + spot.y + "," + spot.z + " shows its history:\n" + result);
                if (expected > 7) {
                    assertTrue(result.contains("/" + (int) Math.ceil(expected / 7.0)), "the page count covers every packed row:\n" + result);
                }
            }
        }

        long median = median(timings);
        System.out.println("packed inspector click: median " + median / 1_000_000.0 + " ms over " + clicks + " clicks");
        assertTrue(median < 25_000_000L, "an inspector click takes " + median / 1_000_000.0 + " ms");
        double groupsPerClick = (ColdSegmentReadCounter.groupReads() - groupsBefore) / (double) clicks;
        long allGroups = totalGroups();
        System.out.println("packed inspector click: decodes " + groupsPerClick + " of " + allGroups + " row groups");
        assertTrue(groupsPerClick < allGroups / 10.0, "a click decodes " + groupsPerClick + " of " + allGroups + " groups of rows");
    }

    @Test
    void aSecondLookupOfTheSamePlaceReadsNothingFromDisk() throws Exception {
        RealisticActivity.Spot spot = activity.spots().get(activity.spots().size() / 2);
        BlockState block = blockAt(spot);
        try (Statement statement = connection.createStatement()) {
            String first = BlockLookup.performLookup("co", statement, block, console, 0, 1, 7);
            long reads = ColdSegmentReadCounter.frameReads();
            String second = BlockLookup.performLookup("co", statement, block, console, 0, 1, 7);
            assertEquals(reads, ColdSegmentReadCounter.frameReads(), "the segments are already in memory");
            assertEquals(first, second);
        }
    }

    @Test
    void radiusLookupsMatchTheUnpackedRowsExactly() throws Exception {
        List<Long> timings = new ArrayList<>();
        for (RealisticActivity.Spot spot : spread(activity.spots(), 40)) {
            String where = "wid = 1 AND x >= " + (spot.x - 10) + " AND x <= " + (spot.x + 10) + " AND z >= " + (spot.z - 10) + " AND z <= " + (spot.z + 10);
            List<Long> expected = ids("SELECT id FROM co_block_all WHERE " + where + " ORDER BY time DESC, id DESC LIMIT 50");

            long began = System.nanoTime();
            SQLiteColdIndex.beginLookup(0, 0);
            List<Long> actual;
            try {
                String source = SQLiteColdIndex.sourceExpression(connection, "block", 1, new Integer[] { 10, spot.x - 10, spot.x + 10, null, null, spot.z - 10, spot.z + 10 });
                actual = ids("SELECT rowid FROM " + source + " WHERE " + where + " ORDER BY time DESC, rowid DESC LIMIT 50");
            }
            finally {
                SQLiteColdIndex.endLookup(connection);
            }
            timings.add(System.nanoTime() - began);
            assertEquals(expected, actual, "radius lookup at " + spot.x + "," + spot.z);
        }
        long median = median(timings);
        System.out.println("packed radius lookup: median " + median / 1_000_000.0 + " ms");
        assertTrue(median < 40_000_000L, "a radius lookup takes " + median / 1_000_000.0 + " ms");
    }

    @Test
    void aPlayersHistoryPagesQuicklyAndCountsWithoutUnpackingBlockData() throws Exception {
        CommandSender sender = console;
        List<Long> timings = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            for (int player = 1; player <= 12; player++) {
                String name = "player" + player;
                long expected = count("SELECT COUNT(*) FROM co_block_all WHERE user = " + player + " AND action IN(0,1) AND time > " + startTime);
                if (expected == 0) {
                    continue;
                }
                List<Long> expectedIds = ids("SELECT id FROM co_block_all WHERE user = " + player + " AND action IN(0,1) AND time > " + startTime + " ORDER BY time DESC, id DESC LIMIT 7");

                long payloadReads = ColdSegmentReadCounter.payloadReads();
                long began = System.nanoTime();
                Long[] rowData = new Long[] { 0L, 0L, 0L, 0L, 0L };
                long total = Lookup.countLookupRows(statement, sender, Collections.emptyList(), Collections.singletonList(name), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                        Arrays.asList(0, 1), EntityActionFilter.NONE, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, rowData,
                        startTime, 0, false, true, null, LookupRollbackState.ANY);
                assertEquals(payloadReads, ColdSegmentReadCounter.payloadReads(), "counting unpacks no block data");
                List<Object[]> page = LookupRaw.performLookupRaw(statement, sender, Collections.emptyList(), Collections.singletonList(name), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                        Arrays.asList(0, 1), EntityActionFilter.NONE, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, rowData,
                        startTime, 0, 0, 7, false, true, null, LookupRollbackState.ANY);
                timings.add(System.nanoTime() - began);

                assertEquals(expected, total, name + "'s count");
                List<Long> actualIds = new ArrayList<>();
                for (Object[] row : page) {
                    actualIds.add(((Number) row[0]).longValue());
                }
                assertEquals(expectedIds, actualIds, name + "'s first page");
            }
        }
        assertTrue(!timings.isEmpty());
        long median = median(timings);
        System.out.println("packed player count and page: median " + median / 1_000_000.0 + " ms");
        assertTrue(median < 60_000_000L, "counting and showing a page of a player's history takes " + median / 1_000_000.0 + " ms");
    }

    private static BlockState blockAt(RealisticActivity.Spot spot) {
        BlockState block = Mockito.mock(BlockState.class);
        Mockito.when(block.getX()).thenReturn(spot.x);
        Mockito.when(block.getY()).thenReturn(spot.y);
        Mockito.when(block.getZ()).thenReturn(spot.z);
        Mockito.when(block.getWorld()).thenReturn(world);
        Mockito.when(block.getType()).thenReturn(Material.STONE);
        return block;
    }

    private static List<RealisticActivity.Spot> spread(List<RealisticActivity.Spot> spots, int count) {
        List<RealisticActivity.Spot> chosen = new ArrayList<>();
        for (int index = 0; index < count && index < spots.size(); index++) {
            chosen.add(spots.get((int) ((index * 7919L) % spots.size())));
        }
        return chosen;
    }

    /** Counts the history lines of an inspector result, which each start with when it happened. */
    private static int lines(String result) {
        int lines = 0;
        for (String line : result.split("\n")) {
            if (line.startsWith("<COMPONENT>POPUP|")) {
                lines++;
            }
        }
        return lines;
    }

    private static long totalGroups() throws SQLException {
        return count("SELECT SUM((row_count + " + (ColdSegmentCodec.GROUP_ROWS - 1) + ") / " + ColdSegmentCodec.GROUP_ROWS + ") FROM co_segment WHERE table_id = 0");
    }

    private static long median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        return sorted.get(sorted.size() / 2);
    }

    private static long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private static List<Long> ids(String query) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                ids.add(results.getLong(1));
            }
        }
        return ids;
    }
}
