package net.coreprotect.database;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.listener.channel.PluginChannelHandshakeListener;
import net.coreprotect.listener.channel.PluginChannelListener;
import net.coreprotect.model.action.EntityActionFilter;
import net.coreprotect.model.action.LookupActions;
import net.coreprotect.model.lookup.EntityLookupContext;
import net.coreprotect.model.lookup.LookupRollbackState;

/**
 * Times pages at every depth of one player's long history, through the lookup path a command
 * takes: sixty compacted days of history, of which the last thirty are looked up, as
 * {@code time:30d} does, and in which the player owns about five million rows; looked up across
 * blocks, containers and items merged by time, and across blocks alone in row id order.
 *
 * <p>
 * For each page it reports how long the page took, how many rows were written to temporary
 * storage, and how many segments were decoded. Timings on a shared machine are noisy; the rows and
 * segments are not, and are what the time follows.
 * </p>
 *
 * <p>
 * The database is built once and kept, so the same history can be measured by different builds:
 * {@code main} only calls what every build of the lookup path has. Run with
 * {@code mvn -DskipTests=false -Dcoreprotect.bench=deep -Dtest=DeepPageBenchmark test}, or through
 * {@code main} with the test class path, giving {@code -Dcoreprotect.bench.database} to choose where
 * the database is kept.
 * </p>
 */
class DeepPageBenchmark {

    private static final long DAY = 86400L;
    private static final int USER = 7;
    /** Days of history written, twice as many as the lookup covers, as on a server kept for months. */
    private static final int DAYS = Integer.getInteger("coreprotect.bench.days", 60);
    /** Days the lookup covers, which cut through the history as {@code time:30d} does. */
    private static final int WINDOW = Integer.getInteger("coreprotect.bench.window", 30);
    private static final int PAGE = 4;

    @Test
    void timesPagesAtEveryDepth() throws Exception {
        assumeTrue("deep".equals(System.getProperty("coreprotect.bench")), "benchmark runs only when asked for");
        main(new String[0]);
    }

    public static void main(String[] arguments) throws Exception {
        Path database = Paths.get(System.getProperty("coreprotect.bench.database", "/tmp/cp_bench/deep.db"));
        int blocksPerDay = Integer.getInteger("coreprotect.bench.blocks", 300_000);
        int sidePerDay = Integer.getInteger("coreprotect.bench.side", 12_000);

        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = Mockito.mock(org.bukkit.Server.class);
            ConsoleCommandSender serverConsole = Mockito.mock(ConsoleCommandSender.class);
            Mockito.when(server.getConsoleSender()).thenReturn(serverConsole);
            Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectBenchmark"));
            org.bukkit.Bukkit.setServer(server);
        }
        CommandSender console = Mockito.mock(CommandSender.class);
        Mockito.when(console.getName()).thenReturn("console");
        new PluginChannelListener();
        new PluginChannelHandshakeListener();

        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.playerIdCache.put("benchplayer", USER);
        ConfigHandler.playerIdCacheReversed.put(USER, "benchplayer");
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        long now = System.currentTimeMillis() / 1000L;
        if (!Files.exists(database)) {
            Files.createDirectories(database.getParent());
            build(database, blocksPerDay, sidePerDay);
        }

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA cache_size=-262144");
                statement.executeUpdate("PRAGMA mmap_size=4294967296");
                statement.executeUpdate("PRAGMA temp_store=FILE");
            }
            SQLiteColdIndex.reload(connection);
            long startTime = now - (WINDOW * DAY);
            System.out.println("segments=" + SQLiteColdIndex.selectSegments(connection, "block", 0, null, 0, 0, null).size());

            measure(connection, console, "merged", new ArrayList<>(), startTime);
            measure(connection, console, "one table", new ArrayList<>(Arrays.asList(LookupActions.BLOCK_BREAK, LookupActions.BLOCK_PLACE)), startTime);
        }
    }

    private static void measure(Connection connection, CommandSender console, String label, List<Integer> actions, long startTime) throws Exception {
        long total;
        long countStart = System.nanoTime();
        try (Statement statement = connection.createStatement()) {
            total = Lookup.countLookupRows(statement, console, Collections.emptyList(), Collections.singletonList("benchplayer"), new ArrayList<>(), new HashMap<>(), Collections.emptyList(),
                    new ArrayList<>(actions), EntityActionFilter.NONE, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null,
                    new Long[] { 0L, 0L, 0L, 0L, 0L }, startTime, 0, false, true, null, LookupRollbackState.ANY);
        }
        System.out.printf("%s: %d rows, counted in %d ms%n", label, total, (System.nanoTime() - countStart) / 1_000_000L);

        long last = ((total - 1) / PAGE) * PAGE;
        long[] offsets = { 0, total / 10, total / 2, (total * 99) / 100, last };
        String[] names = { "0", "10%", "50%", "99%", "last" };
        for (int index = 0; index < offsets.length; index++) {
            for (int run = 1; run <= 2; run++) {
                long written = ColdSegmentReadCounter.scratchRows();
                long reads = ColdSegmentReadCounter.reads();
                long started = System.nanoTime();
                int shown;
                try (Statement statement = connection.createStatement()) {
                    shown = LookupRaw.performLookupRaw(statement, console, Collections.emptyList(), Collections.singletonList("benchplayer"), new ArrayList<>(), new HashMap<>(),
                            Collections.emptyList(), new ArrayList<>(actions), EntityActionFilter.NONE, Collections.emptyList(),
                            EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, new Long[] { 0L, 0L, 0L, 0L, 0L }, startTime, 0,
                            (int) offsets[index], PAGE, false, true, null, LookupRollbackState.ANY).size();
                }
                long millis = (System.nanoTime() - started) / 1_000_000L;
                System.out.printf("  %-9s offset %-9d %s run %d: %6d ms, %9d rows written to temporary storage, %5d segment reads, %d rows shown%n", label, offsets[index],
                        String.format("(%s)", names[index]), run, millis, ColdSegmentReadCounter.scratchRows() - written, ColdSegmentReadCounter.reads() - reads, shown);
            }
        }
    }

    /**
     * Writes the days of activity in which the measured player owns half the placed and broken
     * blocks and half the container and item rows, then packs and compacts every day.
     */
    private static void build(Path database, int blocksPerDay, int sidePerDay) throws Exception {
        long started = System.nanoTime();
        Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + database), DatabaseType.SQLITE, false);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database)) {
            try (Statement statement = connection.createStatement()) {
                SQLiteSchema.applyFileSettings(statement);
                statement.executeUpdate("PRAGMA journal_mode=WAL");
                statement.executeUpdate("PRAGMA synchronous=OFF");
                SQLiteSchema.createTables("co_", statement);
            }
            long firstDay = ((System.currentTimeMillis() / 1000L / DAY) - (DAYS + 2)) * DAY;
            connection.setAutoCommit(false);
            try (PreparedStatement block = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,NULL,?,?,0)");
                    PreparedStatement container = connection.prepareStatement("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?,1,?,64,?,?,0,?,NULL,?,0)");
                    PreparedStatement item = connection.prepareStatement("INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,1,?,64,?,?,?,?,?,0)")) {
                for (int day = 0; day < DAYS; day++) {
                    long dayStart = firstDay + (day * DAY);
                    for (int index = 0; index < blocksPerDay; index++) {
                        block.setLong(1, dayStart + 60 + ((index * 86_000L) / blocksPerDay));
                        block.setInt(2, index % 2 == 0 ? USER : 1 + (index % 9));
                        block.setInt(3, ((index * 37) % 4000) - 2000);
                        block.setInt(4, ((index * 91) % 4000) - 2000);
                        block.setInt(5, 1 + (index % 80));
                        block.setBytes(6, index % 3 == 0 ? new byte[] { (byte) index, 1, 2 } : null);
                        block.setInt(7, index % 2 == 0 ? (index / 2) % 2 : index % 2);
                        block.addBatch();
                        if (index % 10_000 == 9_999) {
                            block.executeBatch();
                        }
                    }
                    block.executeBatch();
                    for (int index = 0; index < sidePerDay; index++) {
                        long time = dayStart + 60 + ((index * 86_000L) / sidePerDay);
                        int user = index % 2 == 0 ? USER : 1 + (index % 9);
                        container.setLong(1, time);
                        container.setInt(2, user);
                        container.setInt(3, index % 500);
                        container.setInt(4, index % 700);
                        container.setInt(5, 1 + (index % 40));
                        container.setInt(6, 1 + (index % 64));
                        container.setInt(7, index % 2);
                        container.addBatch();
                        item.setLong(1, time);
                        item.setInt(2, user);
                        item.setInt(3, index % 500);
                        item.setInt(4, index % 700);
                        item.setInt(5, 1 + (index % 40));
                        item.setBytes(6, new byte[] { (byte) index, 4 });
                        item.setInt(7, 1 + (index % 64));
                        item.setInt(8, 2 + (index % 2));
                        item.addBatch();
                    }
                    container.executeBatch();
                    item.executeBatch();
                    connection.commit();
                }
            }
            connection.setAutoCommit(true);
            System.out.println("wrote " + ((long) DAYS * (blocksPerDay + (2L * sidePerDay))) + " rows in " + (System.nanoTime() - started) / 1_000_000_000L + " s");

            long packing = System.nanoTime();
            ColdRollupTask.rollUp(connection, () -> {
            });
            System.out.println("packed in " + (System.nanoTime() - packing) / 1_000_000_000L + " s");
            long compacting = System.nanoTime();
            SegmentCompactor.compactAll(connection, () -> {
            });
            System.out.println("compacted in " + (System.nanoTime() - compacting) / 1_000_000_000L + " s");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA wal_checkpoint(TRUNCATE)");
            }
        }
    }
}
