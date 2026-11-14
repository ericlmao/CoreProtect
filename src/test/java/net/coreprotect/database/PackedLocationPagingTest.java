package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.api.BlockAPI;
import net.coreprotect.api.LookupOptions;
import net.coreprotect.api.SignAPI;
import net.coreprotect.api.result.BlockResult;
import net.coreprotect.api.result.ContainerResult;
import net.coreprotect.api.result.SignResult;
import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.process.SignUpdateAccess;
import net.coreprotect.database.lookup.BlockLookup;
import net.coreprotect.database.lookup.ChestTransactionLookup;
import net.coreprotect.database.lookup.EntityInteractionLookup;
import net.coreprotect.database.lookup.InspectorSource;
import net.coreprotect.database.lookup.InteractionLookup;
import net.coreprotect.database.lookup.SignMessageLookup;
import net.coreprotect.listener.channel.PluginChannelHandshakeListener;
import net.coreprotect.listener.channel.PluginChannelListener;

/**
 * Lookups of one busy place, or one busy entity, holding far more packed rows than one lookup may
 * copy out of compressed storage at once.
 *
 * <p>
 * A hopper or a farm can log close to a million rows at one block. Every lookup that reads such a
 * place, the inspector's clicks, the API, and a sign update, has to answer exactly what plain SQL
 * over the same rows never packed answers, without ever holding more rows in temporary storage than
 * a lookup may. Each lookup here runs against the packed history and against an unpacked copy of
 * it, and the two answers have to be identical; a page may copy out no more rows than it shows, and
 * a read of every row may never hold more than the limit at once.
 * </p>
 */
class PackedLocationPagingTest {

    private static final long DAY = 86400L;
    private static final int WORLD = 1;
    private static final int X = 120;
    private static final int Y = 64;
    private static final int Z = -340;
    private static final int SPAWN = 7;
    private static final int DAYS = 4;
    private static final int ROWS_PER_DAY = 78_000;
    private static final int BURST = 3000;
    /** Fewer live rows than a page holds, so even the first page shows packed rows. */
    private static final int LIVE = 3;
    private static final int LIMIT = 7;
    private static final int MINECART = 50;
    private static final int ZOMBIE = 51;
    private static final String BLOCK_ACTIONS = "(action IN(0,1,13) OR (action=3 AND type IN(" + MINECART + ")))";
    private static final String SIGN_TEXT = "(LENGTH(line_1) > 0 OR LENGTH(line_2) > 0 OR LENGTH(line_3) > 0 OR LENGTH(line_4) > 0 OR LENGTH(line_5) > 0 OR LENGTH(line_6) > 0 OR LENGTH(line_7) > 0 OR LENGTH(line_8) > 0)";

    @TempDir
    static Path directory;

    private static Connection connection;
    private static Connection plain;
    private static CommandSender console;
    private static World world;
    private static DatabaseType previousType;
    private static boolean previousApi;
    private static long burstTime;

    /** Runs a lookup on a statement and returns what it shows. */
    @FunctionalInterface
    private interface Lookup {
        String run(Statement statement, int page) throws Exception;
    }

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
        previousApi = Config.getGlobal().API_ENABLED;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.storagePath = directory;
        ConfigHandler.sqlite = "database.db";
        ConfigHandler.sqliteDataSource = null;
        ConfigHandler.worlds.put("world", WORLD);
        ConfigHandler.worldsReversed.put(WORLD, "world");
        for (int user = 1; user <= 5; user++) {
            ConfigHandler.playerIdCache.put("player" + user, user);
            ConfigHandler.playerIdCacheReversed.put(user, "player" + user);
        }
        ConfigHandler.entities.put("minecart", MINECART);
        ConfigHandler.entitiesReversed.put(MINECART, "minecart");
        ConfigHandler.entities.put("zombie", ZOMBIE);
        ConfigHandler.entitiesReversed.put(ZOMBIE, "zombie");
        Config.getGlobal().API_ENABLED = true;
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ColdRollupTask.setSegmentRowsForTesting(20_000);
        SegmentCompactor.setTargetRowsForTesting(60_000);

        connection = open(directory.resolve("database.db"));
        plain = open(directory.resolve("plain.db"));

        long now = System.currentTimeMillis() / 1000L;
        long firstDay = ((now / DAY) - (DAYS + 2)) * DAY;
        burstTime = firstDay + DAY + 60 + ((ROWS_PER_DAY / 2) * 86_000L) / ROWS_PER_DAY;
        writeHistory(firstDay);
        copyToPlain();

        assertTrue(ColdRollupTask.rollUp(connection, () -> {
        }) > 0, "the history is packed");
        assertTrue(SegmentCompactor.compactAll(connection, () -> {
        }) > 0, "the packed days are compacted");
        SQLiteColdIndex.reload(connection);

        // Recent activity that has not been packed yet, which every lookup ranks alongside.
        writeLive(now - 3600);
        copyToPlain();
    }

    @AfterAll
    static void closeDatabases() throws SQLException {
        ColdRollupTask.setSegmentRowsForTesting(0);
        SegmentCompactor.setTargetRowsForTesting(0);
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        Config.getGlobal().API_ENABLED = previousApi;
        if (connection != null) {
            connection.close();
        }
        if (plain != null) {
            plain.close();
        }
    }

    @Test
    void theHistoryIsFarLargerThanOneLookupMayHold() throws SQLException {
        for (String table : new String[] { "block", "container", "sign" }) {
            long packed = count(connection, "SELECT SUM(row_count) FROM co_segment WHERE table_id = " + SQLiteColdIndex.tableId(table));
            assertTrue(packed > SQLiteColdIndex.MATERIALIZE_LIMIT + 50_000, table + " holds " + packed + " packed rows");
            assertTrue(count(plain, "SELECT COUNT(*) FROM co_" + table + " WHERE wid = " + WORLD + " AND x = " + X + " AND z = " + Z + " AND y = " + Y) >= 300_000, "at one place in " + table);
        }
        for (String table : new String[] { "entity_container", "entity_interaction" }) {
            assertTrue(count(plain, "SELECT COUNT(*) FROM co_" + table + " WHERE entity_spawn_rowid = " + SPAWN) >= 300_000, "about one entity in " + table);
        }
    }

    @Test
    void anInspectorBlockClickMatchesPlainSql() throws Exception {
        long total = count(plain, "SELECT COUNT(*) FROM co_block WHERE " + place() + " AND " + BLOCK_ACTIONS);
        assertPagesMatch("block click", total, (statement, page) -> BlockLookup.performLookup("co", statement, blockState(), console, 0, page, LIMIT));
    }

    @Test
    void anInspectorInteractionClickMatchesPlainSql() throws Exception {
        long total = count(plain, "SELECT COUNT(*) FROM co_block WHERE " + place() + " AND action=2");
        assertPagesMatch("interaction click", total, (statement, page) -> InteractionLookup.performLookup("co", statement, block(), console, 0, page, LIMIT));
    }

    @Test
    void anInspectorContainerClickMatchesPlainSql() throws Exception {
        long total = count(plain, "SELECT COUNT(*) FROM co_container WHERE " + place());
        // Not exact, so the clause offers each coordinate as a choice of two.
        assertPagesMatch("container click", total, (statement, page) -> String.join("\n", ChestTransactionLookup.performLookup("co", statement, location(), console, page, LIMIT, false)));
    }

    @Test
    void anEntityContainerLookupMatchesPlainSql() throws Exception {
        long total = count(plain, "SELECT COUNT(*) FROM co_entity_container WHERE entity_spawn_rowid = " + SPAWN);
        assertPagesMatch("entity container", total, (statement, page) -> String.join("\n", ChestTransactionLookup.performLookup("co", statement, location(), console, page, LIMIT, false, SPAWN)));
    }

    @Test
    void anEntityInteractionLookupMatchesPlainSql() throws Exception {
        long total = count(plain, "SELECT COUNT(*) FROM co_entity_interaction WHERE entity_spawn_rowid = " + SPAWN);
        assertPagesMatch("entity interaction", total, (statement, page) -> String.join("\n", EntityInteractionLookup.performLookup("co", statement, console, page, LIMIT, SPAWN)));
    }

    @Test
    void aSignLookupMatchesPlainSql() throws Exception {
        long total = count(plain, "SELECT COUNT(*) FROM co_sign WHERE " + place() + " AND action = 1 AND " + SIGN_TEXT);
        assertTrue(total < count(plain, "SELECT COUNT(*) FROM co_sign WHERE " + place() + " AND action = 1"), "some signs were placed without text");
        assertPagesMatch("sign lookup", total, (statement, page) -> String.join("\n", SignMessageLookup.performLookup("co", statement, location(), console, page, LIMIT)));
    }

    @Test
    void anInspectorClickOnTheBusiestPlaceIsFast() throws Exception {
        long packed = count(connection, "SELECT SUM(row_count) FROM co_segment WHERE table_id = " + SQLiteColdIndex.tableId("block"));
        for (int page : new int[] { 1, 20_000 }) {
            List<Long> warm = new ArrayList<>();
            List<Long> cold = new ArrayList<>();
            long written = 0;
            try (Statement statement = connection.createStatement()) {
                for (int run = 0; run < 15; run++) {
                    boolean fromDisk = run % 3 == 0;
                    if (fromDisk) {
                        SegmentFrameCache.clear();
                    }
                    long before = ColdSegmentReadCounter.scratchRows();
                    long started = System.nanoTime();
                    String result = BlockLookup.performLookup("co", statement, blockState(), console, 0, page, LIMIT);
                    long elapsed = System.nanoTime() - started;
                    written = ColdSegmentReadCounter.scratchRows() - before;
                    assertEquals(LIMIT, rows(result), "the click shows a full page:\n" + result);
                    assertTrue(written <= LIMIT, "the click copied out " + written + " rows");
                    (fromDisk ? cold : warm).add(elapsed);
                }
            }
            System.out.println("busiest place inspector click, page " + page + " (" + packed + " packed block rows): warm median " + median(warm) / 1_000_000.0 + " ms, cold median "
                    + median(cold) / 1_000_000.0 + " ms, " + written + " rows copied out");
            assertTrue(median(warm) < 250_000_000L, "a warm click takes " + median(warm) / 1_000_000.0 + " ms");
        }
    }

    @Test
    void theApiReadsEveryRowOfTheBlockWithoutExceedingTheLimit() throws Exception {
        List<String> expected = strings(plain, "SELECT time || ':' || action || ':' || type || ':' || data || ':' || rolled_back FROM co_block WHERE " + place() + " AND time > 0 ORDER BY rowid DESC");
        assertTrue(expected.size() > SQLiteColdIndex.MATERIALIZE_LIMIT, "the block holds " + expected.size() + " rows");

        ColdSegmentReadCounter.resetPeakHeldRows();
        List<String[]> rows = BlockAPI.performLookup(block(), 0);
        List<String> actual = new ArrayList<>(rows.size());
        for (String[] row : rows) {
            actual.add(row[0] + ":" + row[7] + ":" + row[5] + ":" + row[6] + ":" + row[8]);
        }
        assertEquals(expected.size(), actual.size(), "every row of the block");
        assertEquals(expected, actual, "every row of the block, newest first");
        assertTrue(ColdSegmentReadCounter.peakHeldRows() <= SQLiteColdIndex.MATERIALIZE_LIMIT, "the read held " + ColdSegmentReadCounter.peakHeldRows() + " rows at once");
        assertTrue(ColdSegmentReadCounter.peakHeldRows() > 0, "the read went through packed rows");
    }

    @Test
    void theApiWithALimitReadsOnlyThePage() throws Exception {
        List<String> blocks = strings(plain, "SELECT time || ':' || action || ':' || data || ':' || user FROM co_block WHERE " + place() + " AND time > 0 ORDER BY rowid DESC");
        List<String> signs = strings(plain, "SELECT time || ':' || user || ':' || IFNULL(line_1,'') || IFNULL(line_6,'') FROM co_sign WHERE " + place() + " AND time > 0 AND action = '1' AND " + SIGN_TEXT + " ORDER BY rowid DESC");
        for (int offset : new int[] { 0, 3, blocks.size() / 2, blocks.size() - 3 }) {
            long before = ColdSegmentReadCounter.scratchRows();
            List<String> actual = new ArrayList<>();
            for (BlockResult result : BlockAPI.performLookup(block(), LookupOptions.builder().limit(offset, LIMIT).build())) {
                actual.add(result.getTimestamp() / 1000L + ":" + result.getActionId() + ":" + result.getData() + ":" + ConfigHandler.playerIdCache.get(result.getPlayer()));
            }
            assertEquals(slice(blocks, offset), actual, "block page at " + offset);
            assertTrue(ColdSegmentReadCounter.scratchRows() - before <= LIMIT, "the block page at " + offset + " copied out " + (ColdSegmentReadCounter.scratchRows() - before) + " rows");
        }
        for (int offset : new int[] { 0, signs.size() / 2, signs.size() - 2 }) {
            long before = ColdSegmentReadCounter.scratchRows();
            List<String> actual = new ArrayList<>();
            for (SignResult result : SignAPI.performLookup(LookupOptions.builder().location(location()).limit(offset, LIMIT).build())) {
                actual.add(result.getTimestamp() / 1000L + ":" + ConfigHandler.playerIdCache.get(result.getPlayer()) + ":" + result.getLines()[0] + result.getLines()[5]);
            }
            assertEquals(slice(signs, offset), actual, "sign page at " + offset);
            assertTrue(ColdSegmentReadCounter.scratchRows() - before <= LIMIT, "the sign page at " + offset + " copied out " + (ColdSegmentReadCounter.scratchRows() - before) + " rows");
        }
    }

    @Test
    void theContainerApiWalksWithinTheLimitAndStaysExact() throws Exception {
        // The entity rows it merges in are joined to where the entity is now, which only the query
        // can judge, so this lookup walks the history rather than planning its page.
        List<String> expected = strings(plain, "SELECT time || ':' || action || ':' || data || ':' || amount FROM co_container WHERE " + place() + " AND time > 0 ORDER BY time DESC, rowid DESC");
        for (int offset : new int[] { 0, expected.size() / 2 }) {
            ColdSegmentReadCounter.resetPeakHeldRows();
            List<String> actual = new ArrayList<>();
            for (ContainerResult result : BlockAPI.performContainerLookup(LookupOptions.builder().location(location()).limit(offset, LIMIT).build())) {
                actual.add(result.getTimestamp() / 1000L + ":" + result.getActionId() + ":" + result.getData() + ":" + result.getAmount());
            }
            assertEquals(slice(expected, offset), actual, "container page at " + offset);
            assertTrue(ColdSegmentReadCounter.peakHeldRows() <= SQLiteColdIndex.MATERIALIZE_LIMIT, "a window held " + ColdSegmentReadCounter.peakHeldRows() + " rows");
        }
    }

    @Test
    void aSignUpdateRestoresTheRightText() throws Exception {
        String columns = "color, color_secondary, data, waxed, face, line_1, line_2, line_3, line_4, line_5, line_6, line_7, line_8";
        long middle = burstTime + (DAY / 3);
        for (long time : new long[] { burstTime, burstTime + 1, middle, System.currentTimeMillis() / 1000L - 3500 }) {
            for (int user = 1; user <= 5; user++) {
                for (int action = 0; action <= 1; action++) {
                    String where = "user=" + user + " AND wid=" + WORLD + " AND x=" + X + " AND z=" + Z + " AND y=" + Y;
                    String expected = describe(plain, "SELECT " + columns + " FROM co_sign WHERE " + where
                            + (action == 0 ? " AND time < " + time + " ORDER BY rowid DESC" : " AND time >= " + time + " ORDER BY rowid ASC") + " LIMIT 1");
                    long before = ColdSegmentReadCounter.scratchRows();
                    String actual;
                    try (InspectorSource source = InspectorSource.open(connection, "sign", WORLD, X, X, Z, Z, 0);
                            ResultSet results = SignUpdateAccess.loggedText(source, user, WORLD, X, Y, Z, action, (int) time)) {
                        actual = describe(results);
                    }
                    assertEquals(expected, actual, "the text restored for player " + user + (action == 0 ? " before " : " from ") + time);
                    assertTrue(ColdSegmentReadCounter.scratchRows() - before <= 1, "the update copied out " + (ColdSegmentReadCounter.scratchRows() - before) + " rows");
                }
            }
        }
    }

    /**
     * Runs a lookup on the first, second, middle, last and a page past the last, against the
     * unpacked copy and against the packed history, and requires the same answer from both, with
     * no more rows copied out of compressed storage than the page shows.
     */
    private static void assertPagesMatch(String name, long total, Lookup lookup) throws Exception {
        assertTrue(total > 10_000, name + " covers " + total + " rows");
        int pages = (int) Math.ceil(total / (double) LIMIT);
        for (int page : new int[] { 1, 2, pages / 2, pages, pages + 1 }) {
            int shown = (int) Math.max(0, Math.min(LIMIT, total - ((long) (page - 1) * LIMIT)));
            String expected = onPlain(() -> {
                try (Statement statement = plain.createStatement()) {
                    return lookup.run(statement, page);
                }
            });
            assertEquals(shown, rows(expected), name + " page " + page + " of " + pages + " shows its rows from plain SQL:\n" + expected);

            long before = ColdSegmentReadCounter.scratchRows();
            ColdSegmentReadCounter.resetPeakHeldRows();
            String actual;
            try (Statement statement = connection.createStatement()) {
                actual = lookup.run(statement, page);
            }
            long written = ColdSegmentReadCounter.scratchRows() - before;
            assertEquals(normalize(expected), normalize(actual), name + " page " + page + " of " + pages);
            assertTrue(written <= shown, name + " page " + page + " copied out " + written + " rows for " + shown + " shown");
            assertTrue(ColdSegmentReadCounter.peakHeldRows() <= LIMIT, name + " page " + page + " held " + ColdSegmentReadCounter.peakHeldRows() + " rows");
            System.out.println(name + ": page " + page + " of " + pages + " (" + total + " rows) copied out " + written + " rows");
        }
    }

    /** Runs something against the unpacked copy, with nothing packed to read. */
    private static <T> T onPlain(Callable<T> lookup) throws Exception {
        SQLiteColdIndex.invalidate();
        SQLiteColdIndex.reload(plain);
        try {
            return lookup.call();
        }
        finally {
            SQLiteColdIndex.invalidate();
            SQLiteColdIndex.reload(connection);
        }
    }

    private static final Pattern SHOWN_ROW = Pattern.compile("POPUP\\|[^|]*\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    /** Counts the rows a lookup shows, each of which starts with when it happened. */
    private static int rows(String result) {
        int rows = 0;
        Matcher matcher = SHOWN_ROW.matcher(result);
        while (matcher.find()) {
            rows++;
        }
        return rows;
    }

    /** Leaves out how long ago each row was, which moves on between the two lookups. */
    private static String normalize(String result) {
        return result.replaceAll("[0-9]+\\.[0-9]{2}", "#");
    }

    private static String place() {
        return "wid = " + WORLD + " AND x = " + X + " AND z = " + Z + " AND y = " + Y;
    }

    private static Location location() {
        return new Location(world, X, Y, Z);
    }

    private static Block block() {
        Block block = Mockito.mock(Block.class);
        Mockito.when(block.getX()).thenReturn(X);
        Mockito.when(block.getY()).thenReturn(Y);
        Mockito.when(block.getZ()).thenReturn(Z);
        Mockito.when(block.getWorld()).thenReturn(world);
        Mockito.when(block.getType()).thenReturn(Material.HOPPER);
        Mockito.when(block.getLocation()).thenReturn(location());
        return block;
    }

    private static BlockState blockState() {
        BlockState block = Mockito.mock(BlockState.class);
        Mockito.when(block.getX()).thenReturn(X);
        Mockito.when(block.getY()).thenReturn(Y);
        Mockito.when(block.getZ()).thenReturn(Z);
        Mockito.when(block.getWorld()).thenReturn(world);
        Mockito.when(block.getType()).thenReturn(Material.HOPPER);
        return block;
    }

    private static Connection open(Path database) throws SQLException {
        Database.createDatabaseTables("co_", true, DriverManager.getConnection("jdbc:sqlite:" + database), DatabaseType.SQLITE, false);
        Connection opened = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = opened.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            SQLiteSchema.createTables("co_", statement);
        }
        return opened;
    }

    /**
     * Writes several days of activity at one place and about one entity: blocks placed, broken,
     * clicked, and entities spawned and killed there, a hopper's transactions, a sign rewritten
     * over and over, with and without text, and an entity's container and interactions. Rows at
     * neighbouring blocks of the same chunk share the segments without matching, and one second
     * holds thousands of rows in every table.
     */
    private static void writeHistory(long firstDay) throws SQLException {
        connection.setAutoCommit(false);
        try (PreparedStatement block = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?," + WORLD + ",?,?," + Z + ",?,?,NULL,NULL,?,?)");
                PreparedStatement container = connection.prepareStatement("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?," + WORLD + "," + X + "," + Y + "," + Z + ",?,?,?,NULL,?,0)");
                PreparedStatement sign = connection.prepareStatement("INSERT INTO co_sign (time,user,wid,x,y,z,action,color,color_secondary,data,waxed,face,line_1,line_2,line_3,line_4,line_5,line_6,line_7,line_8) VALUES (?,?," + WORLD + "," + X + "," + Y + "," + Z
                        + ",?,?,0,0,0,?,?,?,?,?,?,?,?,?)");
                PreparedStatement entityContainer = connection.prepareStatement("INSERT INTO co_entity_container (time,user,entity_spawn_rowid,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?," + SPAWN + "," + WORLD + ",?," + Y + ",?,?,?,?,NULL,?,0)");
                PreparedStatement entityInteraction = connection.prepareStatement("INSERT INTO co_entity_interaction (time,user,entity_spawn_rowid,wid,x,y,z,type,action,metadata,rolled_back) VALUES (?,?," + SPAWN + "," + WORLD + ",?," + Y + ",?," + ZOMBIE + ",?,NULL,0)")) {
            int index = 0;
            for (int day = 0; day < DAYS; day++) {
                long dayStart = firstDay + (day * DAY);
                for (int row = 0; row < ROWS_PER_DAY; row++) {
                    long time = dayStart + 60 + (row * 86_000L) / ROWS_PER_DAY;
                    int repeats = day == 1 && row == ROWS_PER_DAY / 2 ? BURST : 1;
                    for (int repeat = 0; repeat < repeats; repeat++) {
                        writeRow(block, container, sign, entityContainer, entityInteraction, time, index++);
                    }
                    if (row % 5000 == 4999) {
                        for (PreparedStatement statement : Arrays.asList(block, container, sign, entityContainer, entityInteraction)) {
                            statement.executeBatch();
                        }
                    }
                }
            }
            for (PreparedStatement statement : Arrays.asList(block, container, sign, entityContainer, entityInteraction)) {
                statement.executeBatch();
            }
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private static void writeLive(long time) throws SQLException {
        connection.setAutoCommit(false);
        try (PreparedStatement block = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?," + WORLD + ",?,?," + Z + ",?,?,NULL,NULL,?,?)");
                PreparedStatement container = connection.prepareStatement("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?," + WORLD + "," + X + "," + Y + "," + Z + ",?,?,?,NULL,?,0)");
                PreparedStatement sign = connection.prepareStatement("INSERT INTO co_sign (time,user,wid,x,y,z,action,color,color_secondary,data,waxed,face,line_1,line_2,line_3,line_4,line_5,line_6,line_7,line_8) VALUES (?,?," + WORLD + "," + X + "," + Y + "," + Z
                        + ",?,?,0,0,0,?,?,?,?,?,?,?,?,?)");
                PreparedStatement entityContainer = connection.prepareStatement("INSERT INTO co_entity_container (time,user,entity_spawn_rowid,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?," + SPAWN + "," + WORLD + ",?," + Y + ",?,?,?,?,NULL,?,0)");
                PreparedStatement entityInteraction = connection.prepareStatement("INSERT INTO co_entity_interaction (time,user,entity_spawn_rowid,wid,x,y,z,type,action,metadata,rolled_back) VALUES (?,?," + SPAWN + "," + WORLD + ",?," + Y + ",?," + ZOMBIE + ",?,NULL,0)")) {
            for (int row = 0; row < LIVE; row++) {
                writeRow(block, container, sign, entityContainer, entityInteraction, time + (row / 3), 1_000_000 + row);
            }
            for (PreparedStatement statement : Arrays.asList(block, container, sign, entityContainer, entityInteraction)) {
                statement.executeBatch();
            }
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private static void writeRow(PreparedStatement block, PreparedStatement container, PreparedStatement sign, PreparedStatement entityContainer, PreparedStatement entityInteraction, long time, int index) throws SQLException {
        int user = 1 + (index % 5);

        // At the block: placed, broken, clicked, a minecart killed or a zombie killed, a minecart
        // spawned, with some rolled back.
        int kind = index % 10;
        int action = kind <= 3 ? 1 : kind <= 6 ? 0 : kind == 7 ? 2 : kind == 8 ? 3 : 13;
        int type = action == 3 ? (index % 20 == 8 ? MINECART : ZOMBIE) : action == 13 ? MINECART : 1 + (index % 40);
        addBlock(block, time, user, X, Y, type, index % 1000, action, index % 17 == 0 ? 1 : 0);
        if (index % 4 == 0) {
            // Neighbours in the same chunk, which the lookup has to leave out.
            addBlock(block, time, user, X, Y + 1, type, index % 1000, action, 0);
            addBlock(block, time, user, X + 2, Y, type, index % 1000, action, 0);
        }

        container.setLong(1, time);
        container.setInt(2, user);
        container.setInt(3, 1 + (index % 64));
        container.setInt(4, index % 1000);
        container.setInt(5, 1 + (index % 5));
        container.setInt(6, index % 2);
        container.addBatch();

        // The sign: placed with text on the front, on the back, with every line empty, or with no
        // lines at all, and now and then broken.
        sign.setLong(1, time);
        sign.setInt(2, user);
        sign.setInt(3, index % 6 == 0 ? 0 : 1);
        sign.setInt(4, index % 16);
        sign.setInt(5, index % 4 == 2 ? 1 : 0);
        for (int line = 1; line <= 8; line++) {
            String text;
            switch (index % 4) {
                case 0:
                    text = "";
                    break;
                case 1:
                    text = line == 1 ? "front " + index : "";
                    break;
                case 2:
                    text = line == 6 ? "back " + index : "";
                    break;
                default:
                    text = null;
                    break;
            }
            sign.setString(5 + line, text);
        }
        sign.addBatch();

        // The entity is wandering about, so its rows are spread over neighbouring places.
        entityContainer.setLong(1, time);
        entityContainer.setInt(2, user);
        entityContainer.setInt(3, X + 10 + (index % 3));
        entityContainer.setInt(4, Z + 10);
        entityContainer.setInt(5, 1 + (index % 64));
        entityContainer.setInt(6, index % 1000);
        entityContainer.setInt(7, 1 + (index % 5));
        entityContainer.setInt(8, index % 2);
        entityContainer.addBatch();

        entityInteraction.setLong(1, time);
        entityInteraction.setInt(2, user);
        entityInteraction.setInt(3, X + 10 + (index % 3));
        entityInteraction.setInt(4, Z + 10);
        entityInteraction.setInt(5, index % 4);
        entityInteraction.addBatch();
    }

    private static void addBlock(PreparedStatement block, long time, int user, int x, int y, int type, int data, int action, int rolledBack) throws SQLException {
        block.setLong(1, time);
        block.setInt(2, user);
        block.setInt(3, x);
        block.setInt(4, y);
        block.setInt(5, type);
        block.setInt(6, data);
        block.setInt(7, action);
        block.setInt(8, rolledBack);
        block.addBatch();
    }

    /** Copies every row the unpacked copy does not have yet, keeping its row id. */
    private static void copyToPlain() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("ATTACH DATABASE '" + directory.resolve("plain.db") + "' AS plain");
            for (String table : new String[] { "block", "container", "sign", "entity_container", "entity_interaction" }) {
                String columns = String.join(",", strings(connection, "SELECT name FROM pragma_table_info('co_" + table + "')"));
                statement.executeUpdate("INSERT INTO plain.co_" + table + " (rowid," + columns + ") SELECT rowid," + columns + " FROM main.co_" + table + " WHERE rowid > (SELECT IFNULL(MAX(rowid), 0) FROM plain.co_" + table + ")");
            }
            statement.executeUpdate("DETACH DATABASE plain");
        }
    }

    private static List<String> slice(List<String> rows, int offset) {
        if (offset >= rows.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(rows.subList(offset, Math.min(rows.size(), offset + LIMIT)));
    }

    private static String describe(Connection target, String query) throws SQLException {
        try (Statement statement = target.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return describe(results);
        }
    }

    private static String describe(ResultSet results) throws SQLException {
        if (!results.next()) {
            return "nothing";
        }
        StringBuilder described = new StringBuilder();
        for (int column = 1; column <= results.getMetaData().getColumnCount(); column++) {
            described.append(results.getString(column)).append('|');
        }
        return described.toString();
    }

    private static long median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        return sorted.get(sorted.size() / 2);
    }

    private static long count(Connection target, String query) throws SQLException {
        try (Statement statement = target.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private static List<String> strings(Connection target, String query) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Statement statement = target.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                values.add(results.getString(1));
            }
        }
        return values;
    }
}
