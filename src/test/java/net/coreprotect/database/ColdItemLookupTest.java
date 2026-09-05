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
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.model.action.EntityActionFilter;
import net.coreprotect.model.action.LookupActions;
import net.coreprotect.model.item.ItemTransactionActions;
import net.coreprotect.model.lookup.EntityLookupContext;
import net.coreprotect.model.lookup.LookupRollbackState;

/**
 * Runs item lookups through the real lookup path over a compacted item table, the way
 * {@code /co lookup <user> time:30d include:<item> action:+item} does, and requires every page to
 * hold exactly the rows plain SQL over the uncompressed rows would give.
 */
class ColdItemLookupTest {

    private static final long DAY = 86400L;
    private static final int USER_ID = 7;
    private static final int MACE = 900;
    private static final int PAGE = 7;

    private Connection connection;
    private DatabaseType previousType;
    private final List<String> messages = new CopyOnWriteArrayList<>();
    private CommandSender sender;
    private long now;
    private long startTime;

    @BeforeAll
    static void prepareServer() {
        if (org.bukkit.Bukkit.getServer() == null) {
            org.bukkit.Server server = Mockito.mock(org.bukkit.Server.class);
            ConsoleCommandSender console = Mockito.mock(ConsoleCommandSender.class);
            Mockito.doAnswer(invocation -> {
                System.out.println(String.valueOf(invocation.getArguments()[0]));
                return null;
            }).when(console).sendMessage(Mockito.anyString());
            Mockito.when(server.getConsoleSender()).thenReturn(console);
            Mockito.when(server.getLogger()).thenReturn(java.util.logging.Logger.getLogger("CoreProtectTest"));
            org.bukkit.Bukkit.setServer(server);
        }
    }

    @BeforeEach
    void openDatabase(@TempDir Path directory) throws SQLException {
        previousType = ConfigHandler.databaseType;
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        ConfigHandler.prefix = "co_";
        ConfigHandler.materials.put("minecraft:mace", MACE);
        ConfigHandler.playerIdCache.put("qwnks", USER_ID);
        Config.getGlobal().COLD_DEBUG = true;
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();

        connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("database.db"));
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            // The layout upstream CoreProtect wrote, which is what an imported database still has.
            statement.executeUpdate("CREATE TABLE co_item (id INTEGER PRIMARY KEY ASC, time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data BLOB, amount INTEGER, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_user (id INTEGER PRIMARY KEY ASC, time INTEGER, user TEXT, uuid TEXT);");
            statement.executeUpdate("CREATE TABLE co_block (id INTEGER PRIMARY KEY ASC, time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_container (id INTEGER PRIMARY KEY ASC, time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, amount INTEGER, metadata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE TABLE co_entity_container (id INTEGER PRIMARY KEY ASC, time INTEGER, user INTEGER, entity_spawn_rowid INTEGER NOT NULL, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, amount INTEGER, metadata BLOB, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
        }

        sender = Mockito.mock(CommandSender.class);
        Mockito.doAnswer(invocation -> {
            messages.add(String.valueOf(invocation.getArguments()[0]));
            return null;
        }).when(sender).sendMessage(Mockito.anyString());

        now = System.currentTimeMillis() / 1000L;
        startTime = now - (30 * DAY);
    }

    @AfterEach
    void closeDatabase() throws SQLException {
        SQLiteColdIndex.invalidate();
        SegmentDictionary.clearCache();
        ConfigHandler.databaseType = previousType;
        Config.getGlobal().COLD_DEBUG = false;
        Config.getGlobal().COLD_MAX_ROWS = 0;
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void pickupLookupOverColdItemsShowsEveryPage() throws Exception {
        insertItems(30, 20000, 3);
        compact();
        // action:+item, which the parser reports with an explicit entity action filter
        verifyLookup(Arrays.asList(LookupActions.ITEM, ItemTransactionActions.ADD), EntityActionFilter.NONE,
                "user = " + USER_ID + " AND type = " + MACE + " AND action IN(3,4) AND time > " + startTime);
    }

    @Test
    void dropLookupOverColdItemsShowsEveryPage() throws Exception {
        insertItems(30, 20000, 3);
        compact();
        verifyLookup(Arrays.asList(LookupActions.ITEM, ItemTransactionActions.REMOVE), EntityActionFilter.NONE,
                "user = " + USER_ID + " AND type = " + MACE + " AND action IN(2,5,6,7) AND time > " + startTime);
    }

    @Test
    void itemLookupOverColdItemsShowsEveryPage() throws Exception {
        insertItems(30, 20000, 3);
        compact();
        verifyLookup(Collections.singletonList(LookupActions.ITEM), EntityActionFilter.NONE,
                "user = " + USER_ID + " AND type = " + MACE + " AND action NOT IN(8,9,10,11,12) AND time > " + startTime);
    }

    @Test
    void sparseMatchesUnderALowRowLimitStillShowEveryPage() throws Exception {
        // Matches are rare enough that the newest segments hold none, and the limit on what one
        // lookup may read is as low as it can be set.
        Config.getGlobal().COLD_MAX_ROWS = 10000;
        insertItems(30, 20000, 10);
        compact();
        verifyLookup(Arrays.asList(LookupActions.ITEM, ItemTransactionActions.ADD), EntityActionFilter.NONE,
                "user = " + USER_ID + " AND type = " + MACE + " AND action IN(3,4) AND time > " + startTime);
    }

    @Test
    void inventoryWithdrawalsMergedAcrossTablesShowEveryPage() throws Exception {
        insertItems(30, 20000, 3);
        compact();
        // action:-inventory, which merges the block, container and item tables
        verifyLookup(Arrays.asList(LookupActions.CONTAINER, LookupActions.ITEM, ItemTransactionActions.REMOVE), EntityActionFilter.NONE,
                "user = " + USER_ID + " AND type = " + MACE + " AND action IN(0,3,4,10,12) AND time > " + startTime, LookupRollbackState.ANY);
    }

    @Test
    void pagesTheQueryRejectsMostOfAreReadFurtherBackUntilFull() throws Exception {
        // The compressed reader does not evaluate the rolled back state, so it hands over every
        // pickup and the query keeps the few that were rolled back. Each page has to be read
        // further back until it is full, and the read must stay bounded while it does.
        insertItems(30, 20000, 3);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE co_item SET rolled_back = 2 WHERE user = " + USER_ID + " AND type = " + MACE + " AND action = 3 AND (rowid % 5) = 0");
        }
        compact();
        verifyLookup(Arrays.asList(LookupActions.ITEM, ItemTransactionActions.ADD), EntityActionFilter.NONE,
                "user = " + USER_ID + " AND type = " + MACE + " AND action IN(3,4) AND rolled_back IN(2,3) AND time > " + startTime, LookupRollbackState.ROLLED_BACK);
    }

    private void compact() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE co_item_all AS SELECT * FROM co_item");
        }
        long sealed = ColdRollupTask.rollUp(connection, () -> {
        });
        assertTrue(sealed > 0, "rows were compressed");
        SQLiteColdIndex.reload(connection);
        // Recent rows, which stay in the live table and are merged in.
        insertLiveItems(50);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT OR IGNORE INTO co_item_all SELECT * FROM co_item");
        }
    }

    private void verifyLookup(List<Integer> actionList, EntityActionFilter filter, String where) throws Exception {
        verifyLookup(actionList, filter, where, LookupRollbackState.ANY);
    }

    private void verifyLookup(List<Integer> actionList, EntityActionFilter filter, String where, LookupRollbackState rollbackState) throws Exception {
        long expectedTotal = count("SELECT COUNT(*) FROM co_item_all WHERE " + where);
        List<Long> expectedIds = ids("SELECT id FROM co_item_all WHERE " + where + " ORDER BY time DESC, id DESC");
        assertTrue(expectedTotal > 3 * PAGE, "the sample must span several pages, had " + expectedTotal);

        List<Integer> actions = new ArrayList<>(actionList);
        List<Object> restrict = new ArrayList<>(Collections.singletonList("mace"));
        List<String> users = Collections.singletonList("qwnks");
        Long[] rowData = new Long[] { 0L, 0L, 0L, 0L, 0L };

        try (Statement statement = connection.createStatement()) {
            long total = Lookup.countLookupRows(statement, sender, Collections.emptyList(), users, restrict, new HashMap<>(), Collections.emptyList(), actions, filter, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, rowData, startTime, 0, false, true, null, rollbackState);
            assertEquals(expectedTotal, total, "the count matches plain SQL");

            int pages = (int) Math.ceil(expectedTotal / (double) PAGE);
            for (int page = 1; page <= pages; page++) {
                int offset = (page - 1) * PAGE;
                messages.clear();
                List<Object[]> rows = LookupRaw.performLookupRaw(statement, sender, Collections.emptyList(), users, restrict, new HashMap<>(), Collections.emptyList(), actions, filter, Collections.emptyList(), EntityLookupContext.legacy(Collections.emptySet(), Collections.emptySet()), null, null, rowData, startTime, 0, offset, PAGE, false, true, null, rollbackState);
                assertTrue(messages.isEmpty(), "page " + page + " sent " + messages);
                List<Long> expectedPage = expectedIds.subList(offset, Math.min(expectedIds.size(), offset + PAGE));
                List<Long> actualPage = new ArrayList<>();
                for (Object[] row : rows) {
                    actualPage.add((Long) row[0]);
                }
                assertEquals(expectedPage, actualPage, "page " + page + " of " + pages);
            }
        }
    }

    private void insertItems(int days, int rowsPerDay, int rarity) throws SQLException {
        Random random = new Random(42);
        long firstDay = ((now - ((days + 9) * DAY)) / DAY) * DAY;
        String insert = "INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int day = 0; day < days; day++) {
                long dayStart = firstDay + (day * DAY);
                for (int index = 0; index < rowsPerDay; index++) {
                    long time = dayStart + 60 + ((index * 80000L) / rowsPerDay);
                    int user = 1 + random.nextInt(40);
                    int type = random.nextInt(10 * rarity) == 0 ? MACE : 800 + random.nextInt(30);
                    int action = 2 + random.nextInt(2);
                    addItem(statement, time, user, 1 + (index % 3), type, action, index, day);
                }
                statement.executeBatch();
            }
        }
        connection.commit();
        connection.setAutoCommit(true);
    }

    private void insertLiveItems(int rows) throws SQLException {
        String insert = "INSERT INTO co_item (time,user,wid,x,y,z,type,data,amount,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            for (int index = 0; index < rows; index++) {
                addItem(statement, now - 3600 + index, index % 2 == 0 ? USER_ID : 3, 1, index % 3 == 0 ? MACE : 801, 2 + (index % 2), index, 99);
            }
            statement.executeBatch();
        }
    }

    private static void addItem(PreparedStatement statement, long time, int user, int wid, int type, int action, int index, int day) throws SQLException {
        statement.setLong(1, time);
        statement.setInt(2, user);
        statement.setInt(3, wid);
        statement.setInt(4, index % 500);
        statement.setInt(5, 64);
        statement.setInt(6, (index * 7) % 500);
        statement.setInt(7, type);
        statement.setBytes(8, new byte[] { (byte) index, (byte) day, 1, 2, 3 });
        statement.setInt(9, 1 + (index % 64));
        statement.setInt(10, action);
        statement.setInt(11, 0);
        statement.addBatch();
    }

    private long count(String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            return results.next() ? results.getLong(1) : 0;
        }
    }

    private List<Long> ids(String query) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery(query)) {
            while (results.next()) {
                ids.add(results.getLong(1));
            }
        }
        return ids;
    }
}
