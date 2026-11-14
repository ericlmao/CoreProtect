package net.coreprotect.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates block activity shaped like a real server's, for tests and benchmarks that depend on how
 * activity is distributed rather than only on how much of it there is.
 *
 * <p>
 * Uniformly random coordinates make every segment cover the whole world, which no server does:
 * players build around a home, wander a little, and crowd around spawn. Each simulated player here
 * has a home, walks around it, spends part of the time at spawn, and works with a small palette of
 * block types, and players who are online at once have their actions interleaved in time the way
 * the consumer would write them.
 * </p>
 */
final class RealisticActivity {

    static final long DAY = 86400L;

    /** A block location that was acted on, for choosing where lookups look. */
    static final class Spot {
        final int x;
        final int y;
        final int z;
        final int user;

        Spot(int x, int y, int z, int user) {
            this.x = x;
            this.y = y;
            this.z = z;
            this.user = user;
        }
    }

    private final Random random;
    private final int players;
    private final int[] homeX;
    private final int[] homeZ;
    private final int[] posX;
    private final int[] posZ;
    private final int[][] palette;
    private final List<Spot> spots = new ArrayList<>();

    RealisticActivity(long seed, int players) {
        this.random = new Random(seed);
        this.players = players;
        this.homeX = new int[players];
        this.homeZ = new int[players];
        this.posX = new int[players];
        this.posZ = new int[players];
        this.palette = new int[players][];
        for (int player = 0; player < players; player++) {
            homeX[player] = random.nextInt(10000) - 5000;
            homeZ[player] = random.nextInt(10000) - 5000;
            posX[player] = homeX[player];
            posZ[player] = homeZ[player];
            palette[player] = new int[12];
            for (int index = 0; index < palette[player].length; index++) {
                palette[player][index] = 1 + random.nextInt(900);
            }
        }
    }

    /**
     * @return the online players' ids, which lookups by player use
     */
    int players() {
        return players;
    }

    /** Creates the block table the way an upgraded database has it. */
    static void createBlockTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS co_block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS co_block_index ON co_block(wid,x,z,time);");
            statement.executeUpdate("CREATE INDEX IF NOT EXISTS co_block_user_index ON co_block(user,time);");
            SQLiteSchema.createTables("co_", statement);
        }
    }

    /**
     * Writes activity into the block table.
     *
     * @param connection
     *            an open connection
     * @param firstTime
     *            the timestamp of the first row
     * @param rows
     *            how many rows to write
     * @param perSecond
     *            how many rows are logged per second of game time
     * @throws SQLException
     *             if the rows cannot be written
     */
    void writeBlocks(Connection connection, long firstTime, int rows, int perSecond) throws SQLException {
        writeBlocks(connection, rows, row -> firstTime + row / perSecond);
    }

    /**
     * Writes activity into the block table, spread evenly over a span of time, for tests that need
     * a day to hold fewer rows than one a second.
     *
     * @param connection
     *            an open connection
     * @param firstTime
     *            the timestamp of the first row
     * @param rows
     *            how many rows to write
     * @param seconds
     *            the span of time the rows are spread over
     * @throws SQLException
     *             if the rows cannot be written
     */
    void writeBlocksOver(Connection connection, long firstTime, int rows, long seconds) throws SQLException {
        writeBlocks(connection, rows, row -> firstTime + (row * seconds) / rows);
    }

    private void writeBlocks(Connection connection, int rows, java.util.function.LongUnaryOperator timeOfRow) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_block (time,user,wid,x,y,z,type,data,meta,blockdata,action,rolled_back) VALUES (?,?,?,?,?,?,?,?,NULL,?,?,0)")) {
            int online = Math.max(1, players / 3);
            int[] session = new int[online];
            for (int index = 0; index < online; index++) {
                session[index] = random.nextInt(players);
            }
            for (int row = 0; row < rows; row++) {
                if (random.nextInt(2000) == 0) {
                    session[random.nextInt(online)] = random.nextInt(players); // someone logs off, someone else on
                }
                int player = session[random.nextInt(online)];
                step(player);
                int x = posX[player] + random.nextInt(9) - 4;
                int z = posZ[player] + random.nextInt(9) - 4;
                int y = 60 + random.nextInt(30);
                int type = random.nextInt(5) == 0 ? 1 + random.nextInt(900) : palette[player][random.nextInt(palette[player].length)];

                statement.setLong(1, timeOfRow.applyAsLong(row));
                statement.setInt(2, player + 1);
                statement.setInt(3, 1);
                statement.setInt(4, x);
                statement.setInt(5, y);
                statement.setInt(6, z);
                statement.setInt(7, type);
                statement.setInt(8, 0);
                statement.setBytes(9, random.nextInt(5) == 0 ? ("minecraft:block_" + type + "[facing=" + (random.nextBoolean() ? "north" : "east") + "]").getBytes() : null);
                statement.setInt(10, random.nextInt(3) == 0 ? 0 : 1);
                statement.addBatch();
                if (row % 997 == 0) {
                    spots.add(new Spot(x, y, z, player + 1));
                }
                if (row % 20000 == 19999) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(autoCommit);
    }

    /** Creates the container table the way an upgraded database has it, without indexes. */
    static void createContainerTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            SQLiteSchema.applyFileSettings(statement);
            statement.executeUpdate("PRAGMA journal_mode=WAL");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS co_container (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, "
                    + "amount INTEGER, metadata BLOB, action INTEGER, rolled_back INTEGER);");
            SQLiteSchema.createTables("co_", statement);
        }
    }

    /**
     * Writes a busy server's container activity: every player keeps a few chests at home and uses
     * the shops at spawn, and farms of hoppers move items all day long at fixed places, which is
     * what makes the container table the busiest one. Items that carry data of their own, enchanted
     * or named ones, are logged with a blob of it.
     *
     * @param connection
     *            an open connection
     * @param firstTime
     *            the timestamp of the first row
     * @param rows
     *            how many rows to write
     * @param seconds
     *            the span of time the rows are spread over
     * @throws SQLException
     *             if the rows cannot be written
     */
    void writeContainers(Connection connection, long firstTime, int rows, long seconds) throws SQLException {
        if (chests == null) {
            chests = new int[players][CHESTS_PER_PLAYER * 3];
            for (int player = 0; player < players; player++) {
                for (int chest = 0; chest < CHESTS_PER_PLAYER; chest++) {
                    chests[player][chest * 3] = homeX[player] + random.nextInt(24) - 12;
                    chests[player][chest * 3 + 1] = 60 + random.nextInt(8);
                    chests[player][chest * 3 + 2] = homeZ[player] + random.nextInt(24) - 12;
                }
            }
            shops = new int[SHOPS * 3];
            for (int shop = 0; shop < SHOPS; shop++) {
                shops[shop * 3] = random.nextInt(120) - 60;
                shops[shop * 3 + 1] = 64 + random.nextInt(4);
                shops[shop * 3 + 2] = random.nextInt(120) - 60;
            }
            farms = new int[FARMS * 3];
            for (int farm = 0; farm < FARMS; farm++) {
                int owner = random.nextInt(players);
                farms[farm * 3] = homeX[owner] + random.nextInt(200) - 100;
                farms[farm * 3 + 1] = 40 + random.nextInt(30);
                farms[farm * 3 + 2] = homeZ[owner] + random.nextInt(200) - 100;
            }
        }

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO co_container (time,user,wid,x,y,z,type,data,amount,metadata,action,rolled_back) VALUES (?,?,1,?,?,?,?,0,?,?,?,0)")) {
            int online = Math.max(1, players / 3);
            int[] session = new int[online];
            for (int index = 0; index < online; index++) {
                session[index] = random.nextInt(players);
            }
            for (int row = 0; row < rows; row++) {
                if (random.nextInt(2000) == 0) {
                    session[random.nextInt(online)] = random.nextInt(players);
                }
                int user;
                int x;
                int y;
                int z;
                int type;
                int roll = random.nextInt(10);
                if (roll < 4) {
                    // A hopper of a farm, moving the farm's one or two items along.
                    int farm = random.nextInt(FARMS);
                    user = HOPPER_USER;
                    x = farms[farm * 3];
                    y = farms[farm * 3 + 1] + random.nextInt(3);
                    z = farms[farm * 3 + 2];
                    type = 100 + (farm % 40) + random.nextInt(2);
                }
                else {
                    int player = session[random.nextInt(online)];
                    user = player + 1;
                    int[] chest;
                    int index;
                    if (roll < 8) {
                        chest = chests[player];
                        index = random.nextInt(CHESTS_PER_PLAYER);
                    }
                    else {
                        chest = shops;
                        index = random.nextInt(SHOPS);
                    }
                    x = chest[index * 3];
                    y = chest[index * 3 + 1];
                    z = chest[index * 3 + 2];
                    type = palette[player][random.nextInt(palette[player].length)];
                }

                statement.setLong(1, firstTime + (row * seconds) / rows);
                statement.setInt(2, user);
                statement.setInt(3, x);
                statement.setInt(4, y);
                statement.setInt(5, z);
                statement.setInt(6, type);
                statement.setInt(7, 1 + random.nextInt(64));
                statement.setBytes(8, random.nextInt(7) == 0 ? itemData(type) : null);
                statement.setInt(9, random.nextInt(2));
                statement.addBatch();
                if (row % 997 == 0 && user != HOPPER_USER) {
                    containerSpots.add(new Spot(x, y, z, user));
                }
                if (row % 20000 == 19999) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(autoCommit);
    }

    /** The data an enchanted or named item carries, shaped like the maps items are logged with. */
    private byte[] itemData(int type) {
        StringBuilder data = new StringBuilder("{type=").append(type).append(",meta={");
        if (random.nextBoolean()) {
            data.append("display-name=\"").append(NAMES[random.nextInt(NAMES.length)]).append(" #").append(random.nextInt(500)).append("\",");
        }
        int enchantments = random.nextInt(4);
        for (int index = 0; index < enchantments; index++) {
            data.append("enchant.").append(ENCHANTMENTS[random.nextInt(ENCHANTMENTS.length)]).append('=').append(1 + random.nextInt(5)).append(',');
        }
        data.append("damage=").append(random.nextInt(1500)).append("}}");
        return data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * @return containers players used, spread across the whole history written
     */
    List<Spot> containerSpots() {
        return containerSpots;
    }

    private static final int CHESTS_PER_PLAYER = 15;
    private static final int SHOPS = 60;
    private static final int FARMS = 120;
    static final int HOPPER_USER = 9000;
    private static final String[] NAMES = { "Excalibur", "Bob's Pick", "Shiny", "Diggy", "Old Faithful", "Sharp One", "Lucky" };
    private static final String[] ENCHANTMENTS = { "sharpness", "efficiency", "unbreaking", "fortune", "mending", "protection", "silk_touch" };
    private int[][] chests;
    private int[] shops;
    private int[] farms;
    private final List<Spot> containerSpots = new ArrayList<>();

    /** Moves a player: mostly a short walk, sometimes home, sometimes to spawn. */
    private void step(int player) {
        int roll = random.nextInt(4000);
        if (roll == 0) {
            posX[player] = random.nextInt(300) - 150;
            posZ[player] = random.nextInt(300) - 150;
        }
        else if (roll == 1) {
            posX[player] = homeX[player];
            posZ[player] = homeZ[player];
        }
        else if (roll < 60) {
            posX[player] = posX[player] + random.nextInt(33) - 16;
            posZ[player] = posZ[player] + random.nextInt(33) - 16;
        }
    }

    /**
     * @return places that were acted on, spread across the whole history written
     */
    List<Spot> spots() {
        return spots;
    }
}
