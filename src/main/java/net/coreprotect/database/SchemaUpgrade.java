package net.coreprotect.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Brings tables created by older builds up to the columns the current code reads, every time the
 * schema is opened.
 *
 * <p>
 * The patch scripts only run for versions up to the plugin's own, so a column that a later patch
 * adds would otherwise never reach an existing database while this build runs, whatever version
 * the database records. Every step here is idempotent, never touches existing values, and never
 * rebuilds a table: a column is only ever appended with a constant default, which SQLite, DuckDB
 * and MySQL 8 apply to existing rows without rewriting them.
 * </p>
 */
public final class SchemaUpgrade {

    private static final Logger LOGGER = Logger.getLogger("CoreProtect");

    /** MySQL's error code for a column that already exists. */
    private static final int MYSQL_DUPLICATE_COLUMN = 1060;

    private SchemaUpgrade() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Adds every column the current code reads that the tables may lack.
     *
     * <ul>
     * <li>{@code sign.rolled_back}, added by the 2.25.1 patch, which sign rollbacks and restores read
     * and write. Existing sign rows read as not rolled back, which is what they are.</li>
     * </ul>
     *
     * @param statement
     *            a statement on a connection to the database
     * @param prefix
     *            the table prefix
     * @param databaseType
     *            the backend; ClickHouse manages its own schema and is left alone
     * <p>
     * A column that cannot be added is reported and left missing rather than stopping the plugin:
     * everything else keeps logging, and only what reads the column fails.
     * </p>
     *
     * @return true if a column was added
     */
    public static boolean ensureColumns(Statement statement, String prefix, DatabaseType databaseType) {
        if (databaseType.isClickHouse()) {
            return false;
        }
        String table = prefix + "sign";
        boolean added;
        try {
            added = ensureColumn(statement, databaseType, table, "rolled_back", databaseType.isSQLite() ? "INTEGER DEFAULT 0" : "TINYINT DEFAULT 0");
        }
        catch (SQLException exception) {
            LOGGER.log(Level.SEVERE, "[CoreProtect] Unable to add the rolled_back column to " + table + "; sign rollbacks will fail until it is added", exception);
            return false;
        }
        if (added && databaseType.isSQLite()) {
            // A layout read before the column existed would leave it out of every segment packed
            // from now on.
            SQLiteColdIndex.forgetLayout("sign");
        }
        return added;
    }

    private static boolean ensureColumn(Statement statement, DatabaseType databaseType, String table, String column, String definition) throws SQLException {
        ColumnState state = columnState(statement, databaseType, table, column);
        if (state != ColumnState.MISSING) {
            return false;
        }
        try {
            statement.executeUpdate("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
        }
        catch (SQLException exception) {
            // Another server sharing the database added it first.
            if (databaseType.isMySQL() && exception.getErrorCode() == MYSQL_DUPLICATE_COLUMN) {
                return false;
            }
            throw exception;
        }
        LOGGER.info("[CoreProtect] Added the " + column + " column to " + table + "; existing rows keep their values.");
        return true;
    }

    /** Whether a table has a column, or has not been created at all. */
    private enum ColumnState {
        PRESENT,
        MISSING,
        NO_TABLE
    }

    private static ColumnState columnState(Statement statement, DatabaseType databaseType, String table, String column) throws SQLException {
        if (databaseType.isMySQL()) {
            return mysqlColumnState(statement, table, column);
        }
        if (databaseType.isDuckDB()) {
            return duckDBColumnState(statement, table, column);
        }
        boolean tableFound = false;
        try (ResultSet results = statement.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (results.next()) {
                tableFound = true;
                if (column.equalsIgnoreCase(results.getString("name"))) {
                    return ColumnState.PRESENT;
                }
            }
        }
        return tableFound ? ColumnState.MISSING : ColumnState.NO_TABLE;
    }

    private static ColumnState mysqlColumnState(Statement statement, String table, String column) throws SQLException {
        String query = "SELECT COUNT(*), SUM(column_name = ?) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ?";
        return informationSchemaState(statement, query, table, column);
    }

    private static ColumnState duckDBColumnState(Statement statement, String table, String column) throws SQLException {
        String query = "SELECT COUNT(*), COUNT(*) FILTER (WHERE lower(column_name) = lower(?)) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ?";
        return informationSchemaState(statement, query, table, column);
    }

    private static ColumnState informationSchemaState(Statement statement, String query, String table, String column) throws SQLException {
        try (PreparedStatement prepared = statement.getConnection().prepareStatement(query)) {
            prepared.setString(1, column);
            prepared.setString(2, table);
            try (ResultSet results = prepared.executeQuery()) {
                if (!results.next() || results.getLong(1) == 0) {
                    return ColumnState.NO_TABLE;
                }
                return results.getLong(2) > 0 ? ColumnState.PRESENT : ColumnState.MISSING;
            }
        }
    }
}
