package net.coreprotect.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The WHERE clause of a lookup, read back into conditions the compressed reader can test on its own.
 *
 * <p>
 * A page only ever shows the rows its ordering ranks highest. When the reader can test every
 * condition of the query itself, it knows exactly which compressed rows the query would keep, and
 * so which of them can possibly reach the page, without handing everything else to the database
 * to sort. That is only safe when nothing in the clause is left untested, so a clause is only read
 * when every part of it is understood: comparisons of whole numbers, tests that a column holds text,
 * and choices between conjunctions of these. Anything else, from a text match to a subquery, leaves
 * the lookup on the path that hands every row to the query.
 * </p>
 */
final class ColdPagePredicate {

    static final int EQUAL = 0;
    static final int NOT_EQUAL = 1;
    static final int LESS = 2;
    static final int LESS_OR_EQUAL = 3;
    static final int GREATER = 4;
    static final int GREATER_OR_EQUAL = 5;
    static final int IN = 6;
    static final int NOT_IN = 7;
    /** {@code LENGTH(column) > 0}: the column holds a value of at least one byte. */
    static final int HAS_TEXT = 8;
    /** A choice between several conjunctions, at least one of which has to hold. */
    static final int ANY_OF = 9;

    /** A row the query keeps. */
    static final int MATCH = 0;
    /** A row the query rejects. */
    static final int NO_MATCH = 1;
    /** A row whose value is of a kind the reader does not compare, so only the query can decide. */
    static final int UNKNOWN = 2;

    private static final String NUMBER = "'?(-?[0-9]+)'?";
    private static final Pattern COMPARISON = Pattern.compile("([A-Za-z_][A-Za-z0-9_]*)\\s*(=|==|!=|<>|<=|>=|<|>)\\s*" + NUMBER);
    private static final Pattern LIST = Pattern.compile("([A-Za-z_][A-Za-z0-9_]*)\\s+(NOT\\s+)?IN\\s*\\(([-0-9,\\s]+)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern NUMBER_ONLY = Pattern.compile("-?[0-9]+");
    private static final Pattern HAS_LENGTH = Pattern.compile("LENGTH\\s*\\(\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*\\)\\s*>\\s*0", Pattern.CASE_INSENSITIVE);

    /**
     * One comparison of a column against a constant or a list of constants, a test that a column
     * holds text, or a choice between several conjunctions of these.
     */
    static final class Condition {
        /** The column compared, or null for a choice between conjunctions. */
        final String column;
        final int operator;
        final long value;
        final long[] values;
        /** For {@link #ANY_OF}, the conjunctions to choose between; otherwise null. */
        final List<List<Condition>> alternatives;

        private Condition(String column, int operator, long value, long[] values) {
            this(column, operator, value, values, null);
        }

        private Condition(String column, int operator, long value, long[] values, List<List<Condition>> alternatives) {
            this.column = column;
            this.operator = operator;
            this.value = value;
            this.values = values;
            this.alternatives = alternatives;
        }

        /**
         * @param candidate
         *            a column value
         * @return whether the comparison holds for it
         */
        boolean holds(long candidate) {
            switch (operator) {
                case EQUAL:
                    return candidate == value;
                case NOT_EQUAL:
                    return candidate != value;
                case LESS:
                    return candidate < value;
                case LESS_OR_EQUAL:
                    return candidate <= value;
                case GREATER:
                    return candidate > value;
                case GREATER_OR_EQUAL:
                    return candidate >= value;
                case IN:
                    return Arrays.binarySearch(values, candidate) >= 0;
                default:
                    return Arrays.binarySearch(values, candidate) < 0;
            }
        }

        /**
         * @return the comparison written back as SQL, for asking the live table the same question
         */
        String toSql() {
            if (operator == ANY_OF) {
                StringBuilder sql = new StringBuilder("(");
                for (int alternative = 0; alternative < alternatives.size(); alternative++) {
                    sql.append(alternative > 0 ? " OR (" : "(");
                    List<Condition> conjunction = alternatives.get(alternative);
                    for (int index = 0; index < conjunction.size(); index++) {
                        sql.append(index > 0 ? " AND " : "").append(conjunction.get(index).toSql());
                    }
                    sql.append(conjunction.isEmpty() ? "1)" : ")");
                }
                return sql.append(')').toString();
            }
            if (operator == HAS_TEXT) {
                return "LENGTH(" + column + ") > 0";
            }
            StringBuilder sql = new StringBuilder(column);
            if (operator == IN || operator == NOT_IN) {
                sql.append(operator == IN ? " IN(" : " NOT IN(");
                for (int index = 0; index < values.length; index++) {
                    if (index > 0) {
                        sql.append(',');
                    }
                    sql.append(values[index]);
                }
                return sql.append(')').toString();
            }
            String[] symbols = { " = ", " != ", " < ", " <= ", " > ", " >= " };
            return sql.append(symbols[operator]).append(value).toString();
        }

        /**
         * @return false only if no value between the two bounds can satisfy the comparison
         */
        boolean mayHold(long minimum, long maximum) {
            switch (operator) {
                case EQUAL:
                    return value >= minimum && value <= maximum;
                case NOT_EQUAL:
                    return minimum != maximum || minimum != value;
                case LESS:
                    return minimum < value;
                case LESS_OR_EQUAL:
                    return minimum <= value;
                case GREATER:
                    return maximum > value;
                case GREATER_OR_EQUAL:
                    return maximum >= value;
                case IN:
                    return SQLiteColdIndex.anyWithin(values, minimum, maximum);
                default:
                    return minimum != maximum || Arrays.binarySearch(values, minimum) < 0;
            }
        }
    }

    private final List<Condition> conditions;

    private ColdPagePredicate(List<Condition> conditions) {
        this.conditions = Collections.unmodifiableList(conditions);
    }

    /**
     * @return every condition of the clause, all of which a row has to satisfy; a choice between
     *         alternatives is one condition
     */
    List<Condition> conditions() {
        return conditions;
    }

    /**
     * Reads a WHERE clause into conditions, but only when every part of it is understood.
     *
     * @param where
     *            the clause, without the WHERE keyword
     * @param userColumn
     *            the name the query uses for the user column
     * @return the conditions, or null when any part of the clause cannot be tested by the reader
     */
    static ColdPagePredicate parse(String where, String userColumn) {
        if (where == null) {
            return null;
        }
        List<Condition> conditions = new ArrayList<>();
        String user = userColumn == null || userColumn.isEmpty() ? "user" : userColumn.replace("\"", "").replace("`", "");
        return readConjunction(where, user.toLowerCase(Locale.ROOT), conditions, 0) ? new ColdPagePredicate(conditions) : null;
    }

    private static boolean readConjunction(String clause, String userColumn, List<Condition> conditions, int depth) {
        if (depth > 8) {
            return false;
        }
        // OR binds more loosely than AND, so a clause is first a choice between its alternatives,
        // and each alternative a conjunction.
        List<String> choices = split(clause, "OR");
        if (choices == null) {
            return false;
        }
        if (choices.size() > 1) {
            List<List<Condition>> alternatives = new ArrayList<>();
            for (String choice : choices) {
                List<Condition> alternative = new ArrayList<>();
                if (choice.trim().isEmpty() || !readConjunction(choice, userColumn, alternative, depth + 1)) {
                    return false;
                }
                if (alternative.isEmpty()) {
                    return true; // an alternative with no condition always holds, and so does the choice
                }
                alternatives.add(Collections.unmodifiableList(alternative));
            }
            conditions.add(new Condition(null, ANY_OF, 0, null, Collections.unmodifiableList(alternatives)));
            return true;
        }
        List<String> parts = split(clause, "AND");
        if (parts == null) {
            return false;
        }
        for (String part : parts) {
            String term = part.trim();
            if (term.isEmpty()) {
                return false;
            }
            if (term.equals("1")) {
                continue; // the query's way of saying there is no condition at all
            }
            if (enclosedInParentheses(term)) {
                if (!readConjunction(term.substring(1, term.length() - 1), userColumn, conditions, depth + 1)) {
                    return false;
                }
                continue;
            }
            Condition condition = readCondition(term, userColumn);
            if (condition == null) {
                return false;
            }
            conditions.add(condition);
        }
        return true;
    }

    /**
     * @param where
     *            a WHERE clause, without the keyword
     * @return true if the clause offers alternatives anywhere, in which case only the parts outside
     *         them are certain to hold
     */
    static boolean hasAlternatives(String where) {
        return where != null && ALTERNATIVE.matcher(where).find();
    }

    private static final Pattern ALTERNATIVE = Pattern.compile("\\sOR\\s", Pattern.CASE_INSENSITIVE);

    /**
     * Leaves out every choice between alternatives, so what remains is only what every matching row
     * has to satisfy. Anything that reads restrictions out of a clause's text has to read them from
     * this, or it would take one alternative for a requirement.
     *
     * @param where
     *            a WHERE clause, without the keyword
     * @return the clause with each choice replaced by {@code 1}
     */
    static String requiredPart(String where) {
        if (where == null || !hasAlternatives(where)) {
            return where;
        }
        List<String> choices = split(where, "OR");
        if (choices == null) {
            return where;
        }
        if (choices.size() > 1) {
            return "1";
        }
        List<String> parts = split(where, "AND");
        if (parts == null) {
            return where;
        }
        StringBuilder required = new StringBuilder();
        for (String part : parts) {
            String term = part.trim();
            if (required.length() > 0) {
                required.append(" AND ");
            }
            required.append(enclosedInParentheses(term) ? "(" + requiredPart(term.substring(1, term.length() - 1)) + ")" : term);
        }
        return required.toString();
    }

    /**
     * Splits a clause at every AND, or every OR, that is not inside parentheses or quotes.
     *
     * @param keyword
     *            AND or OR
     * @return the parts, or null when the parentheses or quotes of the clause do not balance
     */
    private static List<String> split(String clause, String keyword) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean quoted = false;
        int start = 0;
        for (int index = 0; index < clause.length(); index++) {
            char character = clause.charAt(index);
            if (character == '\'') {
                quoted = !quoted;
                continue;
            }
            if (quoted) {
                continue;
            }
            if (character == '(') {
                depth++;
            }
            else if (character == ')') {
                depth--;
                if (depth < 0) {
                    return null;
                }
            }
            else if (depth == 0 && Character.isWhitespace(character) && clause.regionMatches(true, index + 1, keyword, 0, keyword.length())
                    && index + keyword.length() + 1 < clause.length() && Character.isWhitespace(clause.charAt(index + keyword.length() + 1))) {
                parts.add(clause.substring(start, index));
                start = index + keyword.length() + 1;
                index = index + keyword.length();
            }
        }
        if (depth != 0 || quoted) {
            return null;
        }
        parts.add(clause.substring(start));
        return parts;
    }

    private static boolean enclosedInParentheses(String term) {
        if (!term.startsWith("(") || !term.endsWith(")")) {
            return false;
        }
        int depth = 0;
        for (int index = 0; index < term.length(); index++) {
            char character = term.charAt(index);
            if (character == '(') {
                depth++;
            }
            else if (character == ')') {
                depth--;
                if (depth == 0 && index < term.length() - 1) {
                    return false; // the opening parenthesis closes before the end, as in (a) AND (b)
                }
            }
        }
        return depth == 0;
    }

    private static Condition readCondition(String term, String userColumn) {
        Matcher length = HAS_LENGTH.matcher(term);
        if (length.matches()) {
            return new Condition(columnName(length.group(1), userColumn), HAS_TEXT, 0, null);
        }
        Matcher list = LIST.matcher(term);
        if (list.matches()) {
            long[] values = readList(list.group(3));
            if (values == null) {
                return null;
            }
            return new Condition(columnName(list.group(1), userColumn), list.group(2) != null ? NOT_IN : IN, 0, values);
        }

        Matcher comparison = COMPARISON.matcher(term);
        if (!comparison.matches() || !balancedQuotes(term)) {
            return null;
        }
        long value;
        try {
            value = Long.parseLong(comparison.group(3));
        }
        catch (NumberFormatException exception) {
            return null;
        }
        return new Condition(columnName(comparison.group(1), userColumn), operator(comparison.group(2)), value, null);
    }

    /** A number is either quoted on both sides or on neither; anything else is not a number. */
    private static boolean balancedQuotes(String term) {
        int quotes = 0;
        for (int index = 0; index < term.length(); index++) {
            if (term.charAt(index) == '\'') {
                quotes++;
            }
        }
        return quotes == 0 || (quotes == 2 && term.endsWith("'"));
    }

    private static long[] readList(String list) {
        String[] parts = list.split(",");
        long[] values = new long[parts.length];
        for (int index = 0; index < parts.length; index++) {
            String part = parts[index].trim();
            if (!NUMBER_ONLY.matcher(part).matches()) {
                return null;
            }
            try {
                values[index] = Long.parseLong(part);
            }
            catch (NumberFormatException exception) {
                return null;
            }
        }
        Arrays.sort(values);
        return values;
    }

    private static String columnName(String column, String userColumn) {
        String name = column.toLowerCase(Locale.ROOT);
        return name.equals(userColumn) ? "user" : name;
    }

    private static int operator(String symbol) {
        switch (symbol) {
            case "=":
            case "==":
                return EQUAL;
            case "!=":
            case "<>":
                return NOT_EQUAL;
            case "<":
                return LESS;
            case "<=":
                return LESS_OR_EQUAL;
            case ">":
                return GREATER;
            default:
                return GREATER_OR_EQUAL;
        }
    }


    /**
     * Ties the conditions to the columns of one table.
     *
     * @param layout
     *            the column layout of the table the clause is for
     * @return the conditions bound to column positions, or null when a condition names a column the
     *         table does not have, or one whose values the reader does not hold in the form the
     *         condition compares
     */
    Bound bind(SQLiteColdIndex.TableLayout layout) {
        return bind(conditions, layout);
    }

    private static Bound bind(List<Condition> conditions, SQLiteColdIndex.TableLayout layout) {
        int[] columns = new int[conditions.size()];
        Bound[][] alternatives = new Bound[conditions.size()][];
        for (int index = 0; index < columns.length; index++) {
            Condition condition = conditions.get(index);
            if (condition.operator == ANY_OF) {
                Bound[] bound = new Bound[condition.alternatives.size()];
                for (int alternative = 0; alternative < bound.length; alternative++) {
                    bound[alternative] = bind(condition.alternatives.get(alternative), layout);
                    if (bound[alternative] == null) {
                        return null;
                    }
                }
                alternatives[index] = bound;
                columns[index] = -1;
                continue;
            }
            int column = columnIndex(layout, condition.column);
            if (column < 0) {
                return null;
            }
            int type = layout.types[column];
            // Text and blob values are known to the reader by their length alone, which is all a
            // test for text needs; every other comparison needs the whole number.
            boolean readable = condition.operator == HAS_TEXT ? type == ColdSegmentCodec.TYPE_TEXT || type == ColdSegmentCodec.TYPE_BLOB : type == ColdSegmentCodec.TYPE_INTEGER;
            if (!readable) {
                return null;
            }
            columns[index] = column;
        }
        return new Bound(conditions.toArray(new Condition[0]), columns, alternatives, layout.rolledBackColumn);
    }

    private static int columnIndex(SQLiteColdIndex.TableLayout layout, String column) {
        for (int index = 0; index < layout.columns.length; index++) {
            if (layout.columns[index].equalsIgnoreCase(column)) {
                return index;
            }
        }
        return -1;
    }

    /**
     * The conditions of a clause, each tied to the position of its column in one table, with every
     * choice between alternatives bound the same way, one conjunction at a time.
     */
    static final class Bound {
        private final Condition[] conditions;
        private final int[] columns;
        private final Bound[][] alternatives;
        private final int rolledBackColumn;

        private Bound(Condition[] conditions, int[] columns, Bound[][] alternatives, int rolledBackColumn) {
            this.conditions = conditions;
            this.columns = columns;
            this.alternatives = alternatives;
            this.rolledBackColumn = rolledBackColumn;
        }

        /**
         * @return true if a condition tests the rolled back state, which can be changed after a row
         *         was packed and so is only known for certain once the changes are looked up
         */
        boolean testsRolledBack() {
            for (int index = 0; index < columns.length; index++) {
                if (columns[index] == rolledBackColumn && rolledBackColumn >= 0) {
                    return true;
                }
                if (alternatives[index] != null) {
                    for (Bound alternative : alternatives[index]) {
                        if (alternative.testsRolledBack()) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        /**
         * Builds the part of the clause the decoder can test while it reads, so rows the query
         * would reject are never built.
         *
         * <p>
         * A row's rolled back state can be overridden after it was packed, and the decoder only
         * sees the packed value, so that condition is left to {@link #evaluate} unless no row of
         * the lookup has been overridden. Leaving a condition out of one alternative only makes
         * that alternative accept more, so the filter never rejects a row the query keeps.
         * </p>
         *
         * @param includeRolledBack
         *            whether the packed rolled back state is known to be current for every row
         * @return the filter, or null when there is nothing for it to test
         */
        ColdSegmentCodec.RowFilter decoderFilter(boolean includeRolledBack) {
            Test test = test(includeRolledBack);
            if (test == null) {
                return null;
            }
            return new ColdSegmentCodec.RowFilter() {
                @Override
                public boolean accept(long[] values, boolean[] present) {
                    return test.accept(values, present);
                }

                @Override
                public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                    return test.mayMatch(minimum, maximum, known);
                }
            };
        }

        /**
         * @return the conditions the decoder tests, or null when none are left to test
         */
        private Test test(boolean includeRolledBack) {
            List<Condition> tested = new ArrayList<>();
            List<Integer> positions = new ArrayList<>();
            List<Test[]> choices = new ArrayList<>();
            for (int index = 0; index < conditions.length; index++) {
                if (alternatives[index] != null) {
                    Test[] options = new Test[alternatives[index].length];
                    boolean open = false;
                    for (int alternative = 0; alternative < options.length; alternative++) {
                        options[alternative] = alternatives[index][alternative].test(includeRolledBack);
                        // An alternative with nothing left to test accepts every row, and so does
                        // the choice it belongs to.
                        open = open || options[alternative] == null;
                    }
                    if (!open) {
                        tested.add(conditions[index]);
                        positions.add(-1);
                        choices.add(options);
                    }
                    continue;
                }
                if (columns[index] == rolledBackColumn && !includeRolledBack) {
                    continue;
                }
                tested.add(conditions[index]);
                positions.add(columns[index]);
                choices.add(null);
            }
            if (tested.isEmpty()) {
                return null;
            }
            int[] testedColumns = new int[positions.size()];
            for (int index = 0; index < testedColumns.length; index++) {
                testedColumns[index] = positions.get(index);
            }
            return new Test(tested.toArray(new Condition[0]), testedColumns, choices.toArray(new Test[0][]));
        }

        /**
         * Builds a test that accepts exactly the rows the query keeps, rolled back state included.
         *
         * <p>
         * The decoder sees each row's rolled back state as it was packed. Where a rollback or a
         * restore has been recorded since, the recorded state is put in its place before the row is
         * tested, which is what the query sees too, since the same state is written into the
         * temporary table. Groups of rows are only ruled out by the conditions the recorded states
         * cannot change.
         * </p>
         *
         * @param overlay
         *            the rolled back state recorded after packing, by row id
         * @return the filter, or null when there is nothing for it to test
         */
        ColdSegmentCodec.RowFilter exactFilter(java.util.Map<Long, Integer> overlay) {
            ColdSegmentCodec.RowFilter full = decoderFilter(true);
            if (full == null || overlay == null || overlay.isEmpty() || !testsRolledBack()) {
                return full;
            }
            ColdSegmentCodec.RowFilter packed = decoderFilter(false);
            int column = rolledBackColumn;
            return new ColdSegmentCodec.RowFilter() {
                @Override
                public boolean accept(long[] values, boolean[] present) {
                    return full.accept(values, present);
                }

                @Override
                public boolean acceptRow(long rowId, long[] values, boolean[] present) {
                    Integer recorded = overlay.get(rowId);
                    if (recorded == null) {
                        return full.accept(values, present);
                    }
                    long packedValue = values[column];
                    boolean packedPresent = present[column];
                    values[column] = recorded;
                    present[column] = true;
                    try {
                        return full.accept(values, present);
                    }
                    finally {
                        values[column] = packedValue;
                        present[column] = packedPresent;
                    }
                }

                @Override
                public boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
                    return packed == null || packed.mayMatch(minimum, maximum, known);
                }
            };
        }

        /**
         * Tests a decoded row against every condition, the way the query would.
         *
         * @param values
         *            the row's values, in table column order
         * @param overlayFlag
         *            the rolled back state recorded after the row was packed, or null
         * @return {@link #MATCH}, {@link #NO_MATCH}, or {@link #UNKNOWN} when a value is of a kind
         *         only the query can compare
         */
        int evaluate(Object[] values, Integer overlayFlag) {
            boolean undecided = false;
            for (int index = 0; index < conditions.length; index++) {
                int verdict;
                if (alternatives[index] != null) {
                    verdict = NO_MATCH;
                    for (Bound alternative : alternatives[index]) {
                        int option = alternative.evaluate(values, overlayFlag);
                        if (option == MATCH) {
                            verdict = MATCH;
                            break;
                        }
                        if (option == UNKNOWN) {
                            verdict = UNKNOWN;
                        }
                    }
                }
                else {
                    int column = columns[index];
                    Object value = column == rolledBackColumn && overlayFlag != null ? Long.valueOf(overlayFlag.longValue()) : values[column];
                    verdict = conditions[index].operator == HAS_TEXT ? hasText(value) : compare(conditions[index], value);
                }
                if (verdict == NO_MATCH) {
                    return NO_MATCH;
                }
                // Any other condition that fails still rules the row out, so keep testing.
                undecided = undecided || verdict == UNKNOWN;
            }
            return undecided ? UNKNOWN : MATCH;
        }

        private static int compare(Condition condition, Object value) {
            if (value == null) {
                return NO_MATCH;
            }
            if (!(value instanceof Long)) {
                return UNKNOWN;
            }
            return condition.holds((Long) value) ? MATCH : NO_MATCH;
        }

        /**
         * Tests a value the way {@code LENGTH(value) > 0} does. SQLite measures text up to its first
         * NUL character, so text that begins with one is left for the query to judge.
         */
        private static int hasText(Object value) {
            if (value == null) {
                return NO_MATCH;
            }
            if (value instanceof String) {
                String text = (String) value;
                return text.isEmpty() ? NO_MATCH : text.charAt(0) == '\0' ? UNKNOWN : MATCH;
            }
            if (value instanceof byte[]) {
                return ((byte[]) value).length > 0 ? MATCH : NO_MATCH;
            }
            // A number always has digits when written as text.
            return MATCH;
        }
    }

    /** The conditions the decoder tests, with each choice between conjunctions as a list of them. */
    private static final class Test {
        private final Condition[] conditions;
        private final int[] columns;
        private final Test[][] choices;

        Test(Condition[] conditions, int[] columns, Test[][] choices) {
            this.conditions = conditions;
            this.columns = columns;
            this.choices = choices;
        }

        boolean accept(long[] values, boolean[] present) {
            for (int index = 0; index < conditions.length; index++) {
                if (choices[index] != null) {
                    boolean any = false;
                    for (Test choice : choices[index]) {
                        if (choice.accept(values, present)) {
                            any = true;
                            break;
                        }
                    }
                    if (!any) {
                        return false;
                    }
                    continue;
                }
                int column = columns[index];
                // A missing value is NULL, and no comparison holds for NULL. A text or blob value
                // is seen by its length in bytes, which is above zero exactly when SQLite's length
                // is, except for text beginning with a NUL character, which SQLite measures as
                // empty. The server removes control characters from the text players write, so
                // logged sign text does not begin with one.
                if (!present[column]) {
                    return false;
                }
                boolean holds = conditions[index].operator == HAS_TEXT ? values[column] > 0 : conditions[index].holds(values[column]);
                if (!holds) {
                    return false;
                }
            }
            return true;
        }

        boolean mayMatch(long[] minimum, long[] maximum, boolean[] known) {
            for (int index = 0; index < conditions.length; index++) {
                if (choices[index] != null) {
                    boolean any = false;
                    for (Test choice : choices[index]) {
                        if (choice.mayMatch(minimum, maximum, known)) {
                            any = true;
                            break;
                        }
                    }
                    if (!any) {
                        return false;
                    }
                    continue;
                }
                int column = columns[index];
                // The range of a group is only kept for whole numbers, so a test for text never
                // rules a group out.
                if (conditions[index].operator != HAS_TEXT && known[column] && !conditions[index].mayHold(minimum[column], maximum[column])) {
                    return false;
                }
            }
            return true;
        }
    }
}
