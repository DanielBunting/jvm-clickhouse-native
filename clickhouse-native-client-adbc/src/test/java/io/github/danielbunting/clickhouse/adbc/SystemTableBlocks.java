package io.github.danielbunting.clickhouse.adbc;

import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.QueryResults;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.codec.UInt64Codec;
import java.util.List;

/**
 * Canned {@link QueryResults} shaped like the {@code system.databases} / {@code system.tables} /
 * {@code system.columns} result sets that {@link GetObjectsBuilder} and
 * {@link ChAdbcConnection#getTableSchema} issue — so metadata logic is unit-testable offline by
 * pairing these with {@code ScriptedConnection.respondTo("system.databases", …)} etc.
 */
final class SystemTableBlocks {

    private SystemTableBlocks() {}

    /** Result of {@code SELECT name FROM system.databases …}. */
    static QueryResults.Scripted databases(String... names) {
        Block block = TestBlocks.blockOf(TestBlocks.stringColumn("name", names, null));
        return QueryResults.of(List.of("name"), List.of("String"), names.length == 0 ? List.of() : List.of(block));
    }

    /** Result of {@code SELECT database, name, engine FROM system.tables …}; rows of (database, name, engine). */
    static QueryResults.Scripted tables(String[]... rows) {
        String[] databases = new String[rows.length];
        String[] names = new String[rows.length];
        String[] engines = new String[rows.length];
        for (int i = 0; i < rows.length; i++) {
            databases[i] = rows[i][0];
            names[i] = rows[i][1];
            engines[i] = rows[i][2];
        }
        Block block = TestBlocks.blockOf(
                TestBlocks.stringColumn("database", databases, null),
                TestBlocks.stringColumn("name", names, null),
                TestBlocks.stringColumn("engine", engines, null));
        return QueryResults.of(
                List.of("database", "name", "engine"),
                List.of("String", "String", "String"),
                rows.length == 0 ? List.of() : List.of(block));
    }

    /**
     * Result of {@code SELECT database, table, name, type, position FROM system.columns …};
     * rows of (database, table, name, type, position).
     */
    static QueryResults.Scripted columns(Object[]... rows) {
        String[] databases = new String[rows.length];
        String[] tables = new String[rows.length];
        String[] names = new String[rows.length];
        String[] types = new String[rows.length];
        Object[] positions = new Object[rows.length];
        for (int i = 0; i < rows.length; i++) {
            databases[i] = (String) rows[i][0];
            tables[i] = (String) rows[i][1];
            names[i] = (String) rows[i][2];
            types[i] = (String) rows[i][3];
            positions[i] = ((Number) rows[i][4]).longValue();
        }
        Block block = TestBlocks.blockOf(
                TestBlocks.stringColumn("database", databases, null),
                TestBlocks.stringColumn("table", tables, null),
                TestBlocks.stringColumn("name", names, null),
                TestBlocks.stringColumn("type", types, null),
                TestBlocks.column("position", "UInt64", new UInt64Codec(), positions));
        return QueryResults.of(
                List.of("database", "table", "name", "type", "position"),
                List.of("String", "String", "String", "String", "UInt64"),
                rows.length == 0 ? List.of() : List.of(block));
    }

    /**
     * Result of the {@code getTableSchema} probe ({@code SELECT name, type FROM system.columns …});
     * rows of (name, type).
     */
    static QueryResults.Scripted schemaColumns(String[]... nameTypeRows) {
        String[] names = new String[nameTypeRows.length];
        String[] types = new String[nameTypeRows.length];
        for (int i = 0; i < nameTypeRows.length; i++) {
            names[i] = nameTypeRows[i][0];
            types[i] = nameTypeRows[i][1];
        }
        Block block = TestBlocks.blockOf(
                TestBlocks.stringColumn("name", names, null),
                TestBlocks.stringColumn("type", types, null));
        return QueryResults.of(
                List.of("name", "type"),
                List.of("String", "String"),
                nameTypeRows.length == 0 ? List.of() : List.of(block));
    }

    /** Result of {@code SELECT version()}. */
    static QueryResults.Scripted version(String version) {
        Block block = TestBlocks.blockOf(TestBlocks.stringColumn("version()", new String[] {version}, null));
        return QueryResults.of(List.of("version()"), List.of("String"), List.of(block));
    }

    /** Reusable reference to a column for callers that need one-off shapes. */
    static Column stringColumn(String name, String... values) {
        return TestBlocks.stringColumn(name, values, null);
    }
}
