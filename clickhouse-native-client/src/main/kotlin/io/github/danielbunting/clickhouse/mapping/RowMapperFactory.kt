package io.github.danielbunting.clickhouse.mapping

/**
 * Builds a [RowMapper] once the bulk-insert target's column names are known.
 *
 * The standard [BulkInserter][io.github.danielbunting.clickhouse.BulkInserter] path derives its
 * mapper from a row class via [RowMappers.forClass]. Callers whose source is not a POJO — e.g.
 * an Arrow `VectorSchemaRoot`, where each "row" is just an index into column vectors — supply a
 * factory instead: it receives the target table's column names (in sample-block order) and
 * returns a mapper that scatters a row into a positional `Object[]` aligned with those names.
 *
 * @param T the row type fed to [BulkInserter.add][io.github.danielbunting.clickhouse.BulkInserter.add]
 */
public fun interface RowMapperFactory<T> {

    /**
     * @param targetColumnNames the insert target's column names, in the order the server's
     *                          sample block reports them (i.e. the order [RowMapper.bind] must
     *                          fill the destination array)
     */
    public fun create(targetColumnNames: Array<String>): RowMapper<T>
}
