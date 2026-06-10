package io.github.danielbunting.clickhouse.types.codec

/**
 * Codec for ClickHouse `Int128`: signed 16-byte little-endian
 * two's-complement integer. Range `-2^127 .. 2^127 - 1`.
 *
 * @constructor Public no-arg constructor required by the codec contract.
 */
public class Int128Codec : WideIntCodec("Int128", 16, false)
