package io.github.danielbunting.clickhouse.types.codec

/**
 * Codec for ClickHouse `Int256`: signed 32-byte little-endian
 * two's-complement integer. Range `-2^255 .. 2^255 - 1`.
 *
 * @constructor Public no-arg constructor required by the codec contract.
 */
public class Int256Codec : WideIntCodec("Int256", 32, false)
