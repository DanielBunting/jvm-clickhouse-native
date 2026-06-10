package io.github.danielbunting.clickhouse.types.codec

/**
 * Codec for ClickHouse `UInt256`: unsigned 32-byte little-endian integer.
 * Range `0 .. 2^256 - 1`.
 *
 * @constructor Public no-arg constructor required by the codec contract.
 */
public class UInt256Codec : WideIntCodec("UInt256", 32, true)
