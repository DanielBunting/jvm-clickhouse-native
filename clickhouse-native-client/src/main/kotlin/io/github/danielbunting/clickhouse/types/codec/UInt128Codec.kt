package io.github.danielbunting.clickhouse.types.codec

/**
 * Codec for ClickHouse `UInt128`: unsigned 16-byte little-endian integer.
 * Range `0 .. 2^128 - 1`.
 *
 * @constructor Public no-arg constructor required by the codec contract.
 */
public class UInt128Codec : WideIntCodec("UInt128", 16, true)
