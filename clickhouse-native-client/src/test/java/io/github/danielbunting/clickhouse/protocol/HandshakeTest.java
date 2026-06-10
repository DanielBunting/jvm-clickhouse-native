package io.github.danielbunting.clickhouse.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ProtocolException;
import io.github.danielbunting.clickhouse.testutil.Bytes;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.jupiter.api.Test;

/**
 * Round-trip and byte-vector tests for the {@link Handshake} Hello exchange.
 * No running server is required.
 */
class HandshakeTest {

    private static byte[] varUInt(long v) {
        return Bytes.capture(w -> {
            try {
                w.writeVarUInt(v);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static byte[] str(String s) {
        return Bytes.capture(w -> {
            try {
                w.writeString(s);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static byte[] concat(byte[]... parts) {
        int len = 0;
        for (byte[] p : parts) {
            len += p.length;
        }
        byte[] out = new byte[len];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }

    @Test
    void sendHelloEmitsExpectedByteVector() {
        ClickHouseConfig config = ClickHouseConfig.builder()
                .database("mydb")
                .username("alice")
                .password("secret")
                .build();

        byte[] actual = Bytes.capture(w -> {
            try {
                Handshake.sendHello(w, config);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        byte[] expected = concat(
                varUInt(ClientPacket.HELLO.code),
                str(Handshake.CLIENT_NAME),
                varUInt(Handshake.CLIENT_VERSION_MAJOR),
                varUInt(Handshake.CLIENT_VERSION_MINOR),
                varUInt(Handshake.CLIENT_PROTOCOL_REVISION),
                str("mydb"),
                str("alice"),
                str("secret"));

        assertArrayEquals(expected, actual);
    }

    @Test
    void sendHelloEmitsTokenInPasswordSlotWithMarkerUser() {
        // Access-token auth: the username is replaced with the sentinel marker and
        // the token is carried in the password slot.
        ClickHouseConfig config = ClickHouseConfig.builder()
                .database("mydb")
                .username("ignored-when-token-set")
                .accessToken("jwt.header.payload")
                .build();

        byte[] actual = Bytes.capture(w -> {
            try {
                Handshake.sendHello(w, config);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        byte[] expected = concat(
                varUInt(ClientPacket.HELLO.code),
                str(Handshake.CLIENT_NAME),
                varUInt(Handshake.CLIENT_VERSION_MAJOR),
                varUInt(Handshake.CLIENT_VERSION_MINOR),
                varUInt(Handshake.CLIENT_PROTOCOL_REVISION),
                str("mydb"),
                str(Handshake.ACCESS_TOKEN_USER_MARKER),
                str("jwt.header.payload"));

        assertArrayEquals(expected, actual);
    }

    @Test
    void sendHelloCertEmitsUsernameWithEmptyPassword() {
        ClickHouseConfig config = ClickHouseConfig.builder()
                .database("mydb")
                .username("certuser")
                .clientCertPath("/c.crt")
                .clientKeyPath("/c.key")
                .build();

        byte[] actual = Bytes.capture(w -> {
            try {
                Handshake.sendHello(w, config);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        byte[] expected = concat(
                varUInt(ClientPacket.HELLO.code),
                str(Handshake.CLIENT_NAME),
                varUInt(Handshake.CLIENT_VERSION_MAJOR),
                varUInt(Handshake.CLIENT_VERSION_MINOR),
                varUInt(Handshake.CLIENT_PROTOCOL_REVISION),
                str("mydb"),
                str("certuser"),
                str(""));

        assertArrayEquals(expected, actual);
    }

    @Test
    void readHelloFullModernRevision() {
        // Revision high enough to include timezone, display name and version patch.
        long revision = Handshake.MIN_REVISION_WITH_VERSION_PATCH + 10;

        byte[] wire = concat(
                varUInt(ServerPacket.HELLO.code),
                str("ClickHouse"),
                varUInt(24),
                varUInt(3),
                varUInt(revision),
                str("UTC"),
                str("display-host"),
                varUInt(7));

        ServerHello hello = readHelloUnchecked(wire);

        assertEquals("ClickHouse", hello.name());
        assertEquals(24, hello.versionMajor());
        assertEquals(3, hello.versionMinor());
        assertEquals(revision, hello.protocolRevision());
        // Revision-dependent fields:
        assertEquals("UTC", hello.timezone());
        assertEquals("display-host", hello.displayName());
        assertEquals(7, hello.versionPatch());
    }

    @Test
    void readHelloOldRevisionOmitsOptionalFields() {
        // Below every optional-field gate: only name + 3 versions present.
        long revision = Handshake.MIN_REVISION_WITH_SERVER_TIMEZONE - 1;

        byte[] wire = concat(
                varUInt(ServerPacket.HELLO.code),
                str("OldServer"),
                varUInt(20),
                varUInt(1),
                varUInt(revision));

        ServerHello hello = readHelloUnchecked(wire);

        assertEquals("OldServer", hello.name());
        assertEquals(revision, hello.protocolRevision());
        // Revision-dependent fields default when absent:
        assertEquals("", hello.timezone());
        assertEquals("", hello.displayName());
        assertEquals(0, hello.versionPatch());
    }

    @Test
    void readHelloTimezoneOnlyRevision() {
        // Includes timezone but not display name or version patch.
        long revision = Handshake.MIN_REVISION_WITH_SERVER_DISPLAY_NAME - 1;

        byte[] wire = concat(
                varUInt(ServerPacket.HELLO.code),
                str("MidServer"),
                varUInt(22),
                varUInt(8),
                varUInt(revision),
                str("Europe/London"));

        ServerHello hello = readHelloUnchecked(wire);

        assertEquals("Europe/London", hello.timezone());
        assertEquals("", hello.displayName());
        assertEquals(0, hello.versionPatch());
    }

    @Test
    void roundTripThroughBytes() {
        ClickHouseConfig config = ClickHouseConfig.builder()
                .database("analytics")
                .username("svc")
                .password("pw")
                .build();

        byte[] clientWire = Bytes.capture(w -> {
            try {
                Handshake.sendHello(w, config);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        // Re-read the client Hello fields manually to confirm the written layout.
        BinaryReader r = Bytes.reader(clientWire);
        try {
            assertEquals(ClientPacket.HELLO.code, (int) r.readVarUInt());
            assertEquals(Handshake.CLIENT_NAME, r.readString());
            assertEquals(Handshake.CLIENT_VERSION_MAJOR, r.readVarUInt());
            assertEquals(Handshake.CLIENT_VERSION_MINOR, r.readVarUInt());
            assertEquals(Handshake.CLIENT_PROTOCOL_REVISION, r.readVarUInt());
            assertEquals("analytics", r.readString());
            assertEquals("svc", r.readString());
            assertEquals("pw", r.readString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void readHelloRejectsWrongPacketCode() {
        // A non-HELLO, non-EXCEPTION code (PONG) is unexpected during the handshake
        // and must be rejected with a ProtocolException. (An EXCEPTION code is now
        // decoded into a ServerException instead — see readHelloDecodesException.)
        byte[] wire = concat(
                varUInt(ServerPacket.PONG.code),
                str("ignored"));

        assertThrows(ProtocolException.class, () -> readHelloUnchecked(wire));
    }

    private static ServerHello readHelloUnchecked(byte[] wire) {
        try {
            return Handshake.readHello(Bytes.reader(wire));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
