package io.github.danielbunting.clickhouse.protocol;

import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Byte-vector decoding tests for {@link ServerPacketReader}.
 */
class ServerPacketReaderTest {

    @Test
    void decodesExceptionPacketAndThrows() {
        byte[] wire = Bytes.capture(w -> {
            try {
                w.writeVarUInt(ServerPacket.EXCEPTION.code);
                w.writeInt32(241);                 // code
                w.writeString("DB::Exception");    // name
                w.writeString("Memory limit exceeded"); // message
                w.writeString("0. stack frame");   // stack trace
                w.writeUInt8(0);                    // has-nested = false
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ServerPacketReader reader = new ServerPacketReader(Bytes.reader(wire));
        ServerException ex = assertThrows(ServerException.class, reader::read);
        assertEquals(241, ex.code());
        assertEquals("DB::Exception", ex.serverExceptionName());
        assertEquals("0. stack frame", ex.serverStackTrace());
        assertTrue(ex.getMessage().contains("Memory limit exceeded"));
    }

    @Test
    void readExceptionStaticDecodesBodyAndConsumesNestedChain() throws IOException {
        byte[] body = Bytes.capture(w -> {
            try {
                w.writeInt32(10);
                w.writeString("Outer");
                w.writeString("outer msg");
                w.writeString("outer stack");
                w.writeUInt8(1);            // has-nested = true
                w.writeInt32(20);           // nested
                w.writeString("Inner");
                w.writeString("inner msg");
                w.writeString("inner stack");
                w.writeUInt8(0);            // no further nesting
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ServerException ex = ServerPacketReader.readException(Bytes.reader(body));
        assertEquals(10, ex.code());
        assertEquals("Outer", ex.serverExceptionName());
        assertEquals("outer stack", ex.serverStackTrace());
    }

    @Test
    void decodesProgressPacket() throws IOException {
        byte[] wire = Bytes.capture(w -> {
            try {
                w.writeVarUInt(ServerPacket.PROGRESS.code);
                w.writeVarUInt(1000);   // rows
                w.writeVarUInt(64000);  // bytes
                w.writeVarUInt(5000);   // total rows
                w.writeVarUInt(7);      // written rows
                w.writeVarUInt(700);    // written bytes
                w.writeVarUInt(123456); // elapsed nanos
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ServerPacketReader reader = new ServerPacketReader(Bytes.reader(wire));
        ServerPacketReader.Packet packet = reader.read();
        assertSame(ServerPacket.PROGRESS, packet.type());
        Progress p = (Progress) packet.body();
        assertEquals(1000L, p.rows());
        assertEquals(64000L, p.bytes());
        assertEquals(5000L, p.totalRows());
        assertEquals(7L, p.writtenRows());
        assertEquals(700L, p.writtenBytes());
        assertEquals(123456L, p.elapsedNanos());
    }

    @Test
    void decodesProfileInfoPacket() throws IOException {
        byte[] wire = Bytes.capture(w -> {
            try {
                w.writeVarUInt(ServerPacket.PROFILE_INFO.code);
                w.writeVarUInt(42);   // rows
                w.writeVarUInt(2);    // blocks
                w.writeVarUInt(2048); // bytes
                w.writeUInt8(1);      // applied limit
                w.writeVarUInt(100);  // rows before limit
                w.writeUInt8(0);      // calculated rows before limit
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ServerPacketReader reader = new ServerPacketReader(Bytes.reader(wire));
        ServerPacketReader.Packet packet = reader.read();
        assertSame(ServerPacket.PROFILE_INFO, packet.type());
        ProfileInfo info = (ProfileInfo) packet.body();
        assertEquals(42L, info.rows());
        assertEquals(2L, info.blocks());
        assertEquals(2048L, info.bytes());
        assertTrue(info.appliedLimit());
        assertEquals(100L, info.rowsBeforeLimit());
        assertFalse(info.calculatedRowsBeforeLimit());
    }

    @Test
    void decodesBodylessPackets() throws IOException {
        byte[] pong = Bytes.capture(w -> {
            try {
                w.writeVarUInt(ServerPacket.PONG.code);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        ServerPacketReader.Packet p = new ServerPacketReader(Bytes.reader(pong)).read();
        assertSame(ServerPacket.PONG, p.type());
        assertNull(p.body());

        byte[] eos = Bytes.capture(w -> {
            try {
                w.writeVarUInt(ServerPacket.END_OF_STREAM.code);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        ServerPacketReader.Packet e = new ServerPacketReader(Bytes.reader(eos)).read();
        assertSame(ServerPacket.END_OF_STREAM, e.type());
        assertNull(e.body());
    }

    @Test
    void dataPacketReportsTypeWithoutDecodingBody() throws IOException {
        byte[] wire = Bytes.capture(w -> {
            try {
                w.writeVarUInt(ServerPacket.DATA.code);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        ServerPacketReader.Packet p = new ServerPacketReader(Bytes.reader(wire)).read();
        assertSame(ServerPacket.DATA, p.type());
        assertNull(p.body());
    }
}
