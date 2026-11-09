package com.rennari;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ProtocolTest {

    private static byte[] fromHex(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    @Test
    void decodePerlSmile() throws Exception {
        Frame f = Protocol.decode(
                fromHex("82a165a5736d696c65a16481a16e01"),
                Protocol.Serializer.MSGPACK, 65536);
        assertEquals("smile", f.event);
    }

    @Test
    void decodePerlSmileId() throws Exception {
        Frame f = Protocol.decode(
                fromHex("83a16481a16e01a269642aa165a5736d696c65"),
                Protocol.Serializer.MSGPACK, 65536);
        assertEquals("smile", f.event);
        assertEquals(42L, f.id);
    }

    @Test
    void decodePerlSmileOffset() throws Exception {
        Frame f = Protocol.decode(
                fromHex("83a165a5736d696c65a16f07a16481a16e01"),
                Protocol.Serializer.MSGPACK, 65536);
        assertEquals("smile", f.event);
        assertEquals(7L, f.offset);
    }

    @Test
    void decodePerlChat() throws Exception {
        Frame f = Protocol.decode(
                fromHex("82a16481a474657874a568656c6c6fa165a463686174"),
                Protocol.Serializer.MSGPACK, 65536);
        assertEquals("chat", f.event);
    }

    @Test
    void roundTrip() throws Exception {
        Frame f = new Frame("chat", java.util.Map.of("text", "hi"))
                .withId(42).withOffset(7).withNamespace("/chat");
        byte[] bytes = Protocol.encode(f, Protocol.Serializer.MSGPACK);
        Frame g = Protocol.decode(bytes, Protocol.Serializer.MSGPACK, 65536);
        assertEquals("chat", g.event);
        assertEquals(42L, g.id);
        assertEquals(7L, g.offset);
        assertEquals("/chat", g.namespace);
    }

    @Test
    void rejectsGarbage() {
        assertThrows(Exception.class, () ->
                Protocol.decode(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff},
                        Protocol.Serializer.MSGPACK, 65536));
    }

    @Test
    void validEventName() {
        assertTrue(Protocol.validEventName("smile"));
        assertTrue(Protocol.validEventName("chat.message"));
        assertFalse(Protocol.validEventName("bad\n"));
        assertFalse(Protocol.validEventName(""));
    }
}
