package com.rennari;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ServerTest {

    private static class TestClient extends WebSocketClient {
        final LinkedBlockingQueue<Frame> frames = new LinkedBlockingQueue<>();

        TestClient(URI uri) {
            super(uri);
        }

        @Override
        public void onOpen(ServerHandshake h) {}

        @Override
        public void onMessage(String m) {}

        @Override
        public void onMessage(ByteBuffer buf) {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            try {
                frames.add(Protocol.decode(bytes, Protocol.Serializer.MSGPACK, 65536));
            } catch (Exception ignored) {}
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {}

        @Override
        public void onError(Exception e) {}

        Frame await(String event, long ms) throws InterruptedException {
            long deadline = System.currentTimeMillis() + ms;
            while (System.currentTimeMillis() < deadline) {
                Frame f = frames.poll(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if (f != null && f.event.equals(event)) return f;
            }
            return null;
        }
    }

    @Test
    void echoRoundTrip() throws Exception {
        Server.Options opts = new Server.Options();
        opts.mock = true;

        Server server = new Server(opts)
                .on("echo", (ctx, p) -> {
                    ctx.sendToSelf("echo_back", p);
                    return null;
                });

        int port = 18000 + (int) (Math.random() * 1000);
        var ws = server.build(new InetSocketAddress("127.0.0.1", port));
        ws.start();
        Thread.sleep(200);

        TestClient c = new TestClient(URI.create("ws://127.0.0.1:" + port + "/ws"));
        c.connectBlocking();
        Thread.sleep(100);

        Frame f = new Frame("echo", Map.of("text", "hi"));
        c.send(Protocol.encode(f, Protocol.Serializer.MSGPACK));

        Frame back = c.await("echo_back", 2000);
        assertNotNull(back, "no echo_back");

        c.close();
        ws.stop(500);
    }

    @Test
    void ackRoundTrip() throws Exception {
        Server.Options opts = new Server.Options();
        opts.mock = true;

        Server server = new Server(opts)
                .on("ping", (ctx, p) -> null);

        int port = 19000 + (int) (Math.random() * 1000);
        var ws = server.build(new InetSocketAddress("127.0.0.1", port));
        ws.start();
        Thread.sleep(200);

        TestClient c = new TestClient(URI.create("ws://127.0.0.1:" + port + "/ws"));
        c.connectBlocking();
        Thread.sleep(100);

        Frame f = new Frame("ping", Map.of("n", 1)).withId(7);
        c.send(Protocol.encode(f, Protocol.Serializer.MSGPACK));

        Frame ack = c.await("rennari_ack", 2000);
        assertNotNull(ack, "no ack");
        assertEquals(7L, ack.data instanceof Map<?, ?> m ? ((Number) m.get("id")).longValue() : -1);

        c.close();
        ws.stop(500);
    }
}
