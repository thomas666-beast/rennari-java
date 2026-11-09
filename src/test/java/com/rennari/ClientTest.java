package com.rennari;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ClientTest {

    @Test
    void echoRoundTrip() throws Exception {
        Server.Options opts = new Server.Options();
        opts.mock = true;

        Server server = new Server(opts)
                .on("echo", (ctx, p) -> {
                    ctx.sendToSelf("echo_back", p);
                    return null;
                });

        int port = 20000 + (int) (Math.random() * 1000);
        var ws = server.build(new InetSocketAddress("127.0.0.1", port));
        ws.start();
        Thread.sleep(200);

        Client c = new Client("ws://127.0.0.1:" + port + "/ws", new Client.Options());
        CountDownLatch latch = new CountDownLatch(1);
        Object[] received = new Object[1];
        c.on("echo_back", p -> {
            received[0] = p;
            latch.countDown();
        });

        c.connect();
        c.of("/").emit("echo", Map.of("text", "hi"));

        assertTrue(latch.await(2, TimeUnit.SECONDS), "no echo_back");
        assertTrue(received[0] instanceof Map);
        assertEquals("hi", ((Map<?, ?>) received[0]).get("text"));

        c.disconnect();
        ws.stop(500);
    }

    @Test
    void ackRoundTrip() throws Exception {
        Server.Options opts = new Server.Options();
        opts.mock = true;

        Server server = new Server(opts)
                .on("ping", (ctx, p) -> null);

        int port = 21000 + (int) (Math.random() * 1000);
        var ws = server.build(new InetSocketAddress("127.0.0.1", port));
        ws.start();
        Thread.sleep(200);

        Client c = new Client("ws://127.0.0.1:" + port + "/ws", new Client.Options());
        c.connect();

        Object ack = c.of("/").emitWithAck("ping", Map.of("n", 1));
        assertTrue(ack instanceof Map);
        assertNotNull(((Map<?, ?>) ack).get("id"));

        c.disconnect();
        ws.stop(500);
    }

    @Test
    void sessionIdPresent() throws Exception {
        Server.Options opts = new Server.Options();
        opts.mock = true;
        opts.recovery = true;

        Server server = new Server(opts);
        int port = 22000 + (int) (Math.random() * 1000);
        var ws = server.build(new InetSocketAddress("127.0.0.1", port));
        ws.start();
        Thread.sleep(200);

        Client c = new Client("ws://127.0.0.1:" + port + "/ws", new Client.Options());
        c.connect();
        Thread.sleep(200);
        assertNotNull(c.sessionId());

        c.disconnect();
        ws.stop(500);
    }
}
