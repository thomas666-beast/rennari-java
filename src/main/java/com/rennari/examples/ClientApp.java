package com.rennari.examples;

import com.rennari.Client;

import java.util.Map;

public class ClientApp {
    public static void main(String[] args) throws Exception {
        String port = args.length > 0 ? args[0] : "3000";
        String url = "ws://127.0.0.1:" + port + "/ws?token=alice-token";

        Client.Options opts = new Client.Options();
        opts.ackTimeoutMs = 2000;
        opts.retries = 1;

        Client c = new Client(url, opts);

        c.on("chat", p -> System.out.println("[client] chat " + p));
        c.on("pong", p -> System.out.println("[client] pong " + p));
        c.on("reply", p -> System.out.println("[client] reply " + p));

        Client.Namespace chat = c.of("/chat");
        chat.on("hello_back", p -> System.out.println("[client] hello_back " + p));

        c.connect();
        System.out.println("[client] connected");

        Thread.sleep(200);
        System.out.println("[client] session " + c.sessionId());

        c.of("/").emit("chat", Map.of("room", "lobby", "text", "hello from java"));
        chat.emit("hello", Map.of("n", 1));

        Object ack = c.of("/").emitWithAck("ping", Map.of("n", 42));
        System.out.println("[client] ack " + ack);

        Thread.sleep(300);
        c.disconnect();
    }
}
