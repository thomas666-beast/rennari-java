package com.rennari.examples;

import com.rennari.Server;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        Map<String, Map<String, Object>> tokens = new HashMap<>();
        tokens.put("alice-token", Map.of("id", "alice", "userId", 1, "name", "alice"));
        tokens.put("bob-token", Map.of("id", "bob", "userId", 2, "name", "bob"));

        Server.Options opts = new Server.Options();
        opts.recovery = true;
        opts.authCb = handshake -> {
            String path = handshake.getResourceDescriptor();
            String token = null;
            int i = path.indexOf("token=");
            if (i >= 0) {
                token = path.substring(i + 6);
                int amp = token.indexOf('&');
                if (amp >= 0) token = token.substring(0, amp);
            }
            return token != null ? tokens.get(token) : null;
        };

        Server server = new Server(opts);

        server.onConnect((ctx, identity) -> {
            System.out.println("[server] connect conn=" + ctx.connId + " identity=" + identity);
            return null;
        });

        server.onDisconnect((ctx, identity) -> {
            System.out.println("[server] disconnect conn=" + ctx.connId + " identity=" + identity);
            return null;
        });

        server.on("chat", (ctx, p) -> {
            Map<?, ?> m = (Map<?, ?>) p;
            String room = (String) m.get("room");
            ctx.join(room);
            ctx.broadcastToRoom(room, "chat", Map.of(
                    "room", room,
                    "from", ctx.connId,
                    "text", m.get("text") == null ? "" : m.get("text")
            ));
            return null;
        });

        server.on("ping", (ctx, p) -> {
            ctx.sendToSelf("pong", Map.of("got", p));
            return null;
        });

        server.on("whoami", (ctx, p) -> Map.of("userId", 1, "name", "alice"));

        server.namespace("/chat");
        server.addHandler("/chat", "hello", (ctx, p) -> {
            ctx.sendToSelf("hello_back", Map.of("from", "/chat", "got", p));
            return null;
        });

        String port = System.getenv().getOrDefault("PORT", "3000");
        var ws = server.build(new InetSocketAddress("127.0.0.1", Integer.parseInt(port)));
        ws.start();
        System.out.println("[server] listening on ws://127.0.0.1:" + port + "/ws");
        Thread.currentThread().join();
    }
}
