package com.rennari;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class Client {
    private static final Logger log = LoggerFactory.getLogger(Client.class);

    public interface Handler {
        void handle(Object payload);
    }

    public static class Options {
        public Protocol.Serializer serializer = Protocol.Serializer.MSGPACK;
        public int maxBytes = Protocol.DEFAULT_MAX_BYTES;
        public long ackTimeoutMs = 5000;
        public int retries = 3;
        public Map<String, String> extraHeaders = new HashMap<>();
    }

    private final String url;
    private final Options options;
    private final Map<String, Map<String, Handler>> handlers = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private long nextId = 0;

    private volatile String sessionId;
    private volatile long lastOffset = -1;

    private InnerClient ws;

    public Client(String url, Options options) {
        this.url = url;
        this.options = options;
        handlers.put("/", new ConcurrentHashMap<>());
    }

    public Namespace of(String name) {
        return new Namespace(name);
    }

    public Client on(String event, Handler handler) {
        of("/").on(event, handler);
        return this;
    }

    public class Namespace {
        private final String name;

        Namespace(String name) {
            this.name = name;
            handlers.computeIfAbsent(name, k -> new ConcurrentHashMap<>());
        }

        public Namespace on(String event, Handler handler) {
            handlers.get(name).put(event, handler);
            return this;
        }

        public void emit(String event, Object payload) {
            try {
                Client.this.emit(name, event, payload, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Object emitWithAck(String event, Object payload) throws Exception {
            return Client.this.emitWithAck(name, event, payload);
        }
    }

    public void connect() throws Exception {
        Map<String, String> headers = new HashMap<>(options.extraHeaders);
        ws = new InnerClient(URI.create(url), headers);
        if (!ws.connectBlocking(10, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new IllegalStateException("connect failed");
        }
        Thread.sleep(50);

        for (String nsp : handlers.keySet()) {
            if (nsp.equals("/")) continue;
            Frame frame = new Frame("rennari_nsp", Map.of("name", nsp));
            ws.send(Protocol.encode(frame, options.serializer));
        }
    }

    public void disconnect() {
        if (ws != null) {
            ws.close();
            ws = null;
        }
    }

    public String sessionId() {
        return sessionId;
    }

    public long lastOffset() {
        return lastOffset;
    }

    private void emit(String nsp, String event, Object payload, Long id) throws Exception {
        if (ws == null) throw new IllegalStateException("not connected");
        Frame frame = new Frame(event, payload);
        if (id != null) frame.withId(id);
        if (!nsp.equals("/")) frame.withNamespace(nsp);
        ws.send(Protocol.encode(frame, options.serializer));
    }

    private Object emitWithAck(String nsp, String event, Object payload) throws Exception {
        int attempts = 0;
        while (true) {
            attempts++;
            long id;
            synchronized (lock) {
                id = ++nextId;
            }
            CompletableFuture<Object> future = new CompletableFuture<>();
            pending.put(id, future);

            emit(nsp, event, payload, id);

            try {
                return future.get(options.ackTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException te) {
                pending.remove(id);
                if (attempts > options.retries) {
                    throw new RuntimeException("ack timeout for '" + event + "'");
                }
            }
        }
    }

    private void dispatch(Frame frame) {
        if (frame.offset != null && frame.offset > lastOffset) {
            lastOffset = frame.offset;
        }

        switch (frame.event) {
            case "rennari_session" -> {
                if (frame.data instanceof Map<?, ?> m && m.get("id") instanceof String s) {
                    sessionId = s;
                }
                return;
            }
            case "rennari_nsp_ok" -> {
                return;
            }
            case "rennari_resumed" -> {
                if (frame.data instanceof Map<?, ?> m && m.get("session_id") instanceof String s) {
                    sessionId = s;
                }
                return;
            }
            case "rennari_resume_failed" -> {
                sessionId = null;
                lastOffset = -1;
                return;
            }
            case "rennari_ack" -> {
                if (frame.data instanceof Map<?, ?> m && m.get("id") instanceof Number n) {
                    CompletableFuture<Object> f = pending.remove(n.longValue());
                    if (f != null) f.complete(frame.data);
                }
                return;
            }
        }

        String nsp = (frame.namespace == null || frame.namespace.isEmpty())
                ? "/" : frame.namespace;

        Map<String, Handler> map = handlers.get(nsp);
        if (map != null) {
            Handler h = map.get(frame.event);
            if (h != null) {
                try {
                    h.handle(frame.data);
                } catch (Exception e) {
                    log.error("handler {} failed", frame.event, e);
                }
            }
        }
    }

    private class InnerClient extends WebSocketClient {
        InnerClient(URI uri, Map<String, String> headers) {
            super(uri, headers);
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
                Frame frame = Protocol.decode(bytes, options.serializer, options.maxBytes);
                dispatch(frame);
            } catch (Exception e) {
                log.warn("decode failed: {}", e.getMessage());
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {}

        @Override
        public void onError(Exception e) {
            log.warn("client ws error: {}", e.getMessage());
        }
    }
}
