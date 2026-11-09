package com.rennari;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    public interface AuthCb {
        Map<String, Object> authenticate(ClientHandshake handshake);
    }

    public interface Handler {
        Object handle(Context ctx, Object payload);
    }

    public static class Options {
        public AuthCb authCb;
        public Protocol.Serializer serializer = Protocol.Serializer.MSGPACK;
        public int maxBytes = Protocol.DEFAULT_MAX_BYTES;
        public boolean recovery = false;
        public long recoveryWindowSecs = 120;
        public boolean mock = false;
    }

    private static class Connection {
        WebSocket ws;
        Map<String, Object> identity;
        String sessionId;
        Set<String> namespaces = new HashSet<>(Set.of("/"));
        long offset = 0;
        boolean offline = false;
    }

    private static class Session {
        Map<String, Object> identity;
        Set<String> namespaces;
        Map<String, List<String>> rooms;
        long offset;
        List<Map.Entry<Long, byte[]>> buffered = new ArrayList<>();
        long connId;
        boolean offline = false;
        long generation = 0;
    }

    private static class Namespace {
        Map<String, Handler> handlers = new ConcurrentHashMap<>();
        Map<String, Set<Long>> rooms = new ConcurrentHashMap<>();
        Set<Long> members = ConcurrentHashMap.newKeySet();
    }

    private final Options options;
    private final Map<String, Namespace> namespaces = new ConcurrentHashMap<>();
    private final Map<Long, Connection> connections = new ConcurrentHashMap<>();
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();
    private long nextId = 0;

    private BiFunction<Context, Map<String, Object>, Void> onConnectCb;
    private BiFunction<Context, Map<String, Object>, Void> onDisconnectCb;

    private final Object lock = new Object();

    public Server(Options options) {
        this.options = options;
        namespaces.put("/", new Namespace());
    }

    public Server on(String event, Handler handler) {
        namespaces.get("/").handlers.put(event, handler);
        return this;
    }

    public Server namespace(String name) {
        namespaces.computeIfAbsent(name, k -> new Namespace());
        return this;
    }

    public Server addHandler(String nsp, String event, Handler handler) {
        namespaces.computeIfAbsent(nsp, k -> new Namespace()).handlers.put(event, handler);
        return this;
    }

    public Server onConnect(BiFunction<Context, Map<String, Object>, Void> cb) {
        this.onConnectCb = cb;
        return this;
    }

    public Server onDisconnect(BiFunction<Context, Map<String, Object>, Void> cb) {
        this.onDisconnectCb = cb;
        return this;
    }

    public WebSocketServer build(InetSocketAddress address) {
        return new WebSocketServer(address) {
            @Override
            public void onOpen(WebSocket ws, ClientHandshake handshake) {
                Map<String, Object> identity;

                if (options.mock) {
                    identity = new LinkedHashMap<>();
                    identity.put("id", "mock");
                    identity.put("mock", true);
                } else if (options.authCb != null) {
                    identity = options.authCb.authenticate(handshake);
                } else {
                    identity = null;
                }

                if (identity == null) {
                    ws.close(1008, "unauthorized");
                    return;
                }

                long connId;
                String sessionId;
                synchronized (lock) {
                    connId = ++nextId;
                    sessionId = newSessionId();
                    Connection c = new Connection();
                    c.ws = ws;
                    c.identity = identity;
                    c.sessionId = sessionId;
                    connections.put(connId, c);
                    ws.setAttachment(connId);
                    namespaces.get("/").members.add(connId);
                    if (options.recovery) {
                        Session s = new Session();
                        s.identity = identity;
                        s.namespaces = new HashSet<>(Set.of("/"));
                        s.rooms = new HashMap<>();
                        s.connId = connId;
                        sessions.put(sessionId, s);
                    }
                }

                if (options.recovery) {
                    sendToConnection(connId, "rennari_session",
                            Map.of("id", sessionId), "/");
                }

                if (onConnectCb != null) {
                    onConnectCb.apply(new Context(Server.this, connId, "/"), identity);
                }
            }

            @Override
            public void onClose(WebSocket ws, int code, String reason, boolean remote) {
                Long connId = ws.getAttachment();
                if (connId != null) {
                    handleClose(connId);
                }
            }

            @Override
            public void onMessage(WebSocket ws, String message) {
                // Only binary is expected; ignore text frames.
            }

            @Override
            public void onMessage(WebSocket ws, ByteBuffer message) {
                Long connId = ws.getAttachment();
                if (connId != null) {
                    byte[] bytes = new byte[message.remaining()];
                    message.get(bytes);
                    handleMessage(connId, bytes);
                }
            }

            @Override
            public void onError(WebSocket ws, Exception ex) {
                log.warn("ws error: {}", ex.getMessage());
            }

            @Override
            public void onStart() {
                log.info("listening on {}", address);
            }
        };
    }

    private String newSessionId() {
        byte[] b = new byte[16];
        random.nextBytes(b);
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    private void handleMessage(long connId, byte[] bytes) {
        Frame frame;
        try {
            frame = Protocol.decode(bytes, options.serializer, options.maxBytes);
        } catch (Exception e) {
            closeWith(connId, 4002);
            return;
        }

        if (frame.event.equals("rennari_resume")) {
            handleResume(connId, frame.data);
            return;
        }
        if (frame.event.equals("rennari_nsp")) {
            handleNamespaceJoin(connId, frame.data);
            return;
        }

        String nspName = (frame.namespace == null || frame.namespace.isEmpty())
                ? "/" : frame.namespace;

        Connection conn;
        Namespace nsp;
        Handler handler;
        synchronized (lock) {
            conn = connections.get(connId);
            if (conn == null || !conn.namespaces.contains(nspName)) {
                closeWith(connId, 4005);
                return;
            }
            nsp = namespaces.get(nspName);
            handler = nsp == null ? null : nsp.handlers.get(frame.event);
        }

        if (handler == null) {
            if (options.mock) return;
            closeWith(connId, 4003);
            return;
        }

        Object reply;
        try {
            reply = handler.handle(new Context(this, connId, nspName), frame.data);
        } catch (Exception e) {
            log.error("handler {} failed", frame.event, e);
            closeWith(connId, 4004);
            return;
        }

        if (reply != null) {
            sendToConnection(connId, "reply", reply, nspName);
        }
        if (frame.id != null) {
            sendToConnection(connId, "rennari_ack", Map.of("id", frame.id), "");
        }
    }

    private void handleNamespaceJoin(long connId, Object data) {
        String name = "/";
        if (data instanceof Map<?, ?> m && m.get("name") instanceof String s) {
            name = s;
        }
        synchronized (lock) {
            Namespace nsp = namespaces.get(name);
            if (nsp == null) {
                closeWith(connId, 4005);
                return;
            }
            Connection conn = connections.get(connId);
            if (conn != null) {
                conn.namespaces.add(name);
                nsp.members.add(connId);
            }
        }
        sendToConnection(connId, "rennari_nsp_ok", Map.of("name", name), "");
    }

    private void handleResume(long connId, Object data) {
        if (!options.recovery) {
            closeWith(connId, 4006);
            return;
        }

        String sid = "";
        long offset = -1;
        if (data instanceof Map<?, ?> m) {
            if (m.get("session_id") instanceof String s) sid = s;
            if (m.get("offset") instanceof Number n) offset = n.longValue();
        }

        synchronized (lock) {
            Session sess = sessions.get(sid);
            if (sess == null || !sess.offline) {
                sendToConnection(connId, "rennari_resume_failed",
                        Map.of("reason", "unknown_session"), "");
                return;
            }

            Connection conn = connections.get(connId);
            String oldSid = conn.sessionId;
            if (!oldSid.equals(sid)) sessions.remove(oldSid);
            if (sess.connId != connId) connections.remove(sess.connId);

            conn.sessionId = sid;
            conn.offset = sess.offset;
            conn.namespaces.addAll(sess.namespaces);
            for (String n : sess.namespaces) {
                Namespace nsp = namespaces.get(n);
                if (nsp != null) nsp.members.add(connId);
            }
            for (var entry : sess.rooms.entrySet()) {
                Namespace nsp = namespaces.get(entry.getKey());
                if (nsp == null) continue;
                for (String room : entry.getValue()) {
                    nsp.rooms.computeIfAbsent(room, k -> ConcurrentHashMap.newKeySet()).add(connId);
                }
            }

            for (var buf : sess.buffered) {
                if (buf.getKey() > offset) {
                    conn.ws.send(buf.getValue());
                }
            }
            sess.buffered.clear();
            sess.connId = connId;
            sess.offline = false;
            sess.generation++;

            sendToConnection(connId, "rennari_resumed",
                    Map.of("offset", sess.offset, "session_id", sid), "");
        }
    }

    private void handleClose(long connId) {
        Connection conn;
        synchronized (lock) {
            conn = connections.get(connId);
            if (conn == null) return;

            Map<String, List<String>> roomsByNsp = new HashMap<>();
            for (String n : conn.namespaces) {
                Namespace nsp = namespaces.get(n);
                if (nsp == null) continue;
                List<String> rooms = new ArrayList<>();
                for (var e : nsp.rooms.entrySet()) {
                    if (e.getValue().contains(connId)) rooms.add(e.getKey());
                }
                if (!rooms.isEmpty()) roomsByNsp.put(n, rooms);
                nsp.members.remove(connId);
                for (var e : nsp.rooms.entrySet()) {
                    e.getValue().remove(connId);
                }
                nsp.rooms.entrySet().removeIf(e -> e.getValue().isEmpty());
            }

            String sid = conn.sessionId;
            Session sess = options.recovery ? sessions.get(sid) : null;

            if (sess != null) {
                sess.namespaces = new HashSet<>(conn.namespaces);
                sess.rooms = roomsByNsp;
                sess.connId = connId;
                sess.offline = true;
                long gen = ++sess.generation;
                new Timer(true).schedule(new TimerTask() {
                    @Override public void run() {
                        synchronized (lock) {
                            Session cur = sessions.get(sid);
                            if (cur == null || !cur.offline || cur.generation != gen) return;
                            connections.remove(cur.connId);
                            sessions.remove(sid);
                        }
                    }
                }, options.recoveryWindowSecs * 1000);
            } else {
                connections.remove(connId);
            }
        }

        if (onDisconnectCb != null) {
            onDisconnectCb.apply(new Context(this, connId, "/"), conn.identity);
        }
    }

    private void closeWith(long connId, int code) {
        Connection conn = connections.get(connId);
        if (conn != null && conn.ws != null) {
            conn.ws.close(code, "");
        }
    }

    boolean sendToConnection(long connId, String event, Object payload, String nsp) {
        synchronized (lock) {
            Connection conn = connections.get(connId);
            if (conn == null) return false;

            if (!conn.offline) {
                Long offset = null;
                if (options.recovery) {
                    conn.offset++;
                    offset = conn.offset;
                    Session sess = sessions.get(conn.sessionId);
                    if (sess != null) sess.offset = offset;
                }
                Frame frame = new Frame(event, payload);
                if (offset != null) frame.withOffset(offset);
                frame.withNamespace(nsp);
                try {
                    byte[] bytes = Protocol.encode(frame, options.serializer);
                    conn.ws.send(bytes);
                    return true;
                } catch (Exception e) {
                    log.warn("send failed: {}", e.getMessage());
                    return false;
                }
            }

            if (!options.recovery) return false;
            Session sess = sessions.get(conn.sessionId);
            if (sess == null) return false;
            sess.offset++;
            Frame frame = new Frame(event, payload)
                    .withOffset(sess.offset).withNamespace(nsp);
            try {
                byte[] bytes = Protocol.encode(frame, options.serializer);
                sess.buffered.add(Map.entry(sess.offset, bytes));
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }

    Namespace namespaceFor(String name) {
        return namespaces.computeIfAbsent(name, k -> new Namespace());
    }

    Object lockObject() {
        return lock;
    }

    public static class Context {
        private final Server server;
        public final long connId;
        public final String namespace;

        Context(Server server, long connId, String namespace) {
            this.server = server;
            this.connId = connId;
            this.namespace = namespace;
        }

        public void join(String room) {
            synchronized (server.lock) {
                Namespace nsp = server.namespaceFor(namespace);
                if (nsp.members.contains(connId)) {
                    nsp.rooms.computeIfAbsent(room, k -> ConcurrentHashMap.newKeySet()).add(connId);
                }
            }
        }

        public void leave(String room) {
            synchronized (server.lock) {
                Namespace nsp = server.namespaceFor(namespace);
                Set<Long> members = nsp.rooms.get(room);
                if (members != null) {
                    members.remove(connId);
                    if (members.isEmpty()) nsp.rooms.remove(room);
                }
            }
        }

        public List<Long> membersOf(String room) {
            synchronized (server.lock) {
                Namespace nsp = server.namespaceFor(namespace);
                Set<Long> m = nsp.rooms.get(room);
                if (m == null) return List.of();
                List<Long> list = new ArrayList<>(m);
                Collections.sort(list);
                return list;
            }
        }

        public List<String> rooms() {
            synchronized (server.lock) {
                Namespace nsp = server.namespaceFor(namespace);
                List<String> out = new ArrayList<>();
                for (var e : nsp.rooms.entrySet()) {
                    if (e.getValue().contains(connId)) out.add(e.getKey());
                }
                Collections.sort(out);
                return out;
            }
        }

        public boolean sendToSelf(String event, Object payload) {
            return server.sendToConnection(connId, event, payload, namespace);
        }

        public boolean sendTo(long toId, String event, Object payload) {
            return server.sendToConnection(toId, event, payload, namespace);
        }

        public int broadcast(String event, Object payload) {
            List<Long> members;
            synchronized (server.lock) {
                Namespace nsp = server.namespaceFor(namespace);
                members = new ArrayList<>(nsp.members);
            }
            int n = 0;
            for (long id : members) {
                if (server.sendToConnection(id, event, payload, namespace)) n++;
            }
            return n;
        }

        public int broadcastToRoom(String room, String event, Object payload) {
            List<Long> members = membersOf(room);
            int n = 0;
            for (long id : members) {
                if (server.sendToConnection(id, event, payload, namespace)) n++;
            }
            return n;
        }
    }
}
