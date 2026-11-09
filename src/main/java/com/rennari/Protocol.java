package com.rennari;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

public final class Protocol {
    public static final int DEFAULT_MAX_BYTES = 64 * 1024;

    private static final Pattern EVENT_RE = Pattern.compile("^[A-Za-z0-9_.:-]{1,64}$");
    private static final Pattern NSP_RE = Pattern.compile("^/[A-Za-z0-9_.:/-]{0,63}$");
    private static final Pattern ID_RE = Pattern.compile("^[0-9]{1,20}$");
    private static final Pattern OFFSET_RE = Pattern.compile("^[0-9]+$");

    private static final ObjectMapper JSON = new ObjectMapper();

    private Protocol() {}

    public static boolean validEventName(String name) {
        return name != null && EVENT_RE.matcher(name).matches();
    }

    public static boolean validNamespaceName(String name) {
        return name != null && NSP_RE.matcher(name).matches();
    }

    public static boolean validId(long id) {
        return id >= 0 && ID_RE.matcher(String.valueOf(id)).matches();
    }

    public static boolean validOffset(long offset) {
        return offset >= 0 && OFFSET_RE.matcher(String.valueOf(offset)).matches();
    }

    public enum Serializer {
        MSGPACK, JSON
    }

    public static byte[] encode(Frame frame, Serializer serializer) throws IOException {
        if (!validEventName(frame.event)) {
            throw new IllegalArgumentException("invalid event name");
        }
        if (frame.id != null && !validId(frame.id)) {
            throw new IllegalArgumentException("invalid id");
        }
        if (frame.offset != null && !validOffset(frame.offset)) {
            throw new IllegalArgumentException("invalid offset");
        }
        if (frame.namespace != null
                && !frame.namespace.isEmpty()
                && !frame.namespace.equals("/")
                && !validNamespaceName(frame.namespace)) {
            throw new IllegalArgumentException("invalid namespace");
        }

        Map<String, Object> map = frame.toMap();

        if (serializer == Serializer.JSON) {
            return JSON.writeValueAsBytes(map);
        }

        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packMapHeader(map.size());
            for (Map.Entry<String, Object> e : map.entrySet()) {
                packer.packString(e.getKey());
                packValue(packer, e.getValue());
            }
        } finally {
            packer.close();
        }
        return packer.toByteArray();
    }

    private static void packValue(MessageBufferPacker packer, Object value) throws IOException {
        if (value == null) {
            packer.packNil();
        } else if (value instanceof String s) {
            packer.packString(s);
        } else if (value instanceof Integer i) {
            packer.packInt(i);
        } else if (value instanceof Long l) {
            packer.packLong(l);
        } else if (value instanceof Boolean b) {
            packer.packBoolean(b);
        } else if (value instanceof Map<?, ?> m) {
            packer.packMapHeader(m.size());
            for (Map.Entry<?, ?> e : m.entrySet()) {
                packer.packString(String.valueOf(e.getKey()));
                packValue(packer, e.getValue());
            }
        } else if (value instanceof java.util.List<?> list) {
            packer.packArrayHeader(list.size());
            for (Object o : list) {
                packValue(packer, o);
            }
        } else {
            packer.packString(value.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public static Frame decode(byte[] bytes, Serializer serializer, int maxBytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("empty input");
        }
        if (bytes.length > maxBytes) {
            throw new IllegalArgumentException("frame exceeds max_bytes");
        }

        Map<String, Object> map;

        if (serializer == Serializer.JSON) {
            map = JSON.readValue(bytes, Map.class);
        } else {
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
            try {
                int size = unpacker.unpackMapHeader();
                map = new java.util.LinkedHashMap<>();
                for (int i = 0; i < size; i++) {
                    String key = unpacker.unpackString();
                    Value v = unpacker.unpackValue();
                    map.put(key, toJava(v));
                }
            } finally {
                unpacker.close();
            }
        }

        Object e = map.get("e");
        if (!(e instanceof String event) || !validEventName(event)) {
            throw new IllegalArgumentException("invalid event name");
        }
        if (!map.containsKey("d")) {
            throw new IllegalArgumentException("missing d");
        }

        Frame frame = new Frame(event, map.get("d"));

        if (map.containsKey("id")) {
            long id = ((Number) map.get("id")).longValue();
            if (!validId(id)) throw new IllegalArgumentException("invalid id");
            frame.id = id;
        }
        if (map.containsKey("o")) {
            long o = ((Number) map.get("o")).longValue();
            if (!validOffset(o)) throw new IllegalArgumentException("invalid offset");
            frame.offset = o;
        }
        if (map.containsKey("n")) {
            String n = (String) map.get("n");
            if (!validNamespaceName(n)) throw new IllegalArgumentException("invalid namespace");
            frame.namespace = n;
        }

        return frame;
    }

    private static Object toJava(Value v) {
        if (v.isNil()) return null;
        if (v.isString()) return v.asStringValue().asString();
        if (v.isInteger()) return v.asIntegerValue().toLong();
        if (v.isBoolean()) return v.asBooleanValue().getBoolean();
        if (v.isMap()) {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            v.asMapValue().map().forEach((k, val) -> m.put(k.asStringValue().asString(), toJava(val)));
            return m;
        }
        if (v.isArray()) {
            java.util.List<Object> list = new java.util.ArrayList<>();
            v.asArrayValue().forEach(val -> list.add(toJava(val)));
            return list;
        }
        return v.toString();
    }
}
