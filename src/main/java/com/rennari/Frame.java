package com.rennari;

import java.util.LinkedHashMap;
import java.util.Map;

public class Frame {
    public String event;
    public Object data;
    public Long id;
    public Long offset;
    public String namespace;

    public Frame() {}

    public Frame(String event, Object data) {
        this.event = event;
        this.data = data;
    }

    public Frame withId(long id) {
        this.id = id;
        return this;
    }

    public Frame withOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public Frame withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("e", event);
        m.put("d", data);
        if (id != null) m.put("id", id);
        if (offset != null) m.put("o", offset);
        if (namespace != null && !namespace.isEmpty() && !namespace.equals("/")) {
            m.put("n", namespace);
        }
        return m;
    }
}
