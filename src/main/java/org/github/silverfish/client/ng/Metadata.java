package org.github.silverfish.client.ng;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Metadata {

    private final Map<String, String> content;

    public Metadata(Map<String, String> content) {
        this.content = new HashMap<>(content);
    }

    public String getProperty(String name) {
        return content.get(name);
    }

    public long getPropertyAsLong(String name, long defaultValue) {
        String value = content.get(name);
        return name == null ? defaultValue : Long.parseLong(value);
    }

    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(content);
    }
}
