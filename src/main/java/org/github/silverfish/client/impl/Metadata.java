package org.github.silverfish.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.github.silverfish.client.util.Util.bytesToString;
import static org.github.silverfish.client.util.Util.getBytes;

public class Metadata {

    private final Map<String, String> content;

    public Metadata(Map<String, String> content) {
        this.content = content;
    }

    public Map<String, String> toMap() {
        return content;
    }

    public Map<byte[], byte[]> toBytesMap() {
        Map<byte[], byte[]> result = new HashMap<>();
        content.forEach((k, v) -> result.put(getBytes(k), getBytes(v)));
        return result;
    }

    @Override
    public String toString() {
        return "{" + new TreeMap<>(content) + "}";
    }

    public static Map<String, String> toStringMap(Map<byte[], byte[]> m) {
        Map<String, String> result = new HashMap<>();
        m.forEach((k, v) -> result.put(bytesToString(k), bytesToString(v)));
        return result;
    }
}
