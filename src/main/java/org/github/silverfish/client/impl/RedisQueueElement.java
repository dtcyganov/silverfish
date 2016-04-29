package org.github.silverfish.client.impl;

import org.github.silverfish.client.QueueElement;

import java.util.Map;

public class RedisQueueElement extends QueueElement<String, String, Map<String, String>> {

    public RedisQueueElement(String id, String element, Map<String, String> metadata) {
        super(id, element, metadata);
    }
}
