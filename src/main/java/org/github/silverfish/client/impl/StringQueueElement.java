package org.github.silverfish.client.impl;

import org.github.silverfish.client.QueueElement;

public class StringQueueElement extends QueueElement<String, String, Metadata> {

    public StringQueueElement(String id, String element, Metadata metadata) {
        super(id, element, metadata);
    }
}
