package org.github.silverfish.client.wrappers;

import org.github.silverfish.client.QueueElement;

@FunctionalInterface
public interface QueueElementCreator<I, E, M, QE extends QueueElement<I, E, M>> {

    QE create(I id, E element, M metadata);
}
