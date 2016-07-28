package org.github.silverfish.client;

@FunctionalInterface
public interface QueueElementCreator<I, E, M, QE extends QueueElement<I, E, M>> {

    QE create(I id, E element, M metadata);
}
