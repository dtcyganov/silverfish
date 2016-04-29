package org.github.silverfish.client;

public class QueueElement<I, E, M> {

    final I id;
    final E element;
    final M metadata;

    public QueueElement(I id, E element, M metadata) {
        this.id = id;
        this.element = element;
        this.metadata = metadata;
    }

    public M getMetadata() {
        return metadata;
    }

    public I getId() {
        return id;
    }

    public E getElement() {
        return element;
    }

    @Override
    public String toString() {
        return "QueueElement{" +
                "id=" + id +
                ", element=" + element +
                ", metadata=" + metadata +
                '}';
    }
}
