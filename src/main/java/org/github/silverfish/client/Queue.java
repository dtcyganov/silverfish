package org.github.silverfish.client;

public interface Queue<E> {

    void enqueue(E e);
    E dequeue();
}
