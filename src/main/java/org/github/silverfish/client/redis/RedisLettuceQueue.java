package org.github.silverfish.client.redis;

import org.github.silverfish.client.Queue;

public class RedisLettuceQueue<E> implements Queue<E> {

    @Override
    public void enqueue(E e) {

    }

    @Override
    public E dequeue() {
        return null;
    }
}
