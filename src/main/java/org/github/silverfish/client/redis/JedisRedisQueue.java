package org.github.silverfish.client.redis;

import org.github.silverfish.client.Queue;
import redis.clients.jedis.Jedis;

public class JedisRedisQueue<E> implements Queue<E> {

    //TODO: remove
    private void test() throws Exception {
        // some interaction example

        try (Jedis jedis = new Jedis("localhost")) {
            jedis.set("foo", "bar");
            jedis.get("foo");
        }
    }

    @Override
    public void enqueue(E e) {

    }

    @Override
    public E dequeue() {
        return null;
    }
}
