package org.github.silverfish.client.ng;

import com.google.common.collect.Lists;
import org.github.silverfish.client.impl.ByteArrayQueueElement;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.stream.Collectors.toList;
import static org.github.silverfish.client.ng.Util.bytesToString;
import static org.github.silverfish.client.ng.Util.getBytes;
import static org.github.silverfish.client.ng.ValidityUtils.*;

public class RedisQueueOperations {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 2_000;
    private static final int DEFAULT_SO_TIMEOUT = 1_000;
    private static final int DELETE_CHUNK_SIZE = 100;

    private final String host;
    private final int port;
    private final int connectionTimeout;
    private final int soTimeout;

    private final String domain;

    private ThreadLocal<Integer> jedisCallsThreadLocal = ThreadLocal.withInitial(() -> 0);
    private ThreadLocal<Jedis> jedisThreadLocal = new ThreadLocal<>();

    public RedisQueueOperations(String host, int port, String domain) {

        this(host, port, domain, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SO_TIMEOUT);
    }

    public RedisQueueOperations(String host, int port, String domain,
                                int connectionTimeout, int soTimeout) {

        this.host = host;
        this.port = port;
        this.domain = domain;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
    }

    public void register(List<ByteArrayQueueElement> elements) {
        assureNotNull(elements);
        if (elements.isEmpty()) {
            return;
        }

        try (Jedis rh = getJedis()) {
            for (ByteArrayQueueElement e : elements) {
                String id = e.getId();
                String key = idToKey(id);
                Long result = rh.setnx(getBytes(itemKey(key)), e.getElement());
                if (result == null || result == 0) {
                    throw new IllegalStateException(String.format("Id '%s' already exists", id));
                }
                rh.hmset(getBytes(metaKey(key)), e.getMetadata().toBytesMap());
            }
        }
    }

    public void unregister(List<String> ids) {
        assureNotNull(ids);
        if (ids.isEmpty()) {
            return;
        }

        try (Jedis rh = getJedis()) {
            for (List<String> part : Lists.partition(idsToKeys(ids), DELETE_CHUNK_SIZE)) {
                byte[][] metaKeysChunk = part.stream().map(this::metaKey).map(Util::getBytes).toArray(byte[][]::new);
                byte[][] dataKeysChunk = part.stream().map(this::itemKey).map(Util::getBytes).toArray(byte[][]::new);
                rh.del(metaKeysChunk);
                rh.del(dataKeysChunk);
            }
        }
    }

    public void enqueue(String queueName, List<String> ids) {
        assureNotNull(queueName);
        assureNotNull(ids);
        if (ids.isEmpty()) {
            return;
        }

        try (Jedis rh = getJedis()) {
            for (String id : ids) {
                String key = idToKey(id);
                Long result = rh.lpush(getBytes(innerQueueName(queueName)), getBytes(key));
                if (result == null || result == 0) {
                    throw new RuntimeException(String.format(
                            "Failed to lpush() item_key: '%s' onto the queue '%s'",
                            key, innerQueueName(queueName)));
                }
            }
        }
    }

    public List<String> requeue(String sourceQueueName, String destQueueName, long count) {
        return requeue(sourceQueueName, destQueueName, count, false, 0);
    }

    public List<String> requeue(String sourceQueueName, String destQueueName, long count,
                                boolean blocking, long timeout) {

        assureNotNull(sourceQueueName);
        assureNotNull(destQueueName);
        assurePositive(count);
        assureNonNegative(timeout);

        try (Jedis rh = getJedis()) {
            List<String> ids = new ArrayList<>();
            long start = System.currentTimeMillis();
            int moved;
            for (moved = 0; moved < count; moved++) {
                byte[] key = rh.rpoplpush(
                        getBytes(innerQueueName(sourceQueueName)),
                        getBytes(innerQueueName(destQueueName)));
                if (key != null) {
                    ids.add(keyToId(bytesToString(key)));
                } else {
                    break;
                }
            }

            if (blocking) {
                while (moved < count) {
                    int operationTimeout = (int) (timeout - (System.currentTimeMillis() - start));
                    if (operationTimeout < 0) {
                        break;
                    }
                    byte[] key = rh.brpoplpush(
                            getBytes(innerQueueName(sourceQueueName)),
                            getBytes(innerQueueName(destQueueName)),
                            operationTimeout);
                    if (key == null) {
                        break;
                    }
                    ids.add(keyToId(bytesToString(key)));
                    moved++;
                }
            }
            return ids;
        }
    }

    public List<String> dequeue(String queueName, List<String> ids) {
        assureNotNull(queueName);
        assureNotNull(ids);
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }

        try (Jedis rh = getJedis()) {
            return ids.stream().filter(id -> {
                Long result = rh.lrem(getBytes(innerQueueName(queueName)), 1, getBytes(idToKey(id)));
                return result != null && result > 0;
            }).collect(toList());
        }
    }

    public List<String> dequeue(String queueName, long count) {
        return dequeue(queueName, count, false, 0);
    }

    public List<String> dequeue(String queueName,
                                long count, boolean blocking, long timeout) {

        assureNotNull(queueName);
        assurePositive(count);
        assureNonNegative(timeout);

        try (Jedis rh = getJedis()) {
            List<String> ids = new ArrayList<>();
            long start = System.currentTimeMillis();
            int moved;
            for (moved = 0; moved < count; moved++) {
                byte[] key = rh.rpop(getBytes(innerQueueName(queueName)));
                if (key != null) {
                    ids.add(keyToId(bytesToString(key)));
                } else {
                    break;
                }
            }

            if (blocking) {
                while (moved < count) {
                    int operationTimeout = (int) (timeout - (System.currentTimeMillis() - start));
                    if (operationTimeout < 0) {
                        break;
                    }
                    List<byte[]> key = rh.brpop(operationTimeout, getBytes(innerQueueName(queueName)));
                    if (key == null || key.isEmpty()) {
                        break;
                    }
                    ids.add(keyToId(bytesToString(key.get(0))));
                    moved++;
                }
            }
            return ids;
        }
    }

    public long length(String queueName) {
        try (Jedis jedis = getJedis()) {
            return jedis.llen(innerQueueName(queueName));
        }
    }

    public List<String> peek(String queueName, long numberOfItems) {
        try (Jedis rh = getJedis()) {
            return rh.lrange(getBytes(innerQueueName(queueName)), -numberOfItems, -1).stream().
                    map(Util::bytesToString).map(this::keyToId).collect(toList());
        }
    }

    public List<String> peekAll(String queueName) {
        return peek(queueName, Long.MAX_VALUE);
    }

    public Long deleteQueue(String queueName) {
        try (Jedis rh = getJedis()) {
            return rh.del(getBytes(innerQueueName(queueName)));
        }
    }

    public long incrementMetadataCounter(String id, String field, long value) {
        try (Jedis rh = getJedis()) {
            return rh.hincrBy(getBytes(metaKey(idToKey(id))), getBytes(field), value);
        }
    }

    public long setMetadataCounter(String id, String field, long value) {
        try (Jedis rh = getJedis()) {
            return rh.hset(getBytes(metaKey(idToKey(id))), getBytes(field), getBytes(String.valueOf(value)));
        }
    }

    @SuppressWarnings("unused")
    public void doInOneConnection(Runnable r) {
        try (Jedis rh = getJedis()) {
            r.run();
        }
    }

    @SuppressWarnings("unused")
    public <R> R doInOneConnection(Callable<R> r) {
        try (Jedis rh = getJedis()) {
            try {
                return r.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public byte[] getPayloadById(String id) {
        try (Jedis rh = getJedis()) {
            byte[] payload = rh.get(getBytes(itemKey(idToKey(id))));
            if (payload == null) {
                throw new IllegalStateException(String.format(
                        "Found item_key: '%s' but not its key! This should never happen!", id));
            }
            return payload;
        }
    }

    public Metadata getMetadataById(String id) {
        try (Jedis rh = getJedis()) {
            Map<byte[], byte[]> metadata = rh.hgetAll(getBytes(metaKey(idToKey(id))));
            if (metadata == null) {
                throw new IllegalStateException(String.format(
                        "Found item_key: '%s' but not its metadata! This should never happen!", id));
            }
            return new Metadata(Metadata.toStringMap(metadata));
        }
    }

    public ByteArrayQueueElement getItemById(String id) {
        return new ByteArrayQueueElement(
                id,
                getPayloadById(id),
                getMetadataById(id)
        );
    }

    private Jedis getJedis() {
        // we are reusing the same jedis instance in nested calls
        jedisCallsThreadLocal.set(jedisCallsThreadLocal.get() + 1);
        if (jedisThreadLocal.get() == null) {
            jedisThreadLocal.set(new Jedis(host, port, connectionTimeout, soTimeout) {
                
                public void close() {
                    jedisCallsThreadLocal.set(jedisCallsThreadLocal.get() - 1);
                    if (jedisCallsThreadLocal.get() == 0) {
                        jedisThreadLocal.set(null);
                        super.close();
                    }
                }
            });
        }
        return jedisThreadLocal.get();
    }

    private String idToKey(String id) {
        return domain + "-" + id;
    }

    private String keyToId(String key) {
        return key.substring(domain.length() + 1);
    }

    private List<String> idsToKeys(List<String> ids) {
        return ids.stream().map(this::idToKey).collect(toList());
    }

    private String itemKey(String key) {
        return "item-" + key;
    }

    private String metaKey(String key) {
        return "meta-" + key;
    }

    private String innerQueueName(String queueName) {
        return domain + "_" + queueName;
    }

}
