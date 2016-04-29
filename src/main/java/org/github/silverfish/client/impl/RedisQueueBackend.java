package org.github.silverfish.client.impl;

import com.google.common.collect.Lists;
import org.github.silverfish.client.Backend;
import org.github.silverfish.client.QueueElement;
import org.github.silverfish.client.ItemState;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;

public class RedisQueueBackend implements Backend<String, String, Map<String, String>, RedisQueueElement> {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 2_000;
    private static final int DEFAULT_SO_TIMEOUT = 1_000;
    private static final int DEFAULT_CLAIM_WAIT_TIMEOUT = 30_000;

    private static final String UNPROCESSED_QUEUE = "unprocessed";
    private static final String WORKING_QUEUE = "working";
    private static final String PROCESSED_QUEUE = "processed";
    private static final String FAILED_QUEUE = "failed";

    private static final EnumMap<ItemState, String> SUBQUEUES_NAME_MAPPING = new EnumMap<>(ItemState.class);

    static {
        SUBQUEUES_NAME_MAPPING.put(ItemState.MAIN, UNPROCESSED_QUEUE);
        SUBQUEUES_NAME_MAPPING.put(ItemState.BUSY, WORKING_QUEUE);
        SUBQUEUES_NAME_MAPPING.put(ItemState.DONE, PROCESSED_QUEUE);
        SUBQUEUES_NAME_MAPPING.put(ItemState.FAILED, FAILED_QUEUE);
    }

    private final String host;
    private final int port;
    private final int connectionTimeout;
    private final int soTimeout;
    private final int claimWaitTimeout;

    private final String queueName;

    private final Supplier<String> idSupplier;
    private final Supplier<Map<String, String>> metadataSupplier;

    public RedisQueueBackend(Supplier<String> idSupplier,
                             Supplier<Map<String, String>> metadataSupplier,
                             String host, int port, String queueName) {

        this(idSupplier, metadataSupplier,
                host, port, queueName,
                DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SO_TIMEOUT, DEFAULT_CLAIM_WAIT_TIMEOUT);
    }

    public RedisQueueBackend(Supplier<String> idSupplier,
                             Supplier<Map<String, String>> metadataSupplier,
                             String host, int port, String queueName,
                             int connectionTimeout, int soTimeout, int claimWaitTimeout) {

        this.idSupplier = idSupplier;
        this.metadataSupplier = metadataSupplier;
        this.host = host;
        this.port = port;
        this.queueName = queueName;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.claimWaitTimeout = claimWaitTimeout;
    }

    @Override
    public List<RedisQueueElement> enqueue(List<String> elements) {
        if (elements.stream().anyMatch(e -> e == null)) {
            throw new NullPointerException("One of elements is null: " + elements.toString());
        }

        List<RedisQueueElement> resultList = new ArrayList<>(elements.size());
        try (Jedis rh = getJedis()) {
            for (String e : elements) {
                String id = idSupplier.get();
                String key = queueName + "-" + id;
                long result = rh.setnx("item-" + key, e);
                if (result == 0) {
                    throw new IllegalStateException("'" + key + "' already exists in queue '" + queueName + "'");
                }
                Map<String, String> metadata = metadataSupplier.get();
                rh.hmset("meta-" + key, metadata);
                rh.lpush(getUnprocessedQueueName(), key);

                resultList.add(new RedisQueueElement(id, e, metadata));
            }
        }
        return resultList;
    }

    @Override
    public List<RedisQueueElement> dequeue(long count, boolean blocking) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count should be greater than 0, value : " + count);
        }
        try (Jedis rh = getJedis()) {
            if (count == 1) {
                if (!blocking) {
                    String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                    if (itemKey == null) {
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(getElement(rh, itemKey));
                } else {
                    String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                    if (itemKey == null) {
                        itemKey = rh.brpoplpush(getUnprocessedQueueName(), getWorkingQueueName(), claimWaitTimeout);
                    }
                    if (itemKey == null) {
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(getElement(rh, itemKey));
                }
            } else {
                // When fetching multiple items:
                // - Non-blocking mode: We try to fetch one item, and then give up.
                // - Blocking mode: We attempt to fetch the first item using brpoplpush, and if it succeeds,
                //                    we switch to rpoplpush for greater throughput.

                // Yes, there is a race, but it's an optimization only.
                // This means that after we know how many items we have...
                long llen = rh.llen(getUnprocessedQueueName());
                // ... we only take those. But the point is that, between these two comments (err, maybe
                // the code statements are more important) there might have been an enqueue_items(), so
                // that we are actually grabbing less than what the user asked for. But we are OK with
                // that, since the new items are too fresh anyway (they might even be too hot for that
                // user's tongue).
                if (count > llen) {
                    count = llen;
                }

                List<RedisQueueElement> items = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                    if (itemKey != null) {
                        items.add(getElement(rh, itemKey));
                    }
                }

                if (items.size() == 0 && blocking) {
                    String firstItemKey = rh.brpoplpush(getUnprocessedQueueName(), getWorkingQueueName(), claimWaitTimeout);

                    if (firstItemKey != null) {
                        items.add(getElement(rh, firstItemKey));

                        for (int i = 1; i < count; i++) {
                            String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                            if (itemKey != null) {
                                items.add(getElement(rh, itemKey));
                            }
                        }
                    }
                }

                return items;
            }
        }
    }

    private RedisQueueElement getElement(Jedis rh, String itemKey) {
        rh.hincrBy("meta-" + itemKey, "process_count", 1);
        String element  = rh.get("item-" + itemKey);
        Map<String, String> metadata = rh.hgetAll("meta-" + itemKey);
        String id = itemKey.substring(queueName.length() + 1);
        return new RedisQueueElement(id, element, metadata);
    }

    @Override
    public long markProcessed(List<String> ids) {

        try (Jedis jedis = getJedis()) {

            List<String> flushed = new ArrayList<>();
            List<String> failed = new ArrayList<>();

            // The callback receives the result of LREM() for removing the item from the working queue and
            // populates % result. If LREM() succeeds, we need to clean up the payload and the metadata.
            for (String itemKey : ids) {
                int lrem_direction = 1;
                // Head - to - tail, though it should not make any difference...since
                // the item is supposed to be unique anyway.
                // See http://redis.io/commands/lrem for details.
                long count = jedis.lrem(getWorkingQueueName(), lrem_direction, itemKey);
                (count > 0 ? flushed : failed).add(itemKey);
            }

            for (List<String> part : Lists.partition(flushed, 100)) {
                String[] metaKeysChunk = part.stream().map(s -> "meta-" + s).toArray(String[]::new);
                String[] dataKeysChunk = part.stream().map(s -> "item-" + s).toArray(String[]::new);
                long count1 = jedis.del(metaKeysChunk);
                long count2 = jedis.del(dataKeysChunk);
                assert count1 + count2 == part.size() * 2;
            }

            assert failed.size() == 0;
            return flushed.size();// (flushed, failed)
        }
    }

    @Override
    public long markFailed(List<String> ids) {
        return 0;
    }

    @Override
    public List<RedisQueueElement> peek(long limit) {
        return null;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public List<RedisQueueElement> collectGarbage() {
        return null;
    }

    @Override
    public void flush() {
        try (Jedis jedis = getJedis()) {
            jedis.flushAll();
        }
    }

    @Override
    public long length() {
        try (Jedis jedis = getJedis()) {
            return jedis.llen(SUBQUEUES_NAME_MAPPING.get(ItemState.MAIN));
        }
    }

    @Override
    public Map<String, Long> stats() {
        try (Jedis jedis = getJedis()) {
            return SUBQUEUES_NAME_MAPPING.entrySet().stream().
                    collect(toMap(e -> e.getKey().toString(), e -> jedis.llen(e.getValue())));
        }
    }

    private Jedis getJedis() {
        return new Jedis(host, port, connectionTimeout, soTimeout);
    }

    private String getUnprocessedQueueName() {
        return queueName + "_" + UNPROCESSED_QUEUE;
    }

    private String getWorkingQueueName() {
        return queueName + "_" + WORKING_QUEUE;
    }

    private String getProcessedQueueName() {
        return queueName + "_" + PROCESSED_QUEUE;
    }

    private String getFailedQueueName() {
        return queueName + "_" + FAILED_QUEUE;
    }
}
