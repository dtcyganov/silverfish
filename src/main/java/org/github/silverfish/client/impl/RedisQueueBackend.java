package org.github.silverfish.client.impl;

import com.google.common.collect.Lists;
import org.github.silverfish.client.Backend;
import org.github.silverfish.client.CleanupAction;
import org.github.silverfish.client.ng.Metadata;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class RedisQueueBackend implements Backend<String, String, Metadata, StringQueueElement> {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 2_000;
    private static final int DEFAULT_SO_TIMEOUT = 1_000;
    private static final int DEFAULT_CLAIM_WAIT_TIMEOUT = 30_000;
    private static final int DEFAULT_REQUEUE_LIMIT = 5;

    private static final String UNPROCESSED_QUEUE = "unprocessed";
    private static final String WORKING_QUEUE = "working";
    private static final String FAILED_QUEUE = "failed";

    private final String host;
    private final int port;
    private final int connectionTimeout;
    private final int soTimeout;
    private final int claimWaitTimeout;
    private final int requeueLimit;

    private final String queueName;

    private final Supplier<String> idSupplier;
    private final Supplier<Metadata> metadataSupplier;

    private ThreadLocal<Integer> jedisCallsThreadLocal = ThreadLocal.withInitial(() -> 0);
    private ThreadLocal<Jedis> jedisThreadLocal = new ThreadLocal<>();

    public RedisQueueBackend(Supplier<String> idSupplier,
                             Supplier<Metadata> metadataSupplier,
                             String host, int port, String queueName) {

        this(idSupplier, metadataSupplier,
                host, port, queueName,
                DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SO_TIMEOUT, DEFAULT_CLAIM_WAIT_TIMEOUT,
                DEFAULT_REQUEUE_LIMIT);
    }

    public RedisQueueBackend(Supplier<String> idSupplier,
                             Supplier<Metadata> metadataSupplier,
                             String host, int port, String queueName,
                             int connectionTimeout, int soTimeout, int claimWaitTimeout,
                             int requeueLimit) {

        this.idSupplier = idSupplier;
        this.metadataSupplier = metadataSupplier;
        this.host = host;
        this.port = port;
        this.queueName = queueName;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.claimWaitTimeout = claimWaitTimeout;
        this.requeueLimit = requeueLimit;
    }

    @Override
    public List<StringQueueElement> enqueueNewElements(List<String> elements) {
        assureNotEmptyAndWithoutNulls(elements);

        try (Jedis rh = getJedis()) {
            List<StringQueueElement> resultList = new ArrayList<>(elements.size());
            for (String e : elements) {
                String id = idSupplier.get();
                String key = idToKey(id);
                Long result = rh.setnx(itemKey(key), e);
                if (result == null || result == 0) {
                    throw new IllegalStateException(String.format("'%s' already exists in queue '%s'", key, queueName));
                }
                Metadata metadata = metadataSupplier.get();
                rh.hmset(metaKey(key), metadata.toMap());
                Long lpushResult = rh.lpush(getUnprocessedQueueName(), key);
                if (lpushResult == null || lpushResult == 0) {
                    throw new RuntimeException(String.format(
                            "Failed to lpush() item_key: '%s' onto the unprocessed queue '%s'",
                            key, getUnprocessedQueueName()));
                }

                resultList.add(new StringQueueElement(id, e, metadata));
            }
            return resultList;
        }
    }

    @Override
    public List<StringQueueElement> enqueueNewElements(String... elements) throws Exception {
        return enqueueNewElements(Arrays.asList(elements));
    }

    @Override
    public List<StringQueueElement> dequeueForProcessing(long count, boolean blocking) {
        assurePositive(count);
        try (Jedis rh = getJedis()) {
            if (count == 1) {
                if (!blocking) {
                    String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                    if (itemKey == null) {
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(getElementAndUpdateProcessCount(itemKey));
                } else {
                    //TODO: why not just call brpoplpush?
                    String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                    if (itemKey == null) {
                        itemKey = rh.brpoplpush(getUnprocessedQueueName(), getWorkingQueueName(), claimWaitTimeout);
                    }
                    if (itemKey == null) {
                        return Collections.emptyList();
                    }
                    return Collections.singletonList(getElementAndUpdateProcessCount(itemKey));
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

                List<StringQueueElement> items = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                    if (itemKey != null) {
                        items.add(getElementAndUpdateProcessCount(itemKey));
                    }
                }

                if (items.size() == 0 && blocking) {
                    String firstItemKey = rh.brpoplpush(getUnprocessedQueueName(), getWorkingQueueName(), claimWaitTimeout);

                    if (firstItemKey != null) {
                        items.add(getElementAndUpdateProcessCount(firstItemKey));

                        for (int i = 1; i < count; i++) {
                            String itemKey = rh.rpoplpush(getUnprocessedQueueName(), getWorkingQueueName());
                            if (itemKey != null) {
                                items.add(getElementAndUpdateProcessCount(itemKey));
                            } else {
                                break;
                            }
                        }
                    }
                }

                return items;
            }
        }
    }

    private StringQueueElement getElementAndUpdateProcessCount(String key) {
        try (Jedis rh = getJedis()) {
            rh.hincrBy(metaKey(key), "process_count", 1);
            return getItemByKey(key);
        }
    }

    @Override
    public long markProcessed(List<String> ids) {
        assureNotEmptyAndWithoutNulls(ids);

        try (Jedis jedis = getJedis()) {
            List<String> flushed = new ArrayList<>();
            List<String> failed = new ArrayList<>();

            // The callback receives the result of LREM() for removing the item from the working queue and
            // populates % result. If LREM() succeeds, we need to clean up the payload and the metadata.
            for (String key : idsToKeys(ids)) {
                // Head - to - tail, though it should not make any difference...since
                // the item is supposed to be unique anyway.
                // See http://redis.io/commands/lrem for details.
                long count = jedis.lrem(getWorkingQueueName(), 1, key);
                (count > 0 ? flushed : failed).add(key);
            }

            for (List<String> part : Lists.partition(flushed, 100)) {
                String[] metaKeysChunk = part.stream().map(this::metaKey).toArray(String[]::new);
                String[] dataKeysChunk = part.stream().map(this::itemKey).toArray(String[]::new);
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
        return requeue(ids, getWorkingQueueName(), true, true);
    }

    @Override
    public List<StringQueueElement> peekUnprocessedElements(long limit) {
        return Lists.reverse(getRawItems(getUnprocessedQueueName(), limit));
    }

    // aka handle_expired_items
    @Override
    public List<StringQueueElement> cleanup(CleanupAction cleanupAction,
                                            Predicate<Metadata> condition) {
        if (cleanupAction == null) {
            throw new NullPointerException("CleanupAction is null");
        }

        try (Jedis rh = getJedis()) {
            // Either we call Redis to know the length of the working queue, and then call
            //   $rh->lrange($self->_working_queue, -$working_queue_length, -1);
            // which costs us two RPCs, or just reverse what you get from a single RPC...
            List<String> keys = rh.lrange(getWorkingQueueName(), 0, -1).stream().collect(toList());
            Map<String, Metadata> itemMetadata = keys.stream().collect(toMap(k -> k, this::getMetadataByKey));
            List<String> candidates = keys.stream().filter(k -> condition.test(itemMetadata.get(k))).collect(toList());
            Map<String, String> itemPayload = candidates.stream().collect(toMap(k -> k, this::getPayloadByKey));

            List<StringQueueElement> expiredItems = new ArrayList<>();

            for (String key : Lists.reverse(candidates)) {
                switch (cleanupAction) {
                    case REQUEUE:
                        if (requeue(Collections.singletonList(key), getWorkingQueueName(), true, true) > 0) {
                            expiredItems.add(new StringQueueElement(
                                    keyToId(key),
                                    itemPayload.get(key),
                                    itemMetadata.get(key)
                            ));
                        }
                        break;
                    case DROP:
                        Long result = rh.lrem(getWorkingQueueName(), -1, itemKey(key));
                        if (result != null && result != 0) {
                            expiredItems.add(new StringQueueElement(
                                    keyToId(key),
                                    itemPayload.get(key),
                                    itemMetadata.get(key)
                            ));
                        }
                        rh.del(itemKey(key));
                        rh.del(metaKey(key));
                        break;
                }
            }
            return expiredItems;
        }
    }

    // aka remove_failed_items
    @Override
    public List<StringQueueElement> removeFailedElements(Predicate<StringQueueElement> filter,
                                                         int chunk, int logLimit) {
        try (Jedis rh = getJedis()) {
            List<StringQueueElement> removedItems = new ArrayList<>();

            processFailedItems(chunk, item -> {
                if (filter.test(item)) {
                    String key = idToKey(item.getId());
                    rh.del(itemKey(key));
                    rh.del(metaKey(key));
                    if (removedItems.size() < logLimit) {
                        removedItems.add(item);
                    }
                    // all good, should be marked deleted
                    return true;
                }
                // since the item was not supposed to be deleted,
                // the callback should send failure since we dont
                // want process failed items to delete the key
                // references, because that'll be very very bad.
                return false;
            });
            return removedItems;
        }
    }

    private long[] processFailedItems(int maxCount, Predicate<StringQueueElement> filter) {
        assurePositive(maxCount);

        try (Jedis rh = getJedis()) {
            String temp_table = "temp-" + getFailedQueueName() + UUID.randomUUID().toString().replace("-", "");
            long failedItems = rh.llen(getFailedQueueName());

            List<String> done = new ArrayList<>();
            int errorCount = 0;
            long itemCount = (maxCount <= failedItems) ? maxCount : failedItems;
            IntStream.range(0, (int) itemCount).
                    forEach(i -> rh.rpoplpush(getFailedQueueName(), temp_table));

            for (int i = 0; i < itemCount; i++) {
                String key = rh.rpop(temp_table);

                try {
                    if (filter.test(getItemByKey(key))) {
                        // true means success, so we should only declare those items done.
                        done.add(key);
                    } else {
                        // We should not throw a warning when the Consumer
                        // explicitly decides to return a false from the
                        // callback.
                        rh.lpush(temp_table, key);
                    }
                } catch (Exception e) {
                    // this is where the requeueing of items is taken care of. The requeueing now becomes a
                    // responsibility of process_failed_items irrespective of what the callback does.
                    errorCount++;
                    rh.lpush(temp_table, key);
                }
            }

            // Accounting for the chunk size, when the failed_queue has more
            // items than what was requested.
            //
            // This also accounts for the order of the elements to be kept
            // the same as it was before processing.
            String key;
            while ((key = rh.lpop(temp_table)) != null) {
                rh.rpush(getFailedQueueName(), key);
            }

            rh.del(temp_table);
            done.forEach(k -> rh.del(metaKey(k), itemKey(k)));

            return new long[] {itemCount, errorCount};
        }
    }

    @Override
    public void flush() {
        try (Jedis rh = getJedis()) {
            for (String queueName : getAllQueues()) {
                List<String> keys = rh.lrange(queueName, 0, -1);
                keys.forEach(key -> rh.del(itemKey(key), metaKey(key)));
                rh.del(queueName);
            }
        }
    }

    @Override
    public long length() {
        try (Jedis jedis = getJedis()) {
            return jedis.llen(getUnprocessedQueueName());
        }
    }

    @Override
    public Map<String, Long> stats() {
        try (Jedis jedis = getJedis()) {
            return getAllQueues().stream().collect(toMap(e -> e, jedis::llen));
        }
    }

    public Map<String, List<StringQueueElement>> getState() {
        return getAllQueues().stream().collect(toMap(q -> q, this::getRawItems));
    }

    @SuppressWarnings("unused")
    public void doInOneConnection(Runnable r) {
        try (Jedis rh = getJedis()) {
            r.run();
        }
    }

    private long requeue(List<String> ids, String sourceQueue, boolean putToTheHead, boolean incrementProcessCount) {
        assureNotEmptyAndWithoutNulls(ids);

        try (Jedis rh = getJedis()) {
            int idsRequeued = 0;
            for (String key : idsToKeys(ids)) {
                long n = rh.lrem(sourceQueue, 1, key);

                if (n > 0) {
                    String metaKey = metaKey(key);
                    long processCount = rh.hincrBy(metaKey, "process_count", incrementProcessCount ? 1 : 0);
                    String destQueue = getUnprocessedQueueName();

                    if (processCount > requeueLimit) {
                        destQueue = getFailedQueueName();
                        rh.hincrBy(metaKey, "bail_count", 1);
                        rh.hset(metaKey, "process_count", "0");
                    }

                    if (putToTheHead) {
                        rh.lpush(destQueue, key);
                    } else {
                        rh.rpush(destQueue, key);
                    }
                    idsRequeued += n;
                }
            }
            return idsRequeued;
        }
    }

    private List<StringQueueElement> getRawItems(String concreteQueueName) {
        return getRawItems(concreteQueueName, Integer.MAX_VALUE);
    }

    private List<StringQueueElement> getRawItems(String concreteQueueName, long numberOfItems) {
        assurePositive(numberOfItems);

        try (Jedis rh = getJedis()) {
            List<String> keys = rh.lrange(concreteQueueName, -numberOfItems, -1);
            return keys.stream().map(this::getItemByKey).collect(toList());
        }
    }

    private List<String> getAllQueues() {
        return Arrays.asList(
                getUnprocessedQueueName(),
                getWorkingQueueName(),
                getFailedQueueName()
        );
    }

    private String getPayloadByKey(String key) {
        try (Jedis rh = getJedis()) {
            String payload = rh.get(itemKey(key));
            if (payload == null) {
                throw new IllegalStateException(String.format(
                        "Found item_key: '%s' but not its key! This should never happen!", key));
            }
            return payload;
        }
    }

    private Metadata getMetadataByKey(String key) {
        try (Jedis rh = getJedis()) {
            Map<String, String> metadata = rh.hgetAll(metaKey(key));
            if (metadata == null) {
                throw new IllegalStateException(String.format(
                        "Found item_key: '%s' but not its metadata! This should never happen!", key));
            }
            return new Metadata(metadata);
        }
    }

    private StringQueueElement getItemByKey(String key) {
        return new StringQueueElement(
                keyToId(key),
                getPayloadByKey(key),
                getMetadataByKey(key)
        );
    }

    private Jedis getJedis() {
        // we are reusing the same jedis instance in nested calls
        jedisCallsThreadLocal.set(jedisCallsThreadLocal.get() + 1);
        if (jedisThreadLocal.get() == null) {
            jedisThreadLocal.set(new Jedis(host, port, connectionTimeout, soTimeout) {
                @Override
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

    private String getUnprocessedQueueName() {
        return queueName + "_" + UNPROCESSED_QUEUE;
    }

    private String getWorkingQueueName() {
        return queueName + "_" + WORKING_QUEUE;
    }

    private String getFailedQueueName() {
        return queueName + "_" + FAILED_QUEUE;
    }

    private String idToKey(String id) {
        return queueName + "-" + id;
    }

    private String keyToId(String key) {
        return key.substring(queueName.length() + 1);
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

    private static void assureNotEmptyAndWithoutNulls(List<?> elements) {
        if (elements == null) {
            throw new NullPointerException("Expected not null collection");
        }
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Expected at least one element");
        }
        if (elements.stream().anyMatch(e -> e == null)) {
            throw new NullPointerException("Null elements not allowed: " + elements);
        }
    }

    private void assurePositive(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Value should be greater than 0: " + value);
        }
    }


}
