package org.github.silverfish.client.redis;

import com.google.common.collect.Lists;
import org.github.silverfish.client.WorkingQueue;
import org.github.silverfish.client.QueueElement;
import org.github.silverfish.client.impl.ByteArrayQueueElement;
import org.github.silverfish.client.impl.Metadata;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.github.silverfish.client.util.ValidityUtils.*;

public class RedisWorkingQueue implements WorkingQueue<String, byte[], Metadata, ByteArrayQueueElement> {

    private static final int DEFAULT_CLAIM_WAIT_TIMEOUT = 30_000;
    private static final int DEFAULT_REQUEUE_LIMIT = 5;

    // queues
    private static final String UNPROCESSED_QUEUE = "unprocessed";
    private static final String WORKING_QUEUE = "working";
    private static final String FAILED_QUEUE = "failed";

    // metadata fields
    private static final String PROCESS_COUNT = "process_count";
    private static final String BAIL_COUNT = "bail_count";

    private final int claimWaitTimeout;
    private final int requeueLimit;

    private final Supplier<String> idSupplier;
    private final Supplier<Metadata> metadataSupplier;

    private final RedisQueueOperations redis;

    public RedisWorkingQueue(RedisQueueOperations redis,
                             Supplier<String> idSupplier, Supplier<Metadata> metadataSupplier) {

        this(redis, idSupplier, metadataSupplier,
                DEFAULT_CLAIM_WAIT_TIMEOUT, DEFAULT_REQUEUE_LIMIT);
    }

    public RedisWorkingQueue(RedisQueueOperations redis,
                             Supplier<String> idSupplier, Supplier<Metadata> metadataSupplier,
                             int claimWaitTimeout, int requeueLimit) {

        this.redis = redis;
        this.idSupplier = idSupplier;
        this.metadataSupplier = metadataSupplier;
        this.claimWaitTimeout = claimWaitTimeout;
        this.requeueLimit = requeueLimit;
    }

    @Override
    public List<ByteArrayQueueElement> enqueueNewElements(List<byte[]> elements) {
        assureNotEmptyAndWithoutNulls(elements);

        List<ByteArrayQueueElement> result = elements.stream().map(
                e -> new ByteArrayQueueElement(idSupplier.get(), e, metadataSupplier.get())
        ).collect(toList());

        redis.doInOneConnection(() -> {
            redis.register(result);
            redis.enqueue(UNPROCESSED_QUEUE, result.stream().map(QueueElement::getId).collect(toList()));
        });
        return result;
    }

    @Override
    public List<ByteArrayQueueElement> dequeueForProcessing(final long count, final boolean blocking) {
        assurePositive(count);

        return redis.doInOneConnection(() -> {
            List<String> ids = redis.requeue(UNPROCESSED_QUEUE, WORKING_QUEUE, count, blocking, claimWaitTimeout);
            ids.forEach(id -> redis.incrementMetadataCounter(id, PROCESS_COUNT, 1));
            return ids.stream().map(redis::getItemById).collect(toList());
        });
    }

    @Override
    public List<String> markProcessed(List<String> ids) {
        assureNotEmptyAndWithoutNulls(ids);

        return redis.doInOneConnection(() -> {
            List<String> removedIds = redis.dequeue(WORKING_QUEUE, ids);
            redis.unregister(ids);
            return removedIds;
        });
    }

    @Override
    public List<String> markFailed(List<String> ids) {
        assureNotEmptyAndWithoutNulls(ids);

        return requeue(ids, WORKING_QUEUE);
    }

    @Override
    public List<ByteArrayQueueElement> peekUnprocessedElements(long limit) {
        assurePositive(limit);

        return redis.doInOneConnection(() -> {
            List<String> ids = redis.peek(UNPROCESSED_QUEUE, limit);
            return ids.stream().map(redis::getItemById).collect(toList());
        });
    }

    @Override
    public void requeueWorkingElements(Predicate<Metadata> filter, Consumer<ByteArrayQueueElement> consumer) {
        assureNotNull(filter);

        redis.doInOneConnection(() -> {
            List<String> itemsToCleanUp = redis.peek(WORKING_QUEUE, Long.MAX_VALUE).stream().
                    filter(id -> filter.test(redis.getMetadataById(id))).
                    collect(toList());

            if (consumer != null) {
                requeue(itemsToCleanUp, WORKING_QUEUE).stream().map(redis::getItemById).forEach(consumer);
            }
        });
    }

    @Override
    public void dropWorkingElements(Predicate<Metadata> filter, Consumer<ByteArrayQueueElement> consumer) {
        assureNotNull(filter);

        redis.doInOneConnection(() -> {
            List<String> itemsToCleanUp = redis.peek(WORKING_QUEUE, Long.MAX_VALUE).stream().
                    filter(id -> filter.test(redis.getMetadataById(id))).
                    collect(toList());

            List<String> removedIds = redis.dequeue(WORKING_QUEUE, itemsToCleanUp);
            if (consumer != null) {
                removedIds.stream().map(redis::getItemById).forEach(consumer);
            }
            redis.unregister(itemsToCleanUp);
        });
    }

    @Override
    public List<ByteArrayQueueElement> removeFailedElements(Predicate<ByteArrayQueueElement> filter,
                                                         int chunk, int logLimit) {
        assureNotNull(filter);
        assurePositive(chunk);
        assureNonNegative(logLimit);

        String tempQueue = "temp-" + FAILED_QUEUE + UUID.randomUUID();
        return redis.doInOneConnection(() -> {
            redis.requeue(FAILED_QUEUE, tempQueue, chunk);
            List<String> ids = redis.dequeue(tempQueue, Long.MAX_VALUE);
            List<String> idsToRemove = ids.stream().filter(id -> filter.test(redis.getItemById(id))).collect(toList());
            List<ByteArrayQueueElement> logItems = idsToRemove.stream().limit(logLimit).map(redis::getItemById).collect(toList());
            ids.removeAll(idsToRemove);
            redis.unregister(idsToRemove);
            redis.enqueue(FAILED_QUEUE, ids);
            redis.deleteQueue(tempQueue);
            return logItems;
        });
    }

    @Override
    public void clean() {
        redis.doInOneConnection(() -> {
            for (String queueName : getAllQueues()) {
                List<String> ids = redis.peekAll(queueName);
                redis.unregister(ids);
                redis.deleteQueue(queueName);
            }
        });
    }

    @Override
    public long getUnprocessedElementsLength() {
        return redis.length(UNPROCESSED_QUEUE);
    }

    @Override
    public Map<String, Long> stats() {
        return redis.doInOneConnection(() ->
            getAllQueues().stream().collect(toMap(e -> e, redis::length))
        );
    }

    private List<String> requeue(List<String> ids, String sourceQueue) {
        return redis.doInOneConnection(() -> {
            List<String> removedIds = redis.dequeue(sourceQueue, ids);
            Map<String, Long> idToProcessCount = removedIds.stream().collect(toMap(
                    id -> id, id -> redis.incrementMetadataCounter(id, PROCESS_COUNT, 1)
            ));
            Predicate<String> moreThenRequeueLimit = id -> idToProcessCount.get(id) > requeueLimit;
            List<String> idsToMoveToFailedQueue = removedIds.stream().filter(moreThenRequeueLimit).collect(toList());
            List<String> idsToMoveToUnprocessedQueue = removedIds.stream().filter(moreThenRequeueLimit.negate()).collect(toList());
            for (String id : idsToMoveToFailedQueue) {
                redis.incrementMetadataCounter(id, BAIL_COUNT, 1);
                redis.setMetadataCounter(id, PROCESS_COUNT, 0);
            }
            redis.enqueue(UNPROCESSED_QUEUE, idsToMoveToUnprocessedQueue);
            redis.enqueue(FAILED_QUEUE, idsToMoveToFailedQueue);
            return Lists.reverse(removedIds);
        });
    }

    @Override
    public Map<String, List<ByteArrayQueueElement>> getState() {
        return redis.doInOneConnection(() ->
                getAllQueues().stream().collect(toMap(q -> q, q -> getRawItems(q, Integer.MAX_VALUE)))
        );
    }

    private List<ByteArrayQueueElement> getRawItems(String queueName, long numberOfItems) {
        assurePositive(numberOfItems);

        return redis.doInOneConnection(() -> redis.peek(queueName, numberOfItems).
                stream().map(redis::getItemById).collect(toList()));
    }

    private List<String> getAllQueues() {
        return asList(
                UNPROCESSED_QUEUE,
                WORKING_QUEUE,
                FAILED_QUEUE
        );
    }
}
