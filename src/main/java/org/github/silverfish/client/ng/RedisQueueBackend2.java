package org.github.silverfish.client.ng;

import com.google.common.collect.Lists;
import org.github.silverfish.client.Backend;
import org.github.silverfish.client.CleanupAction;
import org.github.silverfish.client.QueueElement;
import org.github.silverfish.client.impl.StringQueueElement;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.github.silverfish.client.ng.ValidityUtils.*;

public class RedisQueueBackend2 implements Backend<String, String, Metadata, StringQueueElement> {

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

    public RedisQueueBackend2(RedisQueueOperations redis,
                              Supplier<String> idSupplier, Supplier<Metadata> metadataSupplier) {

        this(redis, idSupplier, metadataSupplier,
                DEFAULT_CLAIM_WAIT_TIMEOUT, DEFAULT_REQUEUE_LIMIT);
    }

    public RedisQueueBackend2(RedisQueueOperations redis,
                              Supplier<String> idSupplier, Supplier<Metadata> metadataSupplier,
                              int claimWaitTimeout, int requeueLimit) {

        this.redis = redis;
        this.idSupplier = idSupplier;
        this.metadataSupplier = metadataSupplier;
        this.claimWaitTimeout = claimWaitTimeout;
        this.requeueLimit = requeueLimit;
    }

    @Override
    public List<StringQueueElement> enqueueNewElements(List<String> elements) {
        assureNotEmptyAndWithoutNulls(elements);

        List<StringQueueElement> result = elements.stream().map(
                e -> new StringQueueElement(idSupplier.get(), e, metadataSupplier.get())
        ).collect(toList());

        redis.doInOneConnection(() -> {
            redis.register(result);
            redis.enqueue(UNPROCESSED_QUEUE, result.stream().map(QueueElement::getId).collect(toList()));
        });
        return result;
    }

    @Override
    public List<StringQueueElement> enqueueNewElements(String... elements) throws Exception {
        return enqueueNewElements(asList(elements));
    }

    @Override
    public List<StringQueueElement> dequeueForProcessing(final long count, final boolean blocking) {
        assurePositive(count);

        return redis.doInOneConnection(() -> {
            List<String> ids = redis.requeue(UNPROCESSED_QUEUE, WORKING_QUEUE, count, blocking, claimWaitTimeout);
            ids.forEach(id -> redis.incrementMetadataCounter(id, PROCESS_COUNT, 1));
            return ids.stream().map(redis::getItemById).collect(toList());
        });
    }

    @Override
    public long markProcessed(List<String> ids) {
        assureNotEmptyAndWithoutNulls(ids);

        return redis.doInOneConnection(() -> {
            List<String> removedIds = redis.dequeue(WORKING_QUEUE, ids);
            redis.unregister(ids);
            return removedIds.size();
        });
    }

    @Override
    public long markFailed(List<String> ids) {
        assureNotEmptyAndWithoutNulls(ids);

        return requeue(ids, WORKING_QUEUE).size();
    }

    @Override
    public List<StringQueueElement> peekUnprocessedElements(long limit) {
        assurePositive(limit);

        return redis.doInOneConnection(() -> {
            List<String> ids = redis.peek(UNPROCESSED_QUEUE, limit);
            return ids.stream().map(redis::getItemById).collect(toList());
        });
    }

    @Override
    public List<StringQueueElement> cleanup(CleanupAction cleanupAction,
                                            Predicate<Metadata> filter) {
        assureNotNull(cleanupAction);
        assureNotNull(filter);

        return redis.doInOneConnection(() -> {
            List<String> itemsToCleanUp = redis.peek(WORKING_QUEUE, Long.MAX_VALUE).stream().
                    filter(id -> filter.test(redis.getMetadataById(id))).
                    collect(toList());

            if (cleanupAction == CleanupAction.DROP) {
                List<String> removedIds = redis.dequeue(WORKING_QUEUE, itemsToCleanUp);
                List<StringQueueElement> result = removedIds.stream().map(redis::getItemById).collect(toList());
                redis.unregister(itemsToCleanUp);
                return result;
            }
            if (cleanupAction == CleanupAction.REQUEUE) {
                return requeue(itemsToCleanUp, WORKING_QUEUE).stream().map(redis::getItemById).collect(toList());
            }
            throw new RuntimeException("Unreachable line");
        });
    }

    @Override
    public List<StringQueueElement> removeFailedElements(Predicate<StringQueueElement> filter,
                                                         int chunk, int logLimit) {
        assureNotNull(filter);
        assurePositive(chunk);
        assureNonNegative(logLimit);

        String tempQueue = "temp-" + FAILED_QUEUE + UUID.randomUUID();
        return redis.doInOneConnection(() -> {
            redis.requeue(FAILED_QUEUE, tempQueue, chunk);
            List<String> ids = redis.dequeue(tempQueue, Long.MAX_VALUE);
            List<String> idsToRemove = ids.stream().filter(id -> filter.test(redis.getItemById(id))).collect(toList());
            List<StringQueueElement> logItems = idsToRemove.stream().limit(logLimit).map(redis::getItemById).collect(toList());
            ids.removeAll(idsToRemove);
            redis.unregister(idsToRemove);
            redis.enqueue(FAILED_QUEUE, ids);
            redis.deleteQueue(tempQueue);
            return logItems;
        });
    }

    @Override
    public void flush() {
        redis.doInOneConnection(() -> {
            for (String queueName : getAllQueues()) {
                List<String> ids = redis.peekAll(queueName);
                redis.unregister(ids);
                redis.deleteQueue(queueName);
            }
        });
    }

    @Override
    public long length() {
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

    public Map<String, List<StringQueueElement>> getState() {
        return redis.doInOneConnection(() ->
                getAllQueues().stream().collect(toMap(q -> q, q -> getRawItems(q, Integer.MAX_VALUE)))
        );
    }

    private List<StringQueueElement> getRawItems(String queueName, long numberOfItems) {
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