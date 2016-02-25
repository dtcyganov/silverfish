package org.github.silverfish.client;

import java.util.List;
import java.util.Map;

public interface Backend<E> {

    /**
     * Send <code>items</code> to the queue, to be enqueued.
     *
     * @param items list of items to send, just data
     * @return number of added items
     */
    int enqueue(List<E> items);

    /**
     * Takes <code>count</code> elements from the queue.
     *
     * @param count how many elements to take from the queue
     * @param blocking blocking behaviour; if true, it will wait for data to appear in the queue, if false, it will
     *                 immediately return on an empty queue
     * @return the data you asked for; keyed on payload UUID, with the payload in the value
     */
    Map<String, E> dequeue(int count, boolean blocking);

    /**
     * Mark elements as processed.
     *
     * @param ids list of UUIDs to mark as processed
     */
    void markProcessed(List<String> ids);

    /**
     * Mark elements as failed.
     *
     * @param ids list of UUIDs to mark as failed
     */
    void markFailed(List<String> ids);

    /**
     * Get the last <code>limit</code> elements from the queue. It is just a way of sampling the data.
     *
     * @param limit element count to return
     * @return last <code>limit</code> elements
     */
    List<E> peek(int limit);

    /**
     * Cleans up the working queue with payloads that have been languishing there for too long. A specified timeout
     * tells you how long a consumer can take in order to process an item. If that timeout is exceeded, it will mark
     * that item as failed and then take the appropriate action. Either re-enqueue it, or remove it from the queue as
     * trash.
     */
    void cleanup();

    /**
     * Collects failed items from the failed sub-queue and takes the appropriate action with these payloads.
     *
     * @return payoads of failed items
     */
    List<E> collectGarbage();

    /**
     * Deletes everything in the queue
     */
    void flush();

    /**
     * Returns number of elements in the queue.
     *
     * @return number of elements in the queue
     */
    int length();

    /**
     * Returns stats of the queue (how many elements are in each state).
     *
     * @return stats of the queue (how many elements are in each state)
     */
    Map<ItemState, Integer> stats();
}
