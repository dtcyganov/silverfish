package org.github.silverfish.client;

import java.util.List;
import java.util.Map;

public interface Backend<E> {

    /**
     * Put <code>items</code> to the queue.
     *
     * @param items items to put
     * @return number of added items
     */
    int enqueue(List<E> items);

    /**
     * Takes <code>count</code> elements from the queue.
     *
     * @param count how many elements to take
     * @param blocking wait until some data
     * @return map from identifier of payload item to the payload item
     */
    Map<String, E> dequeue(int count, boolean blocking);

    /**
     * Mark elements as processed.
     *
     * @param ids which elements to mark
     */
    void markProcessed(List<String> ids);

    /**
     * Mark elements as failed.
     *
     * @param ids which elements to mark
     */
    void markFailed(List<String> ids);

    /**
     * Get last <code>limit</code> elements.
     *
     * @param limit elements count to return
     * @return last elements.
     */
    List<E> peek(int limit);

    /**
     * Removes busy expired items.
     */
    void cleanup();

    /**
     * Handle failed items.
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
