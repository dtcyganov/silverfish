package org.github.silverfish.client;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public interface Backend<I, E, M, QE extends QueueElement<I, E, M>> {

    /**
     * Put <code>elements</code> to the queue.
     *
     * @param elements elements to put
     * @return list of added elements wrapped into {@link QueueElement}
     */
    List<QE> enqueueNewElements(List<E> elements) throws Exception;

    /**
     * Put <code>elements</code> to the queue.
     *
     * @param elements elements to put
     * @return list of added elements wrapped into {@link QueueElement}
     */
    List<QE> enqueueNewElements(E... elements) throws Exception;

    /**
     * Takes <code>count</code> elements from the queue.
     *
     * @param count how many elements to take
     * @param blocking wait until some data
     * @return list of dequeued elements wrapped into {@link QueueElement}
     */
    List<QE> dequeueForProcessing(long count, boolean blocking) throws Exception;

    /**
     * Mark elements as processed.
     *
     * @param ids which elements to mark
     * TODO: return elements
     */
    long markProcessed(List<I> ids) throws Exception;

    /**
     * Mark elements as failed.
     *
     * @param ids which elements to mark
     * TODO: return elements
     */
    long markFailed(List<I> ids) throws Exception;

    /**
     * Get last <code>limit</code> elements.
     *
     * @param limit elements count to return
     * @return last elements.
     */
    List<QE> peekUnprocessedElements(long limit) throws Exception;

    /**
     * Removes busy expired items.
     * TODO: fix javadoc
     */
    List<QE> cleanup(CleanupAction cleanupAction, Predicate<M> condition) throws Exception;

    /**
     * Handle failed items.
     *
     * @return payoads of failed items
     * TODO: fix javadoc
     */
    List<QE> removeFailedElements(Predicate<QE> filter,
                                  int chunk, int logLimit) throws Exception;

    /**
     * Deletes everything in the queue
     */
    void flush() throws Exception;

    /**
     * Returns number of elements in the queue.
     *
     * @return number of elements in the queue
     */
    long length() throws Exception;

    /**
     * Returns stats of the queue (how many elements are in each state).
     *
     * @return stats of the queue (how many elements are in each state)
     */
    Map<String, Long> stats() throws Exception;

    /**
     * Returns content of all subqueues in the system.
     * Use for debugging on small datasets.
     *
     * @return content of all subqueues in the system.
     */
    Map<String, List<QE>> getState();
}
