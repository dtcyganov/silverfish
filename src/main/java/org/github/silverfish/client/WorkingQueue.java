package org.github.silverfish.client;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Arrays.asList;

/**
 * Interface of working queue. Working queue is a set of pipes which
 * supports controlled flow of tasks throw it.
 *
 * Typical producer code looks like this:
 * <pre>{@code
 *     List<ElementClass> elements = getElements(...);
 *     workingQueue.enqueueNewElements(elements);
 * }</pre>
 *
 * Typical consumer code looks like this:
 * <pre>{@code
 *     List<ElementClass> elements = workingQueue.dequeueForProcessing(count, false);
 *     for (ElementClass e : elements) {
 *         try {
 *             process(e);
 *             workingQueue.markProcessed(e.getId());
 *         } catch (BusinessSpecificException ex) {
 *             workingQueue.markFailed(e.getId());
 *         }
 *     }
 * }</pre>
 *
 * @param <I> class of identifier in the working queue
 * @param <E> class of element in the working queue
 * @param <M> class of metadata of element in the working queue
 * @param <QE> class of queue element (it's a triple of id, element and metadata).
 *            So it should be consistent with I, E, M classes.
 */
public interface WorkingQueue<I, E, M, QE extends QueueElement<I, E, M>> {

    /**
     * Add new elements to the queue.
     *
     * @param elements elements to put
     * @return list of added elements wrapped into {@link QueueElement}
     */
    List<QE> enqueueNewElements(List<E> elements) throws Exception;

    /**
     * Add new elements to the queue.
     *
     * @param elements elements to put
     * @return list of added elements wrapped into {@link QueueElement}
     */
    default List<QE> enqueueNewElements(E... elements) throws Exception {
        return enqueueNewElements(asList(elements));
    }

    /**
     * Takes elements from the working queue for processing.
     * Internally the elements are marked that they are taken for processing.
     * In blocking mode the function call blocks until {@code count} elements
     * will be available.
     * In non-blocking mode the function returns immediately with up to {@code count}
     * returned elements.
     *
     * @param count how many elements to take
     * @param blocking wait until some data
     * @return list of taken elements wrapped into {@link QueueElement}
     */
    List<QE> dequeueForProcessing(long count, boolean blocking) throws Exception;

    /**
     * Mark elements as processed.
     * (Should be taken from the queue with {@code dequeueForProcessing} method)
     *
     * @param ids which elements to mark
     * @return list of ids that actually were marked as processed
     */
    List<I> markProcessed(List<I> ids) throws Exception;

    /**
     * Mark elements as processed.
     * (Should be taken from the queue with {@code dequeueForProcessing} method)
     *
     * @param ids which elements to mark
     * @return list of ids that actually were marked as processed
     */
    default List<I> markProcessed(I... ids) throws Exception {
        return markProcessed(asList(ids));
    }

    /**
     * Mark elements as failed.
     * (Should be taken from the queue with {@code dequeueForProcessing} method)
     *
     * @param ids which elements to mark
     * @return list of ids that actually were marked as failed
     */
    List<I> markFailed(List<I> ids) throws Exception;

    /**
     * Mark elements as failed.
     * (Should be taken from the queue with {@code dequeueForProcessing} method)
     *
     * @param ids which elements to mark
     * @return list of ids that actually were marked as failed
     */
    default List<I> markFailed(I... ids) throws Exception {
        return markFailed(asList(ids));
    }

    /**
     * Get last unprocessed {@code limit} elements.
     * (Without changing its state in the queue)
     *
     * @param limit elements count to return
     * @return last elements.
     */
    List<QE> peekUnprocessedElements(long limit) throws Exception;

    /**
     * Requeue items from working queue to unprocessed queue
     * (this makes them available for processing again).
     *
     * @param filter metadata filter, requeue only accepted items
     * @param consumer source for requeued elements (can be null)
     * @throws Exception
     */
    void requeueWorkingElements(Predicate<M> filter, Consumer<QE> consumer) throws Exception;

    /**
     * Requeue items from working queue to unprocessed queue
     * (this makes them available for processing again).
     *
     * @param filter metadata filter, requeue only accepted items
     * @throws Exception
     */
    default void requeueWorkingElements(Predicate<M> filter) throws Exception {
        requeueWorkingElements(filter, null);
    }

    /**
     * Drops items from working queue.
     *
     * @param filter metadata filter, drop only accepted items
     * @param consumer source for dropped elements (can be null)
     * @throws Exception
     */
    void dropWorkingElements(Predicate<M> filter, Consumer<QE> consumer) throws Exception;

    /**
     * Drops items from working queue.
     *
     * @param filter metadata filter, drop only accepted items
     * @throws Exception
     */
    default void dropWorkingElements(Predicate<M> filter) throws Exception {
        dropWorkingElements(filter, null);
    }

    /**
     * Handle failed items.
     *
     * @return payoads of failed items
     * TODO: fix javadoc
     */
    List<QE> removeFailedElements(Predicate<QE> filter,
                                  int chunk, int logLimit) throws Exception;

    /**
     * Clean all elements in the queue.
     * (Resets queue to the initial state).
     */
    void clean() throws Exception;

    /**
     * Returns number of elements in the queue.
     *
     * @return number of elements in the queue
     */
    long getUnprocessedElementsLength() throws Exception;

    /**
     * Returns stats of the working queue (how many elements are in each state).
     *
     * @return stats of the working queue (how many elements are in each state)
     */
    Map<String, Long> stats() throws Exception;

    /**
     * Returns content of all subqueues in the system.
     * Use for debugging on small data sets.
     *
     * @return content of all subqueues in the system.
     */
    Map<String, List<QE>> getState();
}
