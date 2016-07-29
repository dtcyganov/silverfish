package org.github.silverfish.client;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

public class GenericWorkingQueueAdapter<I, E, M, QE extends QueueElement<I, E, M>, BI, BE, BM> implements WorkingQueue<I, E, M, QE> {

    private final WorkingQueue<BI, BE, BM, ? extends QueueElement<BI, BE, BM>> workingQueue;

    private final Function<E, BE> elementSerializer;
    private final Function<BE, E> elementDeserializer;

    private final Function<I, BI> idSerializer;
    private final Function<BI, I> idDeserializer;

    private final Function<M, BM> metadataSerializer;
    private final Function<BM, M> metadataDeserializer;

    private final QueueElementCreator<I, E, M, QE> queueElementCreator;

    public GenericWorkingQueueAdapter(WorkingQueue<BI, BE, BM, ? extends QueueElement<BI, BE, BM>> workingQueue,
                                      Function<E, BE> elementSerializer,
                                      Function<BE, E> elementDeserializer,
                                      Function<I, BI> idSerializer,
                                      Function<BI, I> idDeserializer,
                                      Function<M, BM> metadataSerializer,
                                      Function<BM, M> metadataDeserializer,
                                      QueueElementCreator<I, E, M, QE> queueElementCreator) {

        this.workingQueue = workingQueue;
        this.elementSerializer = elementSerializer;
        this.elementDeserializer = elementDeserializer;
        this.idSerializer = idSerializer;
        this.idDeserializer = idDeserializer;
        this.metadataSerializer = metadataSerializer;
        this.metadataDeserializer = metadataDeserializer;
        this.queueElementCreator = queueElementCreator;
    }

    @Override
    public List<QE> enqueueNewElements(List<E> items) throws Exception {
        List<BE> serializedItems = items.stream().map(elementSerializer::apply).collect(toList());
        return deserializeQueueElements(workingQueue.enqueueNewElements(serializedItems));
    }

    @Override
    public List<QE> dequeueForProcessing(long count, boolean blocking) throws Exception {
        return deserializeQueueElements(workingQueue.dequeueForProcessing(count, blocking));
    }

    @Override
    public List<I> markProcessed(List<I> ids) throws Exception {
        return deserializeIds(workingQueue.markProcessed(serializeIds(ids)));
    }

    @Override
    public List<I> markFailed(List<I> ids) throws Exception {
        return deserializeIds(workingQueue.markFailed(serializeIds(ids)));
    }

    @Override
    public List<QE> peekUnprocessedElements(long limit) throws Exception {
        return deserializeQueueElements(workingQueue.peekUnprocessedElements(limit));
    }

    @Override
    public void requeueWorkingElements(Predicate<M> filter, Consumer<QE> consumer) throws Exception {
        workingQueue.requeueWorkingElements(
                bm -> filter.test(metadataDeserializer.apply(bm)),
                consumer == null ? null :
                        be -> consumer.accept(deserializeQueueElement(be))
        );
    }

    @Override
    public void dropWorkingElements(Predicate<M> filter, Consumer<QE> consumer) throws Exception {
        workingQueue.dropWorkingElements(
                bm -> filter.test(metadataDeserializer.apply(bm)),
                consumer == null ? null :
                        be -> consumer.accept(deserializeQueueElement(be))
        );
    }

    @Override
    public List<QE> removeFailedElements(Predicate<QE> filter, int chunk, int logLimit) throws Exception {
        return deserializeQueueElements(workingQueue.removeFailedElements(
                e -> filter.test(deserializeQueueElement(e)),
                chunk, logLimit
        ));
    }

    @Override
    public void clean() throws Exception {
        workingQueue.clean();
    }

    @Override
    public long getUnprocessedElementsLength() throws Exception {
        return workingQueue.getUnprocessedElementsLength();
    }

    @Override
    public Map<String, Long> stats() throws Exception {
        return workingQueue.stats();
    }

    @Override
    public Map<String, List<QE>> getState() {
        Map<String, List<QE>> result = new LinkedHashMap<>();
        workingQueue.getState().forEach((queueName, queueElements) ->
                result.put(
                        queueName,
                        queueElements.stream().map(this::deserializeQueueElement).collect(toList())
                )
        );
        return result;
    }

    private QE deserializeQueueElement(QueueElement<BI, BE, BM> e) {
        I id = idDeserializer.apply(e.getId());
        E element = elementDeserializer.apply(e.getElement());
        M metadata = metadataDeserializer.apply(e.getMetadata());
        return queueElementCreator.create(id, element, metadata);
    }

    private List<QE> deserializeQueueElements(List<? extends QueueElement<BI, BE, BM>> elements) {
        return elements.stream().map(this::deserializeQueueElement).collect(toList());
    }

    private List<BI> serializeIds(List<I> ids) {
        return ids.stream().map(idSerializer::apply).collect(toList());
    }

    private List<I> deserializeIds(List<BI> ids) {
        return ids.stream().map(idDeserializer::apply).collect(toList());
    }
}
