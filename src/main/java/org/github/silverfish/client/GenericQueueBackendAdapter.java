package org.github.silverfish.client;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

public class GenericQueueBackendAdapter<I, E, M, QE extends QueueElement<I, E, M>, BI, BE, BM> implements Backend<I, E, M, QE> {

    private final Backend<BI, BE, BM, ? extends QueueElement<BI, BE, BM>> backend;

    private final Function<E, BE> elementSerializer;
    private final Function<BE, E> elementDeserializer;

    private final Function<I, BI> idSerializer;
    private final Function<BI, I> idDeserializer;

    private final Function<M, BM> metadataSerializer;
    private final Function<BM, M> metadataDeserializer;

    private final QueueElementCreator<I, E, M, QE> queueElementCreator;

    public GenericQueueBackendAdapter(Backend<BI, BE, BM, ? extends QueueElement<BI, BE, BM>> backend,
                                      Function<E, BE> elementSerializer,
                                      Function<BE, E> elementDeserializer,
                                      Function<I, BI> idSerializer,
                                      Function<BI, I> idDeserializer,
                                      Function<M, BM> metadataSerializer,
                                      Function<BM, M> metadataDeserializer,
                                      QueueElementCreator<I, E, M, QE> queueElementCreator) {

        this.backend = backend;
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
        return deserializeQueueElements(backend.enqueueNewElements(serializedItems));
    }

    @SafeVarargs
    @Override
    public final List<QE> enqueueNewElements(E... elements) throws Exception {
        return enqueueNewElements(Arrays.asList(elements));
    }

    @Override
    public List<QE> dequeueForProcessing(long count, boolean blocking) throws Exception {
        return deserializeQueueElements(backend.dequeueForProcessing(count, blocking));
    }

    @Override
    public long markProcessed(List<I> ids) throws Exception {
        return backend.markProcessed(serializeIds(ids));
    }

    @Override
    public long markFailed(List<I> ids) throws Exception {
        return backend.markFailed(serializeIds(ids));
    }

    @Override
    public List<QE> peekUnprocessedElements(long limit) throws Exception {
        return deserializeQueueElements(backend.peekUnprocessedElements(limit));
    }

    @Override
    public List<QE> cleanup(CleanupAction cleanupAction, Predicate<M> condition) throws Exception {
        return deserializeQueueElements(backend.cleanup(cleanupAction, bm -> condition.test(metadataDeserializer.apply(bm))));
    }

    @Override
    public List<QE> removeFailedElements(Predicate<QE> filter, int chunk, int logLimit) throws Exception {
        return deserializeQueueElements(backend.removeFailedElements(
                e -> filter.test(deserializeQueueElement(e)),
                chunk, logLimit
        ));
    }

    @Override
    public void flush() throws Exception {
        backend.flush();
    }

    @Override
    public long length() throws Exception {
        return backend.length();
    }

    @Override
    public Map<String, Long> stats() throws Exception {
        return backend.stats();
    }

    @Override
    public Map<String, List<QE>> getState() {
        Map<String, List<QE>> result = new LinkedHashMap<>();
        backend.getState().forEach((queueName, queueElements) ->
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
}
