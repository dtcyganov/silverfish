package org.github.silverfish.client.wrappers;

import org.github.silverfish.client.Backend;
import org.github.silverfish.client.CleanupAction;
import org.github.silverfish.client.QueueElement;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class GenericQueueBackendAdapter<I, E, M, BI, BE, BM> implements Backend<I, E, M, QueueElement<I, E, M>> {

    private final Backend<BI, BE, BM, ? extends QueueElement<BI, BE, BM>> backend;

    private final Function<E, BE> elementSerializer;
    private final Function<BE, E> elementDeserializer;

    private final Function<I, BI> idSerializer;
    private final Function<BI, I> idDeserializer;

    private final Function<M, BM> metadataSerializer;
    private final Function<BM, M> metadataDeserializer;

    public GenericQueueBackendAdapter(Backend<BI, BE, BM, ? extends QueueElement<BI, BE, BM>> backend,
                                      Function<E, BE> elementSerializer,
                                      Function<BE, E> elementDeserializer,
                                      Function<I, BI> idSerializer,
                                      Function<BI, I> idDeserializer,
                                      Function<M, BM> metadataSerializer,
                                      Function<BM, M> metadataDeserializer) {

        this.backend = backend;
        this.elementSerializer = elementSerializer;
        this.elementDeserializer = elementDeserializer;
        this.idSerializer = idSerializer;
        this.idDeserializer = idDeserializer;
        this.metadataSerializer = metadataSerializer;
        this.metadataDeserializer = metadataDeserializer;
    }

    @Override
    public List<QueueElement<I, E, M>> enqueue(List<E> items) throws Exception {
        List<BE> serializedItems = items.stream().map(elementSerializer::apply).collect(toList());
        return deserializeQueueElements(backend.enqueue(serializedItems));
    }

    @SafeVarargs
    @Override
    public final List<QueueElement<I, E, M>> enqueue(E... elements) throws Exception {
        return enqueue(Arrays.asList(elements));
    }

    @Override
    public List<QueueElement<I, E, M>> dequeue(long count, boolean blocking) throws Exception {
        return deserializeQueueElements(backend.dequeue(count, blocking));
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
    public List<QueueElement<I, E, M>> peek(long limit) throws Exception {
        return deserializeQueueElements(backend.peek(limit));
    }

    @Override
    public List<QueueElement<I, E, M>> cleanup(CleanupAction cleanupAction, Predicate<M> condition) throws Exception {
        return deserializeQueueElements(backend.cleanup(cleanupAction, bm -> condition.test(metadataDeserializer.apply(bm))));
    }

    @Override
    public List<QueueElement<I, E, M>> collectGarbage(Predicate<QueueElement<I, E, M>> filter, int chunk, int logLimit) throws Exception {
        return deserializeQueueElements(backend.collectGarbage(
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

    private QueueElement<I, E, M> deserializeQueueElement(QueueElement<BI, BE, BM> e) {
        I id = idDeserializer.apply(e.getId());
        E element = elementDeserializer.apply(e.getElement());
        M metadata = metadataDeserializer.apply(e.getMetadata());
        return new QueueElement<>(id, element, metadata);
    }

    private List<QueueElement<I, E, M>> deserializeQueueElements(List<? extends QueueElement<BI, BE, BM>> elements) {
        return elements.stream().map(this::deserializeQueueElement).collect(Collectors.toList());
    }

    private List<BI> serializeIds(List<I> ids) {
        return ids.stream().map(idSerializer::apply).collect(toList());
    }
}
