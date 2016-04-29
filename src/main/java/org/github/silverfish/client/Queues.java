package org.github.silverfish.client;

import org.github.silverfish.client.rabbitmq.RabbitMQ;
import org.github.silverfish.client.wrappers.GenericQueueBackendAdapter;

import static java.util.function.Function.identity;

public class Queues {
    private Queues() {}

    public static <E> Backend<Long, E, Void, QueueElement<Long, E, Void>> createGenericRabbitMQ(RabbitMQ rabbitMQ) {
        return new GenericQueueBackendAdapter<Long, E, Void, Long, byte[], Void>(rabbitMQ,
                Serializers.<E>createPlainJavaSerializer(),
                Serializers.<E>createPlainJavaDeserializer(),
                identity(),
                identity(),
                identity(),
                identity());
    }
}
