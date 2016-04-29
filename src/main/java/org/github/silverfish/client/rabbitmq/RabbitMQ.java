package org.github.silverfish.client.rabbitmq;

import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.GetResponse;
import org.github.silverfish.client.Backend;
import org.github.silverfish.client.QueueElement;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Created by pbiswas on 2/25/2016.
 */
public class RabbitMQ implements Backend<Long, byte[], Void, QueueElement<Long, byte[], Void>> {
    private static final String QUEUE_TYPE;
    //private static final Logger LOGGER;

    static {
        //LOGGER = Logger.getLogger("FileLogger");
        QUEUE_TYPE = "Qv1";
    }

    private final String name;
    private final int maxAttempts;
    private final QueueConnector mqConnection;
    private final Map<Long, Integer> redeliveredIds = new HashMap<>();

    public RabbitMQ(final String name, final int maxAttempts) throws IOException, TimeoutException {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Argument name is null or empty");
        }

        if (maxAttempts < 1 || maxAttempts > 2) {
            throw new IllegalArgumentException("RabbitMQ only supports max_attempts = 1 or 2");
        }

        this.name = name;
        this.maxAttempts = maxAttempts;
        this.mqConnection = new RabbitMQConnector(name);
    }

    public RabbitMQ(final String name) throws IOException, TimeoutException {
        this(name, 2);
    }

    public void close() throws IOException {
        try {
            if (mqConnection != null) {
                mqConnection.close();
            }
        } catch(final IOException e) {
            //LOGGER.log(Level.SEVERE, "Failed to close RabbitMQ connection " + e.getStackTrace());
            throw e;
        }
    }

    @Override
    public List<QueueElement<Long, byte[], Void>> enqueue(final List<byte[]> items) throws IOException {
        if (items == null || items.isEmpty()) {
            return Collections.emptyList();
        }

        List<QueueElement<Long, byte[], Void>> result = new ArrayList<>(items.size());
        for (final byte[] item : items) {
            mqConnection.publish(item);
            //TODO: add real id here
            result.add(new QueueElement<>(null, item, null));
        }

        return result;
    }

    @Override
    public List<QueueElement<Long, byte[], Void>> dequeue(long count, boolean blocking) throws IOException, InterruptedException {
        final List<QueueElement<Long, byte[], Void>> result = new ArrayList<>();

        int timeSpent = 0;

        while (count > 0) {
            final GetResponse response = mqConnection.getResponse();
            if (response == null) {
                if (!blocking) {
                    break;
                }

                Thread.sleep(100);
                count = 1;
                continue;
            }

            final AMQP.BasicProperties props = response.getProps();
            final long deliveryTag = response.getEnvelope().getDeliveryTag();

            if (!props.getType().equals(QUEUE_TYPE)) {
                //LOGGER.log(Level.WARNING,
                //        String.format("Item with type <%s> found where <%s> expected; ACKing and skipping", props.getType(), QUEUE_TYPE));
                mqConnection.ack(deliveryTag);
                continue;
            }

            final byte[] data = response.getBody();

            result.add(new QueueElement<>(deliveryTag, data, null));
            if (response.getEnvelope().isRedeliver()) {
                redeliveredIds.put(deliveryTag, 1);
            }
            timeSpent += (System.currentTimeMillis() - response.getProps().getTimestamp().getTime()) / 1000;
            count--;
        }

        //TODO timeSpent is not returned
        return result;
    }

    @Override
    public long markProcessed(final List<Long> ids) throws IOException {
        if (ids == null || ids.size() == 0) {
            return 0;
        }

        for (final Long id : ids) {
            if (redeliveredIds.containsKey(id)) {
                redeliveredIds.remove(id);
            }
            mqConnection.ack(id);
        }

        return ids.size();
    }

    @Override
    public long markFailed(final List<Long> ids) throws IOException {
        if (ids == null || ids.size() == 0) {
            return 0;
        }

        for (final Long id : ids) {
            boolean requeue = true;
            if (redeliveredIds.containsKey(id) && maxAttempts > 1) {
                redeliveredIds.remove(id);
                requeue = false;
            }
            mqConnection.reject(id, requeue);
        }

        return ids.size();
    }

    @Override
    public List<QueueElement<Long, byte[], Void>> peek(long limit) {
        throw new UnsupportedOperationException("Sorry, RabbitMQ cannot peek");
    }

    @Override
    public void cleanup() {
    }

    @Override
    public List<QueueElement<Long, byte[], Void>> collectGarbage() {
        return null;
    }

    @Override
    public void flush() throws IOException {
        mqConnection.flush();
    }

    @Override
    public long length() throws IOException {
        return mqConnection.length();
    }

    @Override
    public Map<String, Long> stats() throws IOException {
        return mqConnection.stats();
    }
}
