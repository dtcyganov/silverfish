package org.github.silverfish.client.rabbitmq;

import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.GetResponse;
import org.github.silverfish.client.Backend;
import org.github.silverfish.client.ItemState;
import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by pbiswas on 2/25/2016.
 */
public class RabbitMQ<E> implements Backend<E> {
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
    public int enqueue(final List<E> items) throws IOException {
        int totalSize = 0;
        if (items == null || items.isEmpty()) {
            return totalSize;
        }

        for (final E item : items) {
            final byte[] data = serialize(item);
            totalSize += data.length;

            mqConnection.publish(data);
        }

        return totalSize;
    }

    @Override
    public Map<String, E> dequeue(int count, boolean blocking) throws IOException, ClassNotFoundException, InterruptedException {
        final Map<String, E> map = new HashMap<>();

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

            final E data = deserialize(response.getBody());

            map.put("" + deliveryTag, data);
            if (response.getEnvelope().isRedeliver()) {
                redeliveredIds.put(deliveryTag, 1);
            }
            timeSpent += (new Date().getTime() - response.getProps().getTimestamp().getTime()) / 1000;
            count--;
        }

        //TODO timeSpent is not returned
        return map;
    }

    @Override
    public int markProcessed(final List<Long> ids) throws IOException {
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
    public int markFailed(final List<Long> ids) throws IOException {
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
    public List<E> peek(int limit) {
        throw new UnsupportedOperationException("Sorry, RabbitMQ cannot peek");
    }

    @Override
    public void cleanup() {
    }

    @Override
    public List<E> collectGarbage() {
        return null;
    }

    @Override
    public void flush() throws IOException {
        mqConnection.flush();
    }

    @Override
    public int length() throws IOException {
        return mqConnection.length();
    }

    @Override
    public Map<ItemState, Integer> stats() throws IOException {
        return mqConnection.stats();
    }

    private byte[] serialize(final E input) throws IOException {
        ByteArrayOutputStream bos = null;
        ObjectOutputStream out = null;

        try {
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);
            out.writeObject(input);
        } catch (final IOException e) {
            //LOGGER.log(Level.SEVERE, "Failed to serialize input object" + e.getMessage());
            throw e;
        } finally {
            try {
                if (out != null) {
                    out.flush();
                    out.close();
                }
            } catch (final Exception e) {
                //LOGGER.log(Level.SEVERE, "Failed to close ObjectOutputStream");
            }
        }

        return bos != null ? bos.toByteArray() : null;
    }

    private E deserialize(final byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = null;
        ObjectInputStream in = null;

        try {
            bis = new ByteArrayInputStream(data);
            in = new ObjectInputStream(bis);
            return (E) in.readObject();
        } catch (final IOException e) {
            //LOGGER.log(Level.SEVERE, "Failed to deserialize data " + e.getMessage());
            throw e;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (final Exception e) {
                //LOGGER.log(Level.SEVERE, "Failed to close ObjectInputStream");
            }
        }

        return null;
    }
}
