package org.github.silverfish.client.rabbitmq;

import com.google.common.base.Strings;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Created by pbiswas on 2/25/2016.
 */
class RabbitMQConnector implements QueueConnector {
    private static final int CHANNEL;
    private static final String QUEUE_TYPE;
    private static final Address[] SERVER_ADDRESSES;
    //private static final Logger LOGGER;

    private Connection conn;
    private Channel channel;
    private final String name;
    private final Map<String, Object> DEAD_LETTER_OPTIONS;

    static {
        CHANNEL = 1;
        QUEUE_TYPE = "Qv1";
        SERVER_ADDRESSES = new Address[] {
                new Address("dc201redisprov-01.lhr4.dqs.booking.com"),
                new Address("dc201redisprov-02.lhr4.dqs.booking.com"),
                new Address("dc201redisprov-03.lhr4.dqs.booking.com")
        };
        //LOGGER = Logger.getLogger("FileLogger");
    }

    public RabbitMQConnector(final String name) throws IOException, TimeoutException {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Argument name is null or empty");
        }

        this.name = name;
        this.DEAD_LETTER_OPTIONS = new HashMap<String, Object>() {{
            put("x-dead-letter-exchange", getDlxName(name));
        }};
        initialize(name);
    }

    public String getDlxName(final String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Argument name is null or empty");
        }
        return "trash__" + name;
    }

    @Override
    public void close() throws IOException {
        try {
            //no need to close channel as conn close will close channel.
            //but channel can be closed as a good practice.
            if (conn != null) {
                conn.close();
            }
        } catch (final IOException e) {
            //LOGGER.log(LEVEL.SEVERE, "Exception while closing the connection");
            throw e;
        }
    }

    @Override
    public void publish(final byte[] data) throws IOException {
        channel.basicPublish(name, name, true,
                new AMQP.BasicProperties().builder()
                .type(QUEUE_TYPE)
                .timestamp(new Date())
                .deliveryMode(2)
                .build(), data);
    }

    @Override
    public GetResponse getResponse() throws IOException {
        return channel.basicGet(name, false);
    }

    @Override
    public void ack(final long deliveryTag) throws IOException {
        channel.basicAck(deliveryTag, false);
    }

    @Override
    public void reject(final long deliveryTag, final boolean requeue) throws IOException {
        channel.basicReject(deliveryTag, requeue);
    }

    @Override
    public int length() throws IOException {
        final AMQP.Queue.DeclareOk result = channel.queueDeclare(name, true, false, false, null);
        return result.getMessageCount();
    }

    @Override
    public void flush() throws IOException {
        channel.queuePurge(name);
    }

    @Override
    public Map<String, Long> stats() throws IOException {
        final Map<String, Long> map = new HashMap<>();
        final AMQP.Queue.DeclareOk result
                = channel.queueDeclare(name, true, false, false, new HashMap<String, Object>() {{ put("passive", 1); }});

        map.put("getUnprocessedElementsLength", (long) result.getQueue().length());
        map.put("consumers", (long) result.getConsumerCount());
        return map;
    }

    private Connection createConnection() throws TimeoutException, IOException {
        ConnectionFactory factory = new ConnectionFactory();
        //factory.setUsername(""); TODO is it required?
        //factory.setPassword("");

        return factory.newConnection(SERVER_ADDRESSES);
    }

    private void initialize(final String name) throws TimeoutException, IOException {
        try {
            conn = createConnection();
            channel = conn.createChannel(CHANNEL);

            final String dlxName = getDlxName(name);
            channel.exchangeDeclare(dlxName, "direct");
            channel.queueDeclare(dlxName, true, false, false, null);
            channel.queueBind(dlxName, dlxName, name);

            //channel.exchangeDeclare(name, "direct");
            channel.queueDeclare(name, true, false, false, DEAD_LETTER_OPTIONS);
            //channel.queueBind(name, name, name);

        } catch (final IOException e) {
            //LOGGER.log(LEVEL.SEVERE, "Error while initializing channel");
            throw e;
        } catch (final TimeoutException e) {
            //LOGGER.log(LEVEL.SEVERE, "Timeout error while initializing channel");
            throw e;
        }
    }
}
