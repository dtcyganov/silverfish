package org.github.silverfish.client.rabbitmq;

import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Created by pbiswas on 2/26/2016.
 */
interface QueueConnector {
    void close() throws IOException;
    void publish(final byte[] data) throws IOException;
    GetResponse getResponse() throws IOException;
    void ack(final long deliveryTag) throws IOException;
    void reject(final long deliveryTag, final boolean requeue) throws IOException;
    int length() throws IOException;
    void flush() throws IOException;
    Map<String, Long> stats() throws IOException;
}
