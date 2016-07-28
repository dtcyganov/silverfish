package org.github.silverfish.client.redis;

import org.github.silverfish.client.Backend;
import org.github.silverfish.client.CleanupAction;
import org.github.silverfish.client.GenericQueueBackendAdapter;
import org.github.silverfish.client.QueueElement;
import org.github.silverfish.client.impl.Metadata;
import org.github.silverfish.client.impl.StatsMetadataSupplier;
import org.github.silverfish.client.impl.StringQueueElement;
import org.github.silverfish.client.impl.UUIDSupplier;
import org.github.silverfish.client.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class RedisQueueBackendTest {

    private Backend<String, String, Metadata, StringQueueElement> backend;
    private Process redisProcess;

    @Before
    public void setUp() throws Exception {
        redisProcess = new ProcessBuilder("redis-server",  "--port", "6389").start();
        backend = new GenericQueueBackendAdapter<>(
                new RedisQueueBackend(
                        new RedisQueueOperations("localhost", 6389, "test"),
                        new UUIDSupplier(),
                        new StatsMetadataSupplier()),
                Util::getBytes,
                Util::bytesToString,
                identity(),
                identity(),
                identity(),
                identity(),
                StringQueueElement::new
        );

        Thread.sleep(100);
    }

    @After
    public void tearDown() throws Exception {
        redisProcess.destroy();
        Thread.sleep(100);
    }

    @Test
    public void testEnqueueAndPeak() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = backend.enqueueNewElements("test-1", "test-2");
        assertEquals(2, enqueueResult.size());
        assertEquals("test-1", enqueueResult.get(0).getElement());
        assertEquals("test-2", enqueueResult.get(1).getElement());

        List<StringQueueElement> peekResult = backend.peekUnprocessedElements(Integer.MAX_VALUE);
        assertEquals(2, peekResult.size());

        assertEquals("test-2", peekResult.get(0).getElement());
        assertEquals("test-1", peekResult.get(1).getElement());

        assertEquals(enqueueResult.get(0).getId(), peekResult.get(1).getId());
        assertEquals(enqueueResult.get(1).getId(), peekResult.get(0).getId());

        printState();
    }

    @Test
    public void testEnqueueAndDequeue() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = backend.enqueueNewElements("test-1", "test-2");

        List<StringQueueElement> dequeueResult = backend.dequeueForProcessing(Integer.MAX_VALUE, false);
        assertEquals(2, dequeueResult.size());

        assertEquals("test-1", dequeueResult.get(0).getElement());
        assertEquals("test-2", dequeueResult.get(1).getElement());

        assertEquals(enqueueResult.get(0).getId(), dequeueResult.get(0).getId());
        assertEquals(enqueueResult.get(1).getId(), dequeueResult.get(1).getId());

        assertEquals(0, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();
    }

    @Test
    public void testEnqueueDequeueAndMarkProcessed() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = backend.enqueueNewElements("test-1", "test-2");
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());
        backend.dequeueForProcessing(Integer.MAX_VALUE, false);
        assertEquals(2, backend.markProcessed(ids));

        printState();
    }

    @Test
    public void testEnqueueAndSeveralDequeue() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = backend.enqueueNewElements(Arrays.asList("test-1", "test-2", "test-3"));

        List<StringQueueElement> dequeueResult = backend.dequeueForProcessing(1, false);
        assertEquals(1, dequeueResult.size());
        assertEquals(enqueueResult.get(0).getId(), dequeueResult.get(0).getId());

        List<StringQueueElement> secondDequeueResult = backend.dequeueForProcessing(3, false);
        assertEquals(2, secondDequeueResult.size());
        assertEquals(enqueueResult.get(1).getId(), secondDequeueResult.get(0).getId());
        assertEquals(enqueueResult.get(2).getId(), secondDequeueResult.get(1).getId());

        printState();
    }

    @Test
    public void testFlush() throws Exception {
        printState();

        backend.enqueueNewElements("test-1", "test-2");
        backend.flush();
        assertEquals(0, backend.peekUnprocessedElements(Integer.MAX_VALUE).size());
        assertEquals(0, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();
    }

    @Test
    public void testMarkFailed() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = backend.enqueueNewElements("test-1", "test-2");
        printState();
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());
        backend.dequeueForProcessing(Integer.MAX_VALUE, false);
        printState();
        assertEquals(2, backend.markFailed(ids));

        printState();
    }

    @Test
    public void testCollectGarbage() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = backend.enqueueNewElements("test-1", "test-2");
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());

        assertEquals(2, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());
        assertEquals(2, backend.markFailed(ids));

        assertEquals(2, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());
        assertEquals(2, backend.markFailed(ids));

        assertEquals(2, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());
        assertEquals(2, backend.markFailed(ids));

        assertEquals(0, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();

        List<StringQueueElement> garbage = backend.removeFailedElements(m -> true, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(2, garbage.size());
        assertEquals(ids.get(0), garbage.get(0).getId());
        assertEquals(ids.get(1), garbage.get(1).getId());

        printState();
    }

    @Test
    public void testCleanup() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = backend.enqueueNewElements("test-1", "test-2");
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());

        assertEquals(2, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();

        List<StringQueueElement> cleanupResult = backend.cleanup(CleanupAction.REQUEUE, m -> true);
        assertEquals(2, cleanupResult.size());
        assertEquals(ids.get(0), cleanupResult.get(0).getId());
        assertEquals(ids.get(1), cleanupResult.get(1).getId());

        printState();

        assertEquals(2, backend.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();
    }

    private void printState() {
        System.out.println("State:");
        new TreeMap<>(backend.getState()).forEach((queueName, queueElements) -> {
            System.out.println(">>> " + queueName);
            queueElements.forEach(System.out::println);
        });
        System.out.println();
    }

}
