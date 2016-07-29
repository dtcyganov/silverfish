package org.github.silverfish.client.redis;

import org.github.silverfish.client.GenericWorkingQueueAdapter;
import org.github.silverfish.client.QueueElement;
import org.github.silverfish.client.WorkingQueue;
import org.github.silverfish.client.impl.Metadata;
import org.github.silverfish.client.impl.StatsMetadataSupplier;
import org.github.silverfish.client.impl.StringQueueElement;
import org.github.silverfish.client.impl.UUIDSupplier;
import org.github.silverfish.client.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class RedisQueueWorkingQueueTest {

    private WorkingQueue<String, String, Metadata, StringQueueElement> workingQueue;
    private Process redisProcess;

    @Before
    public void setUp() throws Exception {
        redisProcess = new ProcessBuilder("redis-server",  "--port", "6389").start();
        workingQueue = new GenericWorkingQueueAdapter<>(
                new RedisWorkingQueue(
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

        List<StringQueueElement> enqueueResult = workingQueue.enqueueNewElements("test-1", "test-2");
        assertEquals(2, enqueueResult.size());
        assertEquals("test-1", enqueueResult.get(0).getElement());
        assertEquals("test-2", enqueueResult.get(1).getElement());

        List<StringQueueElement> peekResult = workingQueue.peekUnprocessedElements(Integer.MAX_VALUE);
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

        List<StringQueueElement> enqueueResult = workingQueue.enqueueNewElements("test-1", "test-2");

        List<StringQueueElement> dequeueResult = workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false);
        assertEquals(2, dequeueResult.size());

        assertEquals("test-1", dequeueResult.get(0).getElement());
        assertEquals("test-2", dequeueResult.get(1).getElement());

        assertEquals(enqueueResult.get(0).getId(), dequeueResult.get(0).getId());
        assertEquals(enqueueResult.get(1).getId(), dequeueResult.get(1).getId());

        assertEquals(0, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();
    }

    @Test
    public void testEnqueueDequeueAndMarkProcessed() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = workingQueue.enqueueNewElements("test-1", "test-2");
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());
        workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false);
        assertEquals(2, workingQueue.markProcessed(ids).size());

        printState();
    }

    @Test
    public void testEnqueueAndSeveralDequeue() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = workingQueue.enqueueNewElements(Arrays.asList("test-1", "test-2", "test-3"));

        List<StringQueueElement> dequeueResult = workingQueue.dequeueForProcessing(1, false);
        assertEquals(1, dequeueResult.size());
        assertEquals(enqueueResult.get(0).getId(), dequeueResult.get(0).getId());

        List<StringQueueElement> secondDequeueResult = workingQueue.dequeueForProcessing(3, false);
        assertEquals(2, secondDequeueResult.size());
        assertEquals(enqueueResult.get(1).getId(), secondDequeueResult.get(0).getId());
        assertEquals(enqueueResult.get(2).getId(), secondDequeueResult.get(1).getId());

        printState();
    }

    @Test
    public void testFlush() throws Exception {
        printState();

        workingQueue.enqueueNewElements("test-1", "test-2");
        workingQueue.clean();
        assertEquals(0, workingQueue.peekUnprocessedElements(Integer.MAX_VALUE).size());
        assertEquals(0, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();
    }

    @Test
    public void testMarkFailed() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = workingQueue.enqueueNewElements("test-1", "test-2");
        printState();
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());
        workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false);
        printState();
        assertEquals(2, workingQueue.markFailed(ids).size());

        printState();
    }

    @Test
    public void testCollectGarbage() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = workingQueue.enqueueNewElements("test-1", "test-2");
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());

        assertEquals(2, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());
        assertEquals(2, workingQueue.markFailed(ids).size());

        assertEquals(2, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());
        assertEquals(2, workingQueue.markFailed(ids).size());

        assertEquals(2, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());
        assertEquals(2, workingQueue.markFailed(ids).size());

        assertEquals(0, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();

        List<StringQueueElement> garbage = workingQueue.removeFailedElements(m -> true, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(2, garbage.size());
        assertEquals(ids.get(0), garbage.get(0).getId());
        assertEquals(ids.get(1), garbage.get(1).getId());

        printState();
    }

    @Test
    public void testCleanup() throws Exception {
        printState();

        List<StringQueueElement> enqueueResult = workingQueue.enqueueNewElements("test-1", "test-2");
        List<String> ids = enqueueResult.stream().map(QueueElement::getId).collect(toList());

        assertEquals(2, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();

        List<StringQueueElement> cleanupResult = new ArrayList<>();
        workingQueue.requeueWorkingElements(m -> true, cleanupResult::add);
        assertEquals(2, cleanupResult.size());
        assertEquals(ids.get(0), cleanupResult.get(0).getId());
        assertEquals(ids.get(1), cleanupResult.get(1).getId());

        printState();

        assertEquals(2, workingQueue.dequeueForProcessing(Integer.MAX_VALUE, false).size());

        printState();
    }

    private void printState() {
        System.out.println("State:");
        new TreeMap<>(workingQueue.getState()).forEach((queueName, queueElements) -> {
            System.out.println(">>> " + queueName);
            queueElements.forEach(System.out::println);
        });
        System.out.println();
    }

}
