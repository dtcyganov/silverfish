package org.github.silverfish.client.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RedisQueueBackendTest {

    private RedisQueueBackend backend;
    private Process redisProcess;

    @Before
    public void setUp() throws IOException {
        redisProcess = new ProcessBuilder("redis-server",  "--port", "6389").start();
        backend = new RedisQueueBackend(
                new UUIDSupplier(),
                new StatsMetadataSupplier(),
                "localhost", 6389,
                "test");
    }

    @After
    public void tearDown() throws Exception {
        backend.cleanup();
        redisProcess.destroy();
    }

    @Test
    public void testEnqueDequeue() throws Exception {
        List<RedisQueueElement> elements1 = backend.enqueue(Arrays.asList("test-1", "test-2", "test-3"));
        System.out.println("Elements 1:");
        elements1.forEach(System.out::println);

        List<RedisQueueElement> elements2 = backend.dequeue(1, false);
        System.out.println("Elements 2:");
        elements2.forEach(System.out::println);

        List<RedisQueueElement> elements3 = backend.dequeue(3, false);
        System.out.println("Elements 3:");
        elements3.forEach(System.out::println);
    }

}
