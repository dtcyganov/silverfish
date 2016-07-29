package org.github.silverfish.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class StatsMetadataSupplier implements Supplier<Metadata> {

    public static final String PROCESS_COUNT = "process_count";
    public static final String BAIL_COUNT = "bail_count";
    public static final String TIME_CREATED = "time_created";
    public static final String TIME_ENQUEUED = "time_enqueued";

    @Override
    public Metadata get() {
        long millis = System.currentTimeMillis();
        Map<String, String> metadata = new HashMap<>();
        metadata.put(PROCESS_COUNT, "0");
        metadata.put(BAIL_COUNT, "0");
        metadata.put(TIME_CREATED, String.valueOf(millis));
        metadata.put(TIME_ENQUEUED, String.valueOf(millis));
        return new Metadata(metadata);
    }
}
