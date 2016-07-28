package org.github.silverfish.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class StatsMetadataSupplier implements Supplier<Metadata> {

    @Override
    public Metadata get() {
        long millis = System.currentTimeMillis();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("process_count", "0");
        metadata.put("bail_count", "0");
        metadata.put("time_created", String.valueOf(millis));
        metadata.put("time_enqueued", String.valueOf(millis));
        return new Metadata(metadata);
    }
}
