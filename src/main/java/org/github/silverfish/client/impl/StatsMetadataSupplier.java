package org.github.silverfish.client.impl;

import org.github.silverfish.client.ng.Metadata;

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
        // double values used here for compatibility with perl implementation
        metadata.put("time_created", String.valueOf(millis / 1000.));
        metadata.put("time_enqueued", String.valueOf(millis / 1000.));
        return new Metadata(metadata);
    }
}
