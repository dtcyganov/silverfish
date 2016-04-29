package org.github.silverfish.client.impl;

import java.util.UUID;
import java.util.function.Supplier;

public class UUIDSupplier implements Supplier<String> {

    @Override
    public String get() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
