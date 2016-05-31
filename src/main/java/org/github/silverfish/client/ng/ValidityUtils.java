package org.github.silverfish.client.ng;

import java.util.List;

public class ValidityUtils {

    private ValidityUtils() {}

    public static void assureNotEmptyAndWithoutNulls(List<?> elements) {
        if (elements == null) {
            throw new NullPointerException("Expected not null collection");
        }
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Expected at least one element");
        }
        if (elements.stream().anyMatch(e -> e == null)) {
            throw new NullPointerException("Null elements not allowed: " + elements);
        }
    }

    public static void assurePositive(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Value should be greater than 0: " + value);
        }
    }

    public static void assureNonNegative(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Value should be greater or equal to 0: " + value);
        }
    }

    public static void assureNotNull(Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
    }
}
