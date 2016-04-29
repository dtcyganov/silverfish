package org.github.silverfish.client;

import java.io.*;
import java.util.function.Function;

public class Serializers {

    private Serializers() {}

    public static <E> Function<E, byte[]> createPlainJavaSerializer() {
        return input -> {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream out = new ObjectOutputStream(bos)) {
                out.writeObject(input);
                return bos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <E> Function<byte[], E> createPlainJavaDeserializer() {
        return data -> {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                 ObjectInputStream in = new ObjectInputStream(bis)) {
                return (E) in.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
