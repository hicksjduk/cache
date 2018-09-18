package uk.org.thehickses.cache;

import java.util.stream.Stream;

public interface Storage<I, V>
{
    V get(I identifier);
    Stream<I> identifiers();
    void put(I identifier, V value);
    V remove(I identifier);
}
