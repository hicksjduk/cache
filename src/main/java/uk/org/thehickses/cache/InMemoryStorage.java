package uk.org.thehickses.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class InMemoryStorage<I, V> implements Storage<I, V>
{
    private final Map<I, V> data = new HashMap<>();

    @Override
    public V get(I identifier)
    {
        return data.get(identifier);
    }

    @Override
    public Stream<I> identifiers()
    {
        return data.keySet().stream();
    }

    @Override
    public void put(I identifier, V value)
    {
        data.put(identifier, value);
    }

    @Override
    public V remove(I identifier)
    {
        return data.remove(identifier);
    }

}
