package uk.org.thehickses.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import uk.org.thehickses.cache.Datastore.Storage;

/**
 * An implementation of the {@link Storage} interface which uses a {@link Map} as its underlying store.
 * 
 * @author Jeremy Hicks
 *
 * @param <I>
 *            the type of the identifiers which uniquely identify objects in the store.
 * @param <V>
 *            the type of the objects in the store. May be a supertype, allowing objects of different but related types
 *            to be stored.
 */
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
