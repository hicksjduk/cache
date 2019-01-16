package uk.org.thehickses.cache;

import java.util.stream.Stream;

/**
 * The interface that must be implemented by the underlying storage of a {@link Datastore}.
 * 
 * @author Jeremy Hicks
 *
 * @param <I>
 *            the type of the identifiers which uniquely identify objects in the storage.
 * @param <V>
 *            the type of the objects in the storage. May be a supertype, allowing objects of different but related
 *            types to be stored.
 */
public interface Storage<I, V>
{
    /**
     * Gets the object, if any, that is identified by the specified identifier.
     * 
     * @param identifier
     *            the identifier. May not be null.
     * @return the object identified by the identifier, or null if there is none.
     */
    V get(I identifier);

    /**
     * The identifiers in the storage.
     * 
     * @return the identifiers. May be empty, but may not be null.
     */
    Stream<I> identifiers();

    /**
     * Puts the specified value in the store, with the specified identifier. If another value already exists with the
     * same identifier, that value is removed.
     * 
     * @param identifier
     *            the identifier. May not be null.
     * @param value
     *            the value. May not be null.
     */
    void put(I identifier, V value);

    /**
     * Removes the value, if any, that is identified by the specified identifier.
     * 
     * @param identifier
     *            the identifier.
     * @return the value removed, or null if no value was identified by the identifier.
     */
    V remove(I identifier);
}
