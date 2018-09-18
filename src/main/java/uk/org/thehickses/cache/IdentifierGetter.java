package uk.org.thehickses.cache;

/**
 * An object that gets an identifier for an object.
 * 
 * @author Jeremy Hicks
 *
 * @param <I>
 *            the type of the identifier.
 * @param <V>
 *            the type of the object.
 */
@FunctionalInterface
public interface IdentifierGetter<I, V>
{
    /**
     * Gets the identifier that uniquely identifies the specified object.
     * 
     * @param object
     *            the object. May not be null.
     * @return the identifier. May not be null.
     */
    I getIdentifier(V object);
}
