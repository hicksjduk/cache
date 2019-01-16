package uk.org.thehickses.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class defining the functionality and interfaces of a datastore. The store is parameterised with the type of the
 * objects stored in it, and the type of their identifiers.
 * <p>
 * VERY IMPORTANT NOTE! This implementation relies on the stored objects being immutable, or at least their keys not
 * changing while they are in the store.
 * <p>
 * A datastore must be initialised with a backing {@link Storage} implementation, and an {@link IdentifierGetter} which
 * knows how to derive the identifier of an object from the object. It may also optionally be initialised with any of:
 * <ul>
 * <li>A {@link ChangeProcessor}, which is invoked after an object is added, changed or deleted.
 * <li>A {@link ValueNormaliser}, which is invoked before an object is added or changed, and which can be used to
 * normalise values.
 * <li>An {@link AdditionValidator}, which is invoked before an object is added or changed, and which can reject the
 * change by throwing an {@link InvalidAdditionException}.
 * </ul>
 * <p>
 * A datastore can also have any number of indices, each of which is initialised by calling one of the variants of the
 * {@link #index} method.
 * 
 * @author Jeremy Hicks
 *
 * @param <I>
 *            the type of the identifiers which uniquely identify objects in the store.
 * @param <V>
 *            the type of the objects in the store. May be a supertype, allowing objects of different but related types
 *            to be stored.
 */
public class Datastore<I, V>
{
    private final static Logger LOG = LoggerFactory.getLogger(Datastore.class);

    private final Storage<I, V> storage;
    private final IdentifierGetter<I, V> identifierGetter;
    private final ChangeProcessor<V> changeProcessor;
    private final ValueNormaliser<V> valueNormaliser;
    private final AdditionValidator<I, V> additionValidator;
    private final Set<Index<?, I, ? extends V>> indices = new HashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    /**
     * Initialises the datastore with the specified storage supplier and identifier getter, and default values for the
     * other parameters.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), ValueNormaliser.noOp(),
                AdditionValidator.noOp());
    }

    /**
     * Initialises the datastore with the specified storage supplier, identifier getter and change processor, and
     * default values for the other parameters.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     * @param changeProcessor
     *            an object that should be called to process any change to the contents of the datastore. May not be
     *            null.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ChangeProcessor<V> changeProcessor)
    {
        this(storage, identifierGetter, changeProcessor, ValueNormaliser.noOp(),
                AdditionValidator.noOp());
    }

    /**
     * Initialises the datastore with the specified storage supplier, identifier getter and value normaliser, and
     * default values for the other parameters.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     * @param valueNormaliser
     *            an object that should be called to normalise an object before it is added or updated in the datastore.
     *            May not be null, or return a null value for any object.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ValueNormaliser<V> valueNormaliser)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), valueNormaliser,
                AdditionValidator.noOp());
    }

    /**
     * Initialises the datastore with the specified storage supplier, identifier getter and addition validator, and
     * default values for the other parameters.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     * @param additionValidator
     *            an object that should be called to validate the addition or update of an object in the datastore, and
     *            which may veto the operation by throwing an {@link InvalidAdditionException}. May not be null.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            AdditionValidator<I, V> additionValidator)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), ValueNormaliser.noOp(),
                additionValidator);
    }

    /**
     * Initialises the datastore with the specified storage supplier, identifier getter, change processor and addition
     * validator, and default values for the other parameters.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     * @param changeProcessor
     *            an object that should be called to process any change to the contents of the datastore. May not be
     *            null.
     * @param additionValidator
     *            an object that should be called to validate the addition or update of an object in the datastore, and
     *            which may veto the operation by throwing an {@link InvalidAdditionException}. May not be null.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ChangeProcessor<V> changeProcessor, AdditionValidator<I, V> additionValidator)
    {
        this(storage, identifierGetter, changeProcessor, ValueNormaliser.noOp(), additionValidator);
    }

    /**
     * Initialises the datastore with the specified storage supplier, identifier getter, value normaliser and addition
     * validator, and default values for the other parameters.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     * @param valueNormaliser
     *            an object that should be called to normalise an object before it is added or updated in the datastore.
     *            May not be null, or return a null value for any object.
     * @param additionValidator
     *            an object that should be called to validate the addition or update of an object in the datastore, and
     *            which may veto the operation by throwing an {@link InvalidAdditionException}. May not be null.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ValueNormaliser<V> valueNormaliser, AdditionValidator<I, V> additionValidator)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), valueNormaliser, additionValidator);
    }

    /**
     * Initialises the datastore with the specified storage supplier, identifier getter, change processor and value
     * normaliser, and default values for the other parameters.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     * @param changeProcessor
     *            an object that should be called to process any change to the contents of the datastore. May not be
     *            null.
     * @param valueNormaliser
     *            an object that should be called to normalise an object before it is added or updated in the datastore.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ChangeProcessor<V> changeProcessor, ValueNormaliser<V> valueNormaliser)
    {
        this(storage, identifierGetter, changeProcessor, valueNormaliser, AdditionValidator.noOp());
    }

    /**
     * Initialises the datastore with the specified storage supplier, identifier getter, change processor, value
     * normaliser and addition validator.
     * 
     * @param storage
     *            a supplier which when called returns a {@link Storage} implementation. May not be null, or return a
     *            null value.
     * @param identifierGetter
     *            an object that knows how to get the identifier of an object. May not be null, or return a null value
     *            for any object.
     * @param changeProcessor
     *            an object that should be called to process any change to the contents of the datastore. May not be
     *            null.
     * @param valueNormaliser
     *            an object that should be called to normalise an object before it is added or updated in the datastore.
     *            May not be null, or return a null value for any object.
     * @param additionValidator
     *            an object that should be called to validate the addition or update of an object in the datastore, and
     *            which may veto the operation by throwing an {@link InvalidAdditionException}. May not be null.
     */
    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ChangeProcessor<V> changeProcessor, ValueNormaliser<V> valueNormaliser,
            AdditionValidator<I, V> additionValidator)
    {
        Stream
                .of(storage, identifierGetter, changeProcessor, valueNormaliser, additionValidator)
                .forEach(Objects::requireNonNull);
        this.storage = Objects.requireNonNull(storage.get());
        this.identifierGetter = obj -> Objects.requireNonNull(identifierGetter.getIdentifier(obj));
        this.changeProcessor = changeProcessor;
        this.valueNormaliser = obj -> Objects.requireNonNull(valueNormaliser.normalise(obj));
        this.additionValidator = additionValidator;
    }

    /**
     * Creates an index into the datastore.
     * 
     * @param keyGetter
     *            an object that knows how to derive a single index key from an object of the type supported by the
     *            index. May not be null, but may return null for a given object.
     * @return the index.
     */
    @SuppressWarnings("unchecked")
    public <K> Index<K, I, V> index(KeyGetter<K, ? super V> keyGetter)
    {
        Objects.requireNonNull(keyGetter);
        return index(obj -> (V) obj, keyGetter.toKeysGetter());
    }

    /**
     * Creates an index into the datastore.
     * 
     * @param keysGetter
     *            an object that knows how to derive a stream of index keys from an object of the type supported by the
     *            index. May not be null or return null, although it may return an empty stream.
     * @return the index.
     */
    @SuppressWarnings("unchecked")
    public <K> Index<K, I, V> index(KeysGetter<K, ? super V> keysGetter)
    {
        Objects.requireNonNull(keysGetter);
        return index(obj -> (V) obj, keysGetter);
    }

    /**
     * Creates an index into the datastore.
     * 
     * @param objectType
     *            the type of the objects that can be indexed. This can be a sub-type of the type supported by the
     *            datastore; objects that are not of the specified type (or its sub-types) are ignored by this index.
     * @param keyGetter
     *            an object that knows how to derive a single index key from an object of the type supported by the
     *            index. May not be null, but may return null for a given object.
     * @return the index.
     */
    public <K, U extends V> Index<K, I, U> index(Class<U> objectType,
            KeyGetter<K, ? super U> keyGetter)
    {
        Stream.of(objectType, keyGetter).forEach(Objects::requireNonNull);
        return index(Index.caster(objectType), keyGetter.toKeysGetter());
    }

    /**
     * Creates an index into the datastore.
     * 
     * @param objectType
     *            the type of the objects that can be indexed. This can be a sub-type of the type supported by the
     *            datastore; objects that are not of the specified type (or its sub-types) are ignored by this index.
     * @param keysGetter
     *            an object that knows how to derive a stream of index keys from an object of the type supported by the
     *            index. May not be null or return null, although it may return an empty stream.
     * @return the index.
     */
    public <K, U extends V> Index<K, I, U> index(Class<U> objectType,
            KeysGetter<K, ? super U> keysGetter)
    {
        Stream.of(objectType, keysGetter).forEach(Objects::requireNonNull);
        return index(Index.caster(objectType), keysGetter);
    }

    private <K, U extends V> Index<K, I, U> index(Function<Object, U> caster,
            KeysGetter<K, ? super U> keysGetter)
    {
        Index<K, I, U> index = new Index<>(storage, caster, identifierGetter, keysGetter, lock);
        doWithLock(lock.writeLock(), () -> addIndex(index));
        return index;

    }

    private <K, U extends V> void addIndex(Index<K, I, U> index)
    {
        indices.add(index);
        storage.identifiers().forEach(i -> index.add(i, storage.get(i)));
    }

    /**
     * Gets the stored object which has the specified identifier.
     * 
     * @param identifier
     *            the identifier. May not be null.
     * @return the object, or null if the store contains no object with the identifier in question.
     */
    public V get(I identifier)
    {
        Objects.requireNonNull(identifier);
        return doWithLock(lock.readLock(), () -> storage.get(identifier));
    }

    /**
     * Adds the specified object(s) to the store. If any has the same identifier as an existing stored object, the new
     * one replaces the existing one.
     * 
     * @param objects
     *            the object(s). May be empty, but may not be null. If it contains any null objects, they are ignored.
     */
    @SafeVarargs
    public final void add(V... objects)
    {
        add(Stream.of(Objects.requireNonNull(objects)));
    }

    /**
     * Adds the specified object(s) to the store. If any has the same identifier as an existing stored object, the new
     * one replaces the existing one.
     * 
     * @param objects
     *            the object(s). May be empty, but may not be null. If it contains any null objects, they are ignored.
     */
    public void add(Collection<V> objects)
    {
        add(Objects.requireNonNull(objects).stream());
    }

    private void add(Stream<V> objects)
    {
        doWithLock(lock.writeLock(), adder(objects)).forEach(Result::process);
    }

    /**
     * Adds the specified object(s) to the store, replacing any existing contents. Any object in the store which has the
     * same identifier as an input object is replaced by the new one; all other objects in the store are removed.
     * 
     * @param objects
     *            the object(s). May be empty, but may not be null. If it contains any null objects, they are ignored.
     */
    @SafeVarargs
    public final void addReplace(V... objects)
    {
        addReplace(Stream.of(Objects.requireNonNull(objects)));
    }

    /**
     * Adds the specified object(s) to the store, replacing any existing contents. Any object in the store which has the
     * same identifier as an input object is replaced by the new one; all other objects in the store are removed.
     * 
     * @param objects
     *            the object(s). May be empty, but may not be null. If it contains any null objects, they are ignored.
     */
    public void addReplace(Collection<V> objects)
    {
        addReplace(Objects.requireNonNull(objects).stream());
    }

    private void addReplace(Stream<V> objects)
    {
        doWithLock(lock.writeLock(), addReplacer(objects)).forEach(Result::process);
    }

    private Supplier<Stream<Result>> adder(Stream<V> newObjects)
    {
        Stream<Supplier<Result>> transactions = newObjects
                .filter(Objects::nonNull)
                .map(this::adder);
        return () -> transactions.map(Supplier::get);
    }

    private Supplier<Stream<Result>> addReplacer(Stream<V> newObjects)
    {
        Map<I, Supplier<Result>> transactionsByKey = newObjects.filter(Objects::nonNull).collect(
                Collectors.toMap(identifierGetter::getIdentifier, this::adder));
        return () -> {
            storage.identifiers().filter(k -> !transactionsByKey.containsKey(k)).forEach(
                    k -> transactionsByKey.put(k, remover(k)));
            return transactionsByKey.values().stream().map(Supplier::get);
        };
    }

    private void processResult(AddResult result)
    {
        if (result.exception != null)
        {
            LOG.error("Invalid attempt to add object {} (with identifier {})", result.newValue,
                    result.identifier, result.exception);
            return;
        }
        if (result.oldValue == null)
            LOG.debug("New object {} added with identifier {}", result.newValue, result.identifier);
        else
            LOG.debug("Existing object {} replaced by new object {} with identifier {}",
                    result.oldValue, result.newValue, result.identifier);
        if (!Objects.equals(result.oldValue, result.newValue))
            changeProcessor.processChange(result.oldValue, result.newValue);
    }

    /**
     * Clears the datastore, by deleting all objects it contains.
     */
    public void clear()
    {
        addReplace();
    }

    /**
     * Removes the object(s) with the specified identifier(s) from the store.
     * 
     * @param identifiers
     *            the identifier(s). May be empty, but may not be null. If it contains any null objects, they are
     *            ignored. If any identifier is not that of an object in the store, it has no effect.
     */
    @SafeVarargs
    public final void remove(I... identifiers)
    {
        remove(Stream.of(Objects.requireNonNull(identifiers)));
    }

    /**
     * Removes the object(s) with the specified identifier(s) from the store.
     * 
     * @param identifiers
     *            the identifier(s). May be empty, but may not be null. If it contains any null objects, they are
     *            ignored. If any identifier is not that of an object in the store, it has no effect.
     */
    public void remove(Collection<I> identifiers)
    {
        remove(Objects.requireNonNull(identifiers).stream());
    }

    private void remove(Stream<I> identifiers)
    {
        doWithLock(lock.writeLock(), remover(identifiers)).forEach(Result::process);
    }

    private void processResult(RemoveResult result)
    {
        if (result.oldValue == null)
        {
            LOG.debug("No object is stored with identifier {}, nothing removed", result.identifier);
            return;
        }
        LOG.debug("Object {} with identifier {} removed", result.oldValue, result.identifier);
        changeProcessor.processChange(result.oldValue, null);
    }

    private void updateIndices(V oldValue, V newValue)
    {
        indices.stream().forEach(i -> i.update(oldValue, newValue));
    }

    private Supplier<Result> adder(V value)
    {
        return () -> add(value);
    }

    private Result add(V value)
    {
        V newValue = valueNormaliser.normalise(value);
        I identifier = identifierGetter.getIdentifier(newValue);
        V oldValue = storage.get(identifier);
        try
        {
            additionValidator.validate(identifier, oldValue, newValue);
        }
        catch (InvalidAdditionException ex)
        {
            return new AddResult(identifier, oldValue, newValue, ex);
        }
        storage.put(identifier, newValue);
        updateIndices(oldValue, newValue);
        return new AddResult(identifier, oldValue, newValue);
    }

    private Supplier<Result> remover(I identifier)
    {
        return () -> remove(identifier);
    }

    private Result remove(I identifier)
    {
        V oldValue = storage.remove(identifier);
        if (oldValue != null)
            updateIndices(oldValue, null);
        return new RemoveResult(identifier, oldValue);
    }

    private Supplier<Stream<Result>> remover(Stream<I> identifiers)
    {
        Stream<Supplier<Result>> removers = identifiers.filter(Objects::nonNull).map(this::remover);
        return () -> removers.map(Supplier::get);
    }

    private static <T> T doWithLock(Lock lock, Supplier<T> processor)
    {
        lock.lock();
        try
        {
            return processor.get();
        }
        finally
        {
            lock.unlock();
        }
    }

    private static void doWithLock(Lock lock, Runnable processor)
    {
        doWithLock(lock, () -> {
            processor.run();
            return null;
        });
    }

    /**
     * The interface that must be implemented by the underlying storage of a {@link Datastore}.
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

    /**
     * An object that gets an identifier for an object.
     * 
     * <p>This is a functional interface whose functional method is {@link #getIdentifier(Object)}.
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

    /**
     * An interface defining a change processor, which is called whenever the store changes.
     *
     * <p>This is a functional interface whose functional method is {@link #processChange(Object, Object)}.
     *
     * @param <V>
     *            the type of the objects processed.
     */
    @FunctionalInterface
    public static interface ChangeProcessor<V>
    {
        static <V> ChangeProcessor<V> noOp()
        {
            return (oldValue, newValue) -> {
            };
        }

        /**
         * Processes a notification that the store has changed. The change may be:
         * <ul>
         * <li>an add ({@code oldValue} is null, {@code newValue} is not)
         * <li>a replace (both {@code oldValue} and {@code newValue} are non-null)
         * <li>a delete ({@code newValue} is null, {@code oldValue} is not)
         * </ul>
         * 
         * @param oldValue
         *            the object that was removed from the store, or null if none was.
         * @param newValue
         *            the object that was added to the store, or null if none was.
         */
        void processChange(V oldValue, V newValue);
    }

    /**
     * An interface defining a value normaliser, which is called to normalise any value before it is added to the store.
     *
     * <p>This is a functional interface whose functional method is {@link #normalise(Object)}.
     *
     * @param <V>
     *            the type of the objects processed.
     */
    @FunctionalInterface
    public static interface ValueNormaliser<V>
    {
        static <V> ValueNormaliser<V> noOp()
        {
            return value -> value;
        }

        /**
         * Normalises, if necessary, the specified value.
         * 
         * @param value
         *            the value. May not be null.
         * @return the same or a different value. May not be null.
         */
        V normalise(V value);
    }

    /**
     * An interface defining an addition validator, which is called to validate the addition of an object before it is
     * added.
     * 
     * <p>This is a functional interface whose functional method is {@link #validate(Object, Object, Object)}.
     *
     * @param <I>
     *            the type of the identifier which identifies the objects processed.
     * @param <V>
     *            the type of the objects processed.
     */
    @FunctionalInterface
    public static interface AdditionValidator<I, V>
    {
        static <I, V> AdditionValidator<I, V> noOp()
        {
            return (identifier, oldValue, newValue) -> {
            };
        }

        /**
         * Validates the addition of the specified new value, which has the specified identifier and replaces the
         * specified old value if that is not null.
         * 
         * @param identifier
         *            the identifier. May not be null.
         * @param oldValue
         *            the old value being replaced. May be null, if there is no existing value with the specified
         *            identifier.
         * @param newValue
         *            the new value being added. May not be null.
         * @throws InvalidAdditionException
         *             if the addition is unacceptable for any reason.
         */
        void validate(I identifier, V oldValue, V newValue) throws InvalidAdditionException;

    }

    @SuppressWarnings("serial")
    public static class InvalidAdditionException extends Exception
    {
        public InvalidAdditionException()
        {
            super();
        }

        public InvalidAdditionException(String message, Throwable ex)
        {
            super(message, ex);
        }

        public InvalidAdditionException(String message)
        {
            super(message);
        }

        public InvalidAdditionException(Throwable ex)
        {
            super(ex);
        }
    }

    private interface Result
    {
        void process();
    }

    private class AddResult implements Result
    {
        public final I identifier;
        public final V oldValue;
        public final V newValue;
        public final InvalidAdditionException exception;

        public AddResult(I identifier, V oldValue, V newValue)
        {
            this(identifier, oldValue, newValue, null);
        }

        public AddResult(I identifier, V oldValue, V newValue, InvalidAdditionException exception)
        {
            this.identifier = identifier;
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.exception = exception;
        }

        @Override
        public void process()
        {
            processResult(this);
        }
    }

    private class RemoveResult implements Result
    {
        public final I identifier;
        public final V oldValue;

        public RemoveResult(I identifier, V oldValue)
        {
            this.identifier = identifier;
            this.oldValue = oldValue;
        }

        @Override
        public void process()
        {
            processResult(this);
        }
    }

    /**
     * An index into a datastore.
     * <p>
     * Note that this implementation relies on the stored objects being immutable, or at least their identifiers and
     * index keys not being changed while they are in the datastore.
     * <p>
     * Instances of this class are not created directly, but by calling one of the variants of the
     * {@link Datastore#index} method of the datastore on which the index is to be created.
     * 
     * @author Jeremy Hicks
     * 
     * @param <K>
     *            the type of the keys of this index.
     * @param <I>
     *            the type of the identifiers of the indexed objects.
     * @param <V>
     *            the type of the indexed objects.
     */
    public static class Index<K, I, V>
    {
        private final Storage<I, ? super V> storage;

        private final Function<Object, V> caster;

        private final Map<K, Set<I>> identifiersByKey = new HashMap<>();

        private final IdentifierGetter<I, V> identifierGetter;

        private final KeysGetter<K, ? super V> keysGetter;

        private final ReadWriteLock lock;

        private Index(Storage<I, ? super V> storage, Function<Object, V> caster,
                IdentifierGetter<I, ? super V> identifierGetter,
                KeysGetter<K, ? super V> keysGetter, ReadWriteLock lock)
        {
            this.storage = storage;
            this.caster = caster;
            this.identifierGetter = obj -> Objects
                    .requireNonNull(identifierGetter.getIdentifier(obj));
            this.keysGetter = obj -> Objects
                    .requireNonNull(keysGetter.getKeys(obj))
                    .filter(Objects::nonNull);
            this.lock = lock;
        }

        /**
         * Gets the keys, if any, which index objects in the index.
         * 
         * @return the keys. May be empty, but may not be null.
         */
        public Stream<K> getKeys()
        {
            Stream.Builder<K> builder = Stream.builder();
            doWithLock(lock.readLock(), () -> {
                identifiersByKey.keySet().stream().forEach(builder);
            });
            return builder.build();
        }

        /**
         * Gets the identifiers of the objects, if any, that are associated with the specified key.
         * 
         * @param key
         *            the key. May not be null.
         * @return the associated identifiers, if there are any. May be empty, but may not be null.
         */
        public Stream<I> getIdentifiers(K key)
        {
            Objects.requireNonNull(key);
            Stream.Builder<I> builder = Stream.builder();
            doWithLock(lock.readLock(), () -> {
                Set<I> idsForKey = identifiersByKey.get(key);
                if (idsForKey != null)
                    idsForKey.stream().forEach(builder);
            });
            return builder.build();
        }

        /**
         * Gets the objects, if any, that are associated with the specified key.
         * 
         * @param key
         *            the key. May not be null.
         * @return the associated objects, if there are any. May be empty, but is never null.
         */
        public Stream<V> getObjects(K key)
        {
            return doWithLock(lock.readLock(), () -> getIdentifiers(key).map(storage::get))
                    .map(caster::apply);
        }

        private void add(Object object)
        {
            V castObject = caster.apply(object);
            if (castObject != null)
                addObject(identifierGetter.getIdentifier(castObject), castObject);
        }

        private void add(I identifier, Object object)
        {
            V castObject = caster.apply(object);
            if (castObject != null)
                addObject(identifier, castObject);
        }

        private void addObject(I identifier, V object)
        {
            Stream<K> keys = keysGetter.getKeys(object);
            doWithLock(lock.writeLock(), () -> {
                keys.forEach(key -> {
                    Set<I> ids = identifiersByKey.get(key);
                    if (ids == null)
                        identifiersByKey.put(key, ids = new HashSet<>());
                    ids.add(identifier);
                });
            });
        }

        private void remove(Object object)
        {
            V castObject = caster.apply(object);
            if (castObject == null)
                return;
            I identifier = identifierGetter.getIdentifier(castObject);
            Stream<K> keys = keysGetter.getKeys(castObject);
            doWithLock(lock.writeLock(), () -> {
                keys.forEach(key -> {
                    Set<I> objects = identifiersByKey.get(key);
                    if (objects != null && objects.remove(identifier) && objects.isEmpty())
                        identifiersByKey.remove(key);
                });
            });
        }

        private void update(Object oldValue, Object newValue)
        {
            if (oldValue != null)
                remove(oldValue);
            if (newValue != null)
                add(newValue);
        }

        private static <V> Function<Object, V> caster(Class<V> objectType)
        {
            return obj -> {
                if (objectType.isAssignableFrom(obj.getClass()))
                    return objectType.cast(obj);
                return null;
            };
        }
    }

    /**
     * An interface defining an object that can get a single index key from an object.
     *
     * <p>This is a functional interface whose functional method is {@link #getKey(Object)}.
     *
     * @param <K>
     *            the type of the key.
     * @param <V>
     *            the type of the object.
     */
    @FunctionalInterface
    public static interface KeyGetter<K, V>
    {
        /**
         * Gets the index key associated with the specified object.
         * 
         * @param object
         *            the object. May not be null.
         * @return the key. May be null, which denotes that the object has no key in this index and should therefore be
         *         excluded.
         */
        K getKey(V value);

        default KeysGetter<K, V> toKeysGetter()
        {
            return v -> Stream.of(getKey(v)).filter(Objects::nonNull);
        }
    }

    /**
     * An interface defining an object that can get multiple index keys from a single object.
     *
     * <p>This is a functional interface whose functional method is {@link #getKeys(Object)}.
     *
     * @param <K>
     *            the type of the keys.
     * @param <V>
     *            the type of the object.
     */
    @FunctionalInterface
    public static interface KeysGetter<K, V>
    {
        /**
         * Gets the index key(s) associated with the specified object.
         * 
         * @param object
         *            the object. May not be null.
         * @return the key(s). May be empty (which denotes that the object has no key in this index and should therefore
         *         be excluded), but may not be null. Null keys are not supported, so if the stream contains nulls they
         *         are ignored.
         */
        Stream<K> getKeys(V object);
    }

}
