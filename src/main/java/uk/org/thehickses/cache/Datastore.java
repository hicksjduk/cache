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
 * objects stored in it and the type of their identifiers.
 * <p>
 * VERY IMPORTANT NOTE! This implementation relies on the stored objects being immutable, or at least their keys not
 * changing while they are in the store.
 * <p>
 * A datastore must be initialised with an {@link IdentifierGetter}, which knows how to derive the identifier of an
 * object from the object. It may also optionally be initialised with any of:
 * <ul>
 * <li>A {@link ChangeProcessor}, which is invoked whenever an object is added, changed or deleted.
 * <li>A {@link ValueNormaliser}, which is invoked whenever an object is added or changed, and which can be used to
 * normalise values.
 * <li>An {@link AdditionValidator}, which is invoked whenever an object is added or changed, and which can reject the
 * change by throwing an {@link InvalidAdditionException}.
 * </ul>
 * <p>
 * A datastore can also have any number of indices.
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

    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), ValueNormaliser.noOp(),
                AdditionValidator.noOp());
    }

    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ChangeProcessor<V> changeProcessor)
    {
        this(storage, identifierGetter, changeProcessor, ValueNormaliser.noOp(),
                AdditionValidator.noOp());
    }

    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ValueNormaliser<V> valueNormaliser)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), valueNormaliser,
                AdditionValidator.noOp());
    }

    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            AdditionValidator<I, V> additionValidator)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), ValueNormaliser.noOp(),
                additionValidator);
    }

    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ChangeProcessor<V> changeProcessor, AdditionValidator<I, V> additionValidator)
    {
        this(storage, identifierGetter, changeProcessor, ValueNormaliser.noOp(), additionValidator);
    }

    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ValueNormaliser<V> valueNormaliser, AdditionValidator<I, V> additionValidator)
    {
        this(storage, identifierGetter, ChangeProcessor.noOp(), valueNormaliser, additionValidator);
    }

    public Datastore(Supplier<Storage<I, V>> storage, IdentifierGetter<I, V> identifierGetter,
            ChangeProcessor<V> changeProcessor, ValueNormaliser<V> valueNormaliser)
    {
        this(storage, identifierGetter, changeProcessor, valueNormaliser, AdditionValidator.noOp());
    }

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
     *            index.
     * @return the index.
     */
    public <K> Index<K, I, V> index(KeyGetter<K, ? super V> keyGetter)
    {
        Objects.requireNonNull(keyGetter);
        @SuppressWarnings("unchecked")
        Index<K, I, V> index = new Index<>(obj -> (V) obj, identifierGetter, keyGetter, lock);
        doWithLock(lock.writeLock(), () -> addIndex(index));
        return index;
    }

    /**
     * Creates an index into the datastore.
     * 
     * @param keyGetter
     *            an object that knows how to derive a stream of index keys from an object of the type supported by the
     *            index.
     * @return the index.
     */
    public <K> Index<K, I, V> index(KeysGetter<K, ? super V> keysGetter)
    {
        Objects.requireNonNull(keysGetter);
        @SuppressWarnings("unchecked")
        Index<K, I, V> index = new Index<>(obj -> (V) obj, identifierGetter, keysGetter,
                lock);
        doWithLock(lock.writeLock(), () -> addIndex(index));
        return index;
    }

    /**
     * Creates an index into the datastore.
     * 
     * @param objectType
     *            the type of the objects that can be indexed. This can be a sub-type of the type supported by the
     *            datastore; objects that are not of the specified type (or its sub-types) are ignored by this index.
     * @param keyGetter
     *            an object that knows how to derive a single index key from an object of the type supported by the
     *            index.
     * @return the index.
     */
    public <K, U extends V> Index<K, I, U> index(Class<U> objectType,
            KeyGetter<K, ? super U> keyGetter)
    {
        Stream.of(objectType, keyGetter).forEach(Objects::requireNonNull);
        Index<K, I, U> index = new Index<>(Index.caster(objectType), identifierGetter, keyGetter,
                lock);
        doWithLock(lock.writeLock(), () -> addIndex(index));
        return index;
    }

    /**
     * Creates an index into the datastore.
     * 
     * @param objectType
     *            the type of the objects that can be indexed. This can be a sub-type of the type supported by the
     *            datastore; objects that are not of the specified type (or its sub-types) are ignored by this index.
     * @param keyGetter
     *            an object that knows how to derive a stream of index keys from an object of the type supported by the
     *            index.
     * @return the index.
     */
    public <K, U extends V> Index<K, I, U> index(Class<U> objectType,
            KeysGetter<K, ? super U> keysGetter)
    {
        Stream.of(objectType, keysGetter).forEach(Objects::requireNonNull);
        Index<K, I, U> index = new Index<>(Index.caster(objectType), identifierGetter, keysGetter,
                lock);
        doWithLock(lock.writeLock(), () -> addIndex(index));
        return index;
    }

    /**
     * Adds the specified index to the store.
     * 
     * @param index
     *            the index. May not be null.
     */
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
        add(false, Stream.of(Objects.requireNonNull(objects)));
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
        add(false, Objects.requireNonNull(objects).stream());
    }

    /**
     * Adds the specified object(s) to the store, replacing any existing contents. Any object in the store which have
     * the same identifier as an input object is replaced by the new one; all other objects in the store are removed.
     * 
     * @param objects
     *            the object(s). May be empty, but may not be null. If it contains any null objects, they are ignored.
     */
    @SafeVarargs
    public final void addReplace(V... objects)
    {
        add(true, Stream.of(Objects.requireNonNull(objects)));
    }

    /**
     * Adds the specified object(s) to the store, replacing any existing contents. Any object in the store which have
     * the same identifier as an input object is replaced by the new one; all other objects in the store are removed.
     * 
     * @param objects
     *            the object(s). May be empty, but may not be null. If it contains any null objects, they are ignored.
     */
    public void addReplace(Collection<V> objects)
    {
        add(true, Objects.requireNonNull(objects).stream());
    }

    /**
     * Adds the object(s) in the specified stream to the store. This method should always be called by other "add"
     * methods, since it handles locking correctly.
     * 
     * @param replace
     *            whether the object(s) should replace the current contents of the store, in which case all existing
     *            objects in the store whose keys do not match those of any of the input objects are removed.
     * @param objects
     *            the stream containing the objects to add. May be empty, but may not be null. If it contains any null
     *            objects, they are ignored.
     */
    private void add(boolean replace, Stream<V> objects)
    {
        doWithLock(lock.writeLock(), adder(replace, objects)).forEach(Result::process);
    }

    /**
     * Gets an object which when its {@link Supplier#get()} method is called performs the specified add operation and
     * returns a stream of results.
     * 
     * @param replace
     *            whether the specified object(s) should replace the current contents of the store, in which case all
     *            existing objects in the store whose keys do not match those of any of the input objects are removed.
     * @param newObjects
     *            the object(s) to add. May be empty, but may not be null. If it contains any null objects, they are
     *            ignored.
     * @return the results of adding and/or removing objects. May be empty, but may not be null or contain any null
     *         objects.
     */
    private Supplier<Stream<Result>> adder(boolean replace, Stream<V> newObjects)
    {
        Map<I, Supplier<Result>> transactionsByKey = newObjects.filter(Objects::nonNull).collect(
                Collectors.toMap(identifierGetter::getIdentifier, this::adder));
        return () -> {
            if (replace)
                storage.identifiers().filter(k -> !transactionsByKey.containsKey(k)).forEach(
                        k -> transactionsByKey.put(k, remover(k)));
            return transactionsByKey.values().stream().map(Supplier::get);
        };
    }

    /**
     * Processes the result of adding an object to the store, by logging the outcome and notifying the change processor
     * if an actual change has taken place.
     * 
     * @param result
     *            the result. May not be null.
     */
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

    public void clear()
    {
        addReplace();
    }

    /**
     * Removes the object(s) with the specified identifier(s) from the store.
     * 
     * @param identifiers
     *            the identifier(s). May be empty, but may not be null. If it contains any null objects, they are
     *            ignored.
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
     *            ignored.
     */
    public void remove(Collection<I> identifiers)
    {
        remove(Objects.requireNonNull(identifiers).stream());
    }

    /**
     * Removes the object(s) whose identifier(s) are in the specified stream from the store. This method should always
     * be called by other "remove" methods, since it handles locking correctly.
     * 
     * @param identifiers
     *            the stream containing the identifiers to remove. May be empty, but may not be null. If it contains any
     *            null objects, they are ignored.
     */
    private void remove(Stream<I> identifiers)
    {
        doWithLock(lock.writeLock(), remover(identifiers)).forEach(Result::process);
    }

    /**
     * Processes the result of removing an object from the store, by logging the outcome and notifying the change
     * processor if one is registered and an actual change has taken place.
     * 
     * @param result
     *            the result. May not be null.
     */
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

    /**
     * Updates the store's indices to reflect the specified change. The change may be:
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
    private void updateIndices(V oldValue, V newValue)
    {
        indices.stream().forEach(i -> i.update(oldValue, newValue));
    }

    /**
     * Gets an "adder" object which attempts to add the specified object to the store.
     * 
     * @param value
     *            the object to add. May not be null.
     * @return an object which when its {@link Supplier.get()} method is called tries to add the object and returns an
     *         object that contains details of the outcome.
     */
    private Supplier<Result> adder(V value)
    {
        return () -> add(value);
    }

    /**
     * Attempts to add the specified object to the store. The value may be normalised, if a {@link ValueNormaliser} is
     * configured, and the addition may be rejected, if an {@link AdditionValidator} is configured. If the addition is
     * not rejected, the value is added to the store, replacing any existing object with the same identifier.
     * 
     * @param value
     *            the object. May not be null.
     * @return the result of the addition.
     */
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

    /**
     * Gets a "remover" object which attempts to remove the specified identifier from the store.
     * 
     * @param identifier
     *            the identifier to remove. May not be null.
     * @return an object which when its {@link Supplier.get()} method is called tries to remove the identifier and
     *         returns an object that contains details of the outcome.
     */
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

    /**
     * Gets a "remover" object which attempts to remove all the specified identifiers from the store.
     * 
     * @param identifiers
     *            the identifier(s) to remove. May be empty, but may not be null. If it contains any null objects, they
     *            are ignored.
     * @return an object which when its {@link Supplier.get()} method is called tries to remove the identifiers and
     *         returns a stream of objects containing details of the outcome of each removal.
     */
    private Supplier<Stream<Result>> remover(Stream<I> identifiers)
    {
        Stream<Supplier<Result>> removers = identifiers.filter(Objects::nonNull).map(this::remover);
        return () -> removers.map(Supplier::get);
    }

    /**
     * Invokes the specified object, with the specified lock locked.
     * 
     * @param lock
     *            the lock. May not be null. If the lock is unavailable because another thread has locked it, the method
     *            blocks until it becomes available.
     * @param processor
     *            the object which is to be invoked. May not be null.
     * @return the result of invoking the object. May be null.
     */
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

    /**
     * Invokes the specified object, with the specified lock locked.
     * 
     * @param lock
     *            the lock. May not be null. If the lock is unavailable because another thread has locked it, the method
     *            blocks until it becomes available.
     * @param processor
     *            the object which is to be invoked. May not be null.
     */
    private static void doWithLock(Lock lock, Runnable processor)
    {
        lock.lock();
        try
        {
            processor.run();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * An interface defining a change processor, which is called whenever the store changes.
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

    /**
     * The result of an operation that changes the store.
     */
    private interface Result
    {
        /**
         * Processes the operation.
         */
        void process();
    }

    /**
     * The result of a request to add an object to the store.
     */
    private class AddResult implements Result
    {
        /**
         * The identifier of the object whose addition was requested.
         */
        private final I identifier;
        /**
         * The object with the specified identifier that was in the store before the request was made. May be null, if
         * there was no object with that identifier. If the request failed validation, this object is still in the
         * store; if the request passed validation, it is no longer in the store and has been replaced by
         * {@link newValue}.
         */
        private final V oldValue;
        /**
         * The object whose addition was requested. If the request failed validation, this object was not added to the
         * store; if the request passed validation, this object was added.
         */
        private final V newValue;
        /**
         * If not null, the request failed validation and so no changes were made. If null, the requested change was
         * made.
         */
        private final InvalidAdditionException exception;

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

    /**
     * The result of a request to remove an identifier from the store.
     */
    private class RemoveResult implements Result
    {
        /**
         * The identifier whose removal was requested.
         */
        private final I identifier;
        /**
         * The object with the specified identifier that was in the store before the request was made. May be null, if
         * there was no existing object. This object is no longer in the store as a result of the request.
         */
        private final V oldValue;

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
     * The index is initialised with:
     * <ul>
     * <li>The type of the objects that can be indexed by the index. This allows different indices in the same datastore
     * to support different data types; an object that is not of a supported type is simply ignored by the index.
     * <li>An IdentifierGetter that knows how to get the identifier which uniquely identifies any indexed object. This
     * is the same as is used by the containing datastore.
     * <li>A way of getting the index key(s) for any indexed object. There are two alternative interfaces: KeyGetter
     * which supports only one index key per object, and KeysGetter which supports multiple keys.
     * </ul>
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
        private final Function<Object, V> caster;

        // NB because different objects can have the same index key, the objects for each key are stored in a
        // collection; and to support efficient removal that collection is a map where the key is the object's
        // identifier.
        protected final Map<K, Map<I, V>> objectsByKey = new HashMap<>();

        private final IdentifierGetter<I, V> identifierGetter;

        private final KeysGetter<K, ? super V> keysGetter;

        private final ReadWriteLock lock;

        /**
         * Initialises the index with the specified object type, identifier getter and (single) key getter.
         * 
         * @param objectType
         *            the object type. May not be null.
         * @param identifierGetter
         *            the identifier getter. May not be null.
         * @param keyGetter
         *            the key getter. May not be null.
         */
        private Index(Function<Object, V> caster, IdentifierGetter<I, ? super V> identifierGetter,
                KeyGetter<K, ? super V> keyGetter, ReadWriteLock lock)
        {
            this(caster, identifierGetter, (KeysGetter<K, V>) v -> Stream.of(keyGetter.getKey(v)),
                    lock);
        }

        /**
         * Initialises the index with the specified object type, identifier getter and (multiple) key getter.
         * 
         * @param objectType
         *            the object type. May not be null.
         * @param identifierGetter
         *            the identifier getter. May not be null.
         * @param keysGetter
         *            the key getter. May not be null.
         */
        private Index(Function<Object, V> caster, IdentifierGetter<I, ? super V> identifierGetter,
                KeysGetter<K, ? super V> keysGetter, ReadWriteLock lock)
        {
            this.caster = caster;
            this.identifierGetter = obj -> Objects
                    .requireNonNull(identifierGetter.getIdentifier(obj));
            this.keysGetter = obj -> Objects
                    .requireNonNull(keysGetter.getKeys(obj))
                    .filter(Objects::nonNull);
            this.lock = lock;
        }

        /**
         * Gets the identifiers of the objects, if any, that are associated with the specified key.
         * 
         * @param key
         *            the key. May not be null.
         * @return the associated identifiers, if there are any. May be empty, but is never null.
         */
        public Stream<I> getIdentifiers(K key)
        {
            return get(key, Map::keySet);
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
            return get(key, Map::values);
        }

        private <T> Stream<T> get(K key, Function<Map<I, V>, Iterable<T>> getter)
        {
            Objects.requireNonNull(key);
            Stream.Builder<T> builder = Stream.builder();
            doWithLock(lock.readLock(), () -> {
                Map<I, V> objsForKey = objectsByKey.get(key);
                if (objsForKey != null)
                    getter.apply(objsForKey).forEach(builder);
            });
            return builder.build();
        }

        /**
         * Adds the specified object to the index, if it is of the supported type.
         * 
         * @param object
         *            the object. May not be null.
         */
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
                    Map<I, V> objects = objectsByKey.get(key);
                    if (objects == null)
                        objectsByKey.put(key, objects = new HashMap<>());
                    objects.put(identifier, object);
                });
            });
        }

        /**
         * Removes the specified object from the index, if it is of the supported type.
         * 
         * @param object
         *            the object. May not be null.
         */
        private void remove(Object object)
        {
            V castObject = caster.apply(object);
            if (castObject == null)
                return;
            I identifier = identifierGetter.getIdentifier(castObject);
            Stream<K> keys = keysGetter.getKeys(castObject);
            doWithLock(lock.writeLock(), () -> {
                keys.forEach(key -> {
                    Map<I, V> objects = objectsByKey.get(key);
                    if (objects != null && objects.remove(identifier) != null && objects.isEmpty())
                        objectsByKey.remove(key);
                });
            });
        }

        /**
         * Updates the index to reflect the specified change. The change may be:
         * <ul>
         * <li>an add (oldValue is null, newValue is not)
         * <li>a replace (both oldValue and newValue are non-null)
         * <li>a delete (newValue is null, oldValue is not)
         * </ul>
         * 
         * @param oldValue
         *            the object that was removed from the store, or null if none was.
         * @param newValue
         *            the object that was added to the store, or null if none was.
         */
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
    }

    /**
     * An interface defining an object that can get multiple index keys from a single object.
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
