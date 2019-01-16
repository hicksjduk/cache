package uk.org.thehickses.cache;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.org.thehickses.cache.Datastore.ChangeProcessor;
import uk.org.thehickses.cache.Datastore.Index;

public class DatastoreTest
{
    private TestDatastore datastore;
    private TestStorage storage;
    private ChangeProcessor<StoredObject> changeProcessor;

    @SuppressWarnings("unchecked")
    @Before
    public void setup()
    {
        IdentifierGetter<String, StoredObject> identifierGetter = StoredObject::getKey;
        storage = new TestStorage();
        changeProcessor = mock(ChangeProcessor.class);
        datastore = new TestDatastore(storage, identifierGetter, changeProcessor);
    }

    @After
    public void tearDown()
    {
        verifyNoMoreInteractions(changeProcessor);
    }

    @Test
    public void testAddOneObject()
    {
        String key = "a";
        StoredObject obj = new StoredObject(key, 4, "aasdfa");
        datastore.add(obj);
        verifyPresent(obj);
        verify(changeProcessor).processChange(same(null), same(obj));
    }

    @Test
    public void testAddOneObjectThenAnEqualObject()
    {
        String key = "a";
        StoredObject obj1 = new StoredObject(key, 4, "aasdfa");
        StoredObject obj2 = new StoredObject(key, 4, "aasdfa");
        datastore.add(obj1);
        datastore.add(obj2);
        verifyAbsent(obj1);
        verifyPresent(obj2);
        verify(changeProcessor).processChange(same(null), same(obj1));
    }

    @Test
    public void testAddOneObjectThenADifferentObjectWithSameKey()
    {
        StoredObject obj1 = new StoredObject("a", 4, "aasdfa");
        StoredObject obj2 = new StoredObject("a", 6, "aggg");
        datastore.add(obj1);
        datastore.add(obj2);
        verify(changeProcessor).processChange(same(null), same(obj1));
        verify(changeProcessor).processChange(same(obj1), same(obj2));
        verifyAbsent(obj1);
        verifyPresent(obj2);
    }

    @Test
    public void testAddTwoObjects()
    {
        StoredObject obj1 = new StoredObject("a", 4, "aasdfa");
        StoredObject obj2 = new StoredObject("b", 6, "aggg");
        datastore.add(obj1);
        datastore.add(obj2);
        verifyPresent(obj1);
        verifyPresent(obj2);
        verify(changeProcessor).processChange(same(null), same(obj1));
        verify(changeProcessor).processChange(same(null), same(obj2));
    }

    @Test
    public void testAddTwoObjectsThenRemoveOneByKey()
    {
        StoredObject obj1 = new StoredObject("a", 4, "aasdfa");
        StoredObject obj2 = new StoredObject("b", 6, "aggg");
        datastore.add(obj1);
        datastore.add(obj2);
        datastore.remove(obj2.getKey());
        verifyPresent(obj1);
        verifyAbsent(obj2);
        verify(changeProcessor).processChange(same(null), same(obj1));
        verify(changeProcessor).processChange(same(null), same(obj2));
        verify(changeProcessor).processChange(same(obj2), same(null));
    }

    @Test
    public void testAddTwoObjectsWithSameKeyThenRemoveTheSecondOne()
    {
        String key = "xxx";
        StoredObject obj1 = new StoredObject(key, 4, "aasdfa");
        StoredObject obj2 = new StoredObject(key, 6, "aggg");
        datastore.add(obj1);
        datastore.add(obj2);
        datastore.remove(obj2.getKey());
        verifyAbsent(obj1);
        verifyAbsent(obj2);
        verify(changeProcessor).processChange(same(null), same(obj1));
        verify(changeProcessor).processChange(same(obj1), same(obj2));
        verify(changeProcessor).processChange(same(obj2), same(null));
    }

    @Test
    public void testAddTwoObjectsWithSameIndexKey()
    {
        StoredObject obj1 = new StoredObject("a", 4, "aasdfa");
        StoredObject obj2 = new StoredObject("b", 4, "aggg");
        datastore.add(obj1);
        datastore.add(obj2);
        verifyPresent(obj1);
        verifyPresent(obj2);
        verify(changeProcessor).processChange(same(null), same(obj1));
        verify(changeProcessor).processChange(same(null), same(obj2));
        assertThat(datastore.index1.getObjects(obj1.getValue1())).containsExactlyInAnyOrder(obj1,
                obj2);
        assertThat(datastore.index2.getObjects(obj1.getValue2())).containsExactly(obj1);
        assertThat(datastore.index2.getObjects(obj2.getValue2())).containsExactly(obj2);
    }

    @Test
    public void testAddTwoObjectsWithSameIndexKeyThenRemoveOne()
    {
        StoredObject obj1 = new StoredObject("a", 4, "aasdfa");
        StoredObject obj2 = new StoredObject("b", 4, "aggg");
        datastore.add(obj1);
        datastore.add(obj2);
        datastore.remove(obj2.getKey());
        verifyPresent(obj1);
        verifyAbsent(obj2);
        verify(changeProcessor).processChange(same(null), same(obj1));
        verify(changeProcessor).processChange(same(null), same(obj2));
        verify(changeProcessor).processChange(same(obj2), same(null));
        assertThat(datastore.index1.getObjects(obj1.getValue1())).containsExactly(obj1);
        assertThat(datastore.index2.getObjects(obj1.getValue2())).containsExactly(obj1);
        assertThat(datastore.index2.getObjects(obj2.getValue2())).isEmpty();
    }

    @Test
    public void testAddObjectWithUnacceptableKey()
    {
        String key = "rejectThis";
        StoredObject obj = new StoredObject(key, 4, "aasdfa");
        datastore.add(obj);
        verifyAbsent(obj);
    }

    @Test
    public void testAddWithReplace()
    {
        Set<StoredObject> firstLot = Stream
                .of("1", "2", "3")
                .map(key -> new StoredObject(key, 1, "xxx"))
                .collect(Collectors.toSet());
        Set<StoredObject> secondLot = Stream
                .of("4", "2", "5")
                .map(key -> new StoredObject(key, 1, "xxx"))
                .collect(Collectors.toSet());
        datastore.add(firstLot);
        datastore.addReplace(secondLot);
        Stream.concat(firstLot.stream(), secondLot.stream()).distinct().forEach(
                obj -> verify(changeProcessor).processChange(same(null), same(obj)));
        firstLot.stream().filter(obj -> !secondLot.contains(obj)).forEach(obj -> {
            verify(changeProcessor).processChange(same(obj), same(null));
            verifyAbsent(obj);
        });
        secondLot.stream().forEach(obj -> verifyPresent(obj));
    }

    @Test
    public void testEmptyAddWithReplace()
    {
        Set<StoredObject> objects = Stream
                .of("1", "2", "3")
                .map(key -> new StoredObject(key, 1, "xxx"))
                .collect(Collectors.toSet());
        datastore.add(objects);
        datastore.addReplace();
        objects.stream().forEach(obj -> {
            verifyAbsent(obj);
            verify(changeProcessor).processChange(same(null), same(obj));
            verify(changeProcessor).processChange(same(obj), same(null));
        });
    }

    @Test
    public void testIndexingOfDifferentTypes()
    {
        class Super
        {
            public final String id;

            public Super(String id)
            {
                this.id = id;
            }
        }

        class Sub1 extends Super
        {
            public final String key;

            public Sub1(String id, String key)
            {
                super(id);
                this.key = key;
            }
        }

        class Sub2 extends Super
        {
            public final String key;

            public Sub2(String id, String key)
            {
                super(id);
                this.key = key;
            }
        }

        Datastore<String, Super> store = new Datastore<>(InMemoryStorage::new, o -> o.id);
        Index<String, String, Sub1> index1 = store.index(Sub1.class, (Sub1 o) -> o.key);
        Index<String, String, Sub2> index2 = store.index(Sub2.class, (Sub2 o) -> o.key);

        Super o1 = new Super("a");
        Sub1 o2 = new Sub1("b", "bb");
        Sub2 o3 = new Sub2("c", "cc");
        store.add(o1, o2, o3);
        Stream.of(o1, o2, o3).forEach(o -> assertThat(store.get(o.id)).isSameAs(o));
        assertThat(index1.getKeys()).containsOnly(o2.key);
        assertThat(index1.getIdentifiers(o2.key)).containsOnly(o2.id);
        assertThat(index1.getObjects(o2.key)).containsOnly(o2);
        assertThat(index2.getKeys()).containsOnly(o3.key);
        assertThat(index2.getIdentifiers(o3.key)).containsOnly(o3.id);
        assertThat(index2.getObjects(o3.key)).containsOnly(o3);
    }

    private void verifyPresent(StoredObject o)
    {
        assertThat(storage.store.get(o.getKey())).isSameAs(o);
        Stream
                .of(indexValidator(datastore.index1, o.getValue1(), o),
                        indexValidator(datastore.index2, o.getValue2(), o))
                .forEach(Runnable::run);
    }

    private static <K> Runnable indexValidator(Index<K, String, StoredObject> index, K indexKey,
            StoredObject expectedObject)
    {
        return () -> {
            assertThat(index.getObjects(indexKey)).areExactly(1, exactMatch(expectedObject));
            assertThat(index.getIdentifiers(indexKey)).contains(expectedObject.getKey());
        };
    }

    private void verifyAbsent(StoredObject o)
    {
        Condition<StoredObject> exactMatch = exactMatch(o);
        assertThat(storage.store.get(o.getKey())).isNotSameAs(o);
        assertThat(datastore.index1.getObjects(o.getValue1())).doNotHave(exactMatch);
        assertThat(datastore.index2.getObjects(o.getValue2())).doNotHave(exactMatch);
    }

    private static class StoredObject
    {
        private final String key;
        private final int value1;
        private final String value2;

        public StoredObject(String key, int value1, String value2)
        {
            this.key = key;
            this.value1 = value1;
            this.value2 = value2;
        }

        public String getKey()
        {
            return key;
        }

        public int getValue1()
        {
            return value1;
        }

        public String getValue2()
        {
            return value2;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + value1;
            result = prime * result + ((value2 == null) ? 0 : value2.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StoredObject other = (StoredObject) obj;
            if (key == null)
            {
                if (other.key != null)
                    return false;
            }
            else if (!key.equals(other.key))
                return false;
            if (value1 != other.value1)
                return false;
            if (value2 == null)
            {
                if (other.value2 != null)
                    return false;
            }
            else if (!value2.equals(other.value2))
                return false;
            return true;
        }

        @Override
        public String toString()
        {
            return "StoredObject [key=" + key + ", value1=" + value1 + ", value2=" + value2 + "]";
        }
    }

    private static class TestStorage implements Storage<String, StoredObject>
    {
        private final Map<String, StoredObject> store = new HashMap<>();

        @Override
        public StoredObject get(String identifier)
        {
            return store.get(identifier);
        }

        @Override
        public Stream<String> identifiers()
        {
            return store.keySet().stream();
        }

        @Override
        public void put(String identifier, StoredObject value)
        {
            store.put(identifier, value);
        }

        @Override
        public StoredObject remove(String identifier)
        {
            return store.remove(identifier);
        }

    }

    private static class TestDatastore extends Datastore<String, StoredObject>
    {
        public final Index<Integer, String, StoredObject> index1;
        public final Index<String, String, StoredObject> index2;

        private static <I, V> AdditionValidator<I, V> validator()
        {
            return (identifier, newValue, oldValue) -> {
                if ("rejectThis".equals(identifier))
                    throw new InvalidAdditionException("Don't like this one");
            };
        }

        public TestDatastore(Storage<String, StoredObject> storage,
                IdentifierGetter<String, StoredObject> identifierGetter,
                ChangeProcessor<StoredObject> changeProcessor)
        {
            super(() -> storage, identifierGetter, changeProcessor, validator());
            index1 = index(StoredObject::getValue1);
            index2 = index(StoredObject::getValue2);
        }
    }

    private static <T> Condition<T> exactMatch(T expected)
    {
        return new Condition<>(actual -> actual == expected, "Expecting an exact match");
    }
}
