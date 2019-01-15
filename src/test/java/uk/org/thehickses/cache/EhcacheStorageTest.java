package uk.org.thehickses.cache;

import static org.assertj.core.api.Assertions.*;
import java.io.File;
import java.io.Serializable;
import java.util.stream.IntStream;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

public class EhcacheStorageTest
{

    @Test
    public void test()
    {
        int objCount = 10000;
        Datastore<Integer, TestObj> store = new Datastore<>(this::storage, o -> o.id);
        store.add(IntStream.range(0, objCount).mapToObj(TestObj::new).toArray(TestObj[]::new));
        IntStream.range(0, objCount).forEach(i -> {
            TestObj obj = store.get(i);
            assertThat(obj).as(String.format("Looking up %d", i)).isNotNull();
            assertThat(obj.id).isEqualTo(i);
        });
    }

    private Storage<Integer, TestObj> storage()
    {
        return new EhcacheStorage<>("storage",
                CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, TestObj.class,
                        ResourcePoolsBuilder.heap(1000).disk(50, MemoryUnit.MB)),
                new File("diskCache"));
    }

    private static class TestObj implements Serializable
    {
        private static final long serialVersionUID = 6083880774403243884L;

        public final int id;

        public TestObj(int id)
        {
            this.id = id;
        }
    }
}
