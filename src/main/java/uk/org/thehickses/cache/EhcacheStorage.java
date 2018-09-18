package uk.org.thehickses.cache;

import java.io.File;
import java.util.Collections;
import java.util.stream.Stream;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EhcacheStorage<I, V> implements Storage<I, V>
{
    private final static Logger LOG = LoggerFactory.getLogger(EhcacheStorage.class);

    private final CacheManager cacheManager;
    private final Cache<I, V> cache;
    private final CacheEventListener<I, V> evictionListener = this::processEviction;

    public EhcacheStorage(String name, CacheConfigurationBuilder<I, V> config)
    {
        this(name, config, null);
    }

    public EhcacheStorage(String name, CacheConfigurationBuilder<I, V> config, File storagePath)
    {
        cacheManager = storagePath != null
                ? CacheManagerBuilder
                        .newCacheManagerBuilder()
                        .with(CacheManagerBuilder.persistence(storagePath))
                        .build(true)
                : CacheManagerBuilder.newCacheManagerBuilder().build(true);
        CacheEventListenerConfigurationBuilder listenerConfig = CacheEventListenerConfigurationBuilder
                .newEventListenerConfiguration(evictionListener,
                        Collections.singleton(EventType.EVICTED));
        cache = cacheManager.createCache(name, config.add(listenerConfig));
    }

    private void processEviction(CacheEvent<? extends I, ? extends V> evt)
    {
        LOG.error("Cache is too small, object with identifier {} evicted", evt.getKey());
    }

    @Override
    public V get(I identifier)
    {
        V answer = cache.get(identifier);
        LOG.debug("Value returned for identifier {} is {}", identifier, answer);
        return answer;
    }

    @Override
    public Stream<I> identifiers()
    {
        Stream.Builder<I> builder = Stream.builder();
        cache.forEach(e -> builder.add(e.getKey()));
        return builder.build();
    }

    @Override
    public void put(I identifier, V value)
    {
        cache.put(identifier, value);
        LOG.debug("Value put for identifier {} is {}", identifier, value);
    }

    @Override
    public V remove(I identifier)
    {
        V oldValue = cache.get(identifier);
        if (oldValue != null)
            cache.remove(identifier);
        return oldValue;
    }

}
