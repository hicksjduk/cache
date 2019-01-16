package uk.org.thehickses.cache;

import java.io.File;
import java.util.Collections;
import java.util.Objects;
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

/**
 * An implementation of the {@link Storage} interface which uses a {@link Cache} as its underlying store.
 * 
 * @author Jeremy Hicks
 *
 * @param <I>
 *            the type of the identifiers which uniquely identify objects in the store.
 * @param <V>
 *            the type of the objects in the store. May be a supertype, allowing objects of different but related types
 *            to be stored.
 */
public class EhcacheStorage<I, V> implements Storage<I, V>
{
    private final static Logger LOG = LoggerFactory.getLogger(EhcacheStorage.class);

    private final Cache<I, V> cache;

    /**
     * Initialises the object with the specified name and configuration, and no persistence layer.
     * 
     * @param name
     *            the name. May not be null.
     * @param config
     *            the configuration. May not be null.
     */
    public EhcacheStorage(String name, CacheConfigurationBuilder<I, V> config)
    {
        this(name, config, CacheManagerBuilder.newCacheManagerBuilder().build(true));
    }

    /**
     * Initialises the object with the specified name and configuration, and a persistence layer housed in the specified
     * storage path.
     * 
     * @param name
     *            the name. May not be null.
     * @param config
     *            the configuration. May not be null.
     * @param storagePath
     *            the storage path. May not be null.
     */
    public EhcacheStorage(String name, CacheConfigurationBuilder<I, V> config, File storagePath)
    {
        this(name, config,
                CacheManagerBuilder
                        .newCacheManagerBuilder()
                        .with(CacheManagerBuilder.persistence(Objects.requireNonNull(storagePath)))
                        .build(true));
    }

    private EhcacheStorage(String name, CacheConfigurationBuilder<I, V> config,
            CacheManager cacheManager)
    {
        Stream.of(name, config).forEach(Objects::requireNonNull);
        CacheEventListener<I, V> evictionListener = this::processEviction;
        CacheEventListenerConfigurationBuilder listenerConfig = CacheEventListenerConfigurationBuilder
                .newEventListenerConfiguration(evictionListener,
                        Collections.singleton(EventType.EVICTED));
        this.cache = cacheManager.createCache(name, config.add(listenerConfig));
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
        LOG.debug("Value removed for identifier {} is {}", identifier, oldValue);
        return oldValue;
    }

}
