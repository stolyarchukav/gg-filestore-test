package test;

import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.cache.store.local.GridCacheFileLocalStore;
import org.gridgain.grid.dr.cache.receiver.GridDrReceiverCacheConfiguration;
import org.gridgain.grid.dr.cache.receiver.GridDrReceiverCacheConflictResolverMode;
import org.gridgain.grid.lang.GridBiPredicate;
import org.gridgain.grid.marshaller.jdk.GridJdkMarshaller;
import org.gridgain.grid.spi.GridSpiException;
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class StoreTest {

    public static final String CACHE_NAME = "test";

    @Test
    public void test() throws Exception {
        //start two nodes
        UUID n1 = startNode("n1");
        UUID n2 = startNode("n2");

        //put value to each node
        GridCache cache1 = GridGain.grid(n1).cache(CACHE_NAME);
        cache1.put("1", "value_1");
        GridCache cache2 = GridGain.grid(n2).cache(CACHE_NAME);
        cache2.put("2", "value_2");

        //verify values
        assertEquals("value_2", cache1.get("2"));
        assertEquals("value_2", cache2.get("2"));

        //stop second node
        stopNode("n2");

        //update value stored by second node
        cache1.put("2", "value_2_updated");
        assertEquals("value_2_updated", cache1.get("2"));

        //start second node again
        startNode("n2");

        //wait for replicated cache
        Thread.sleep(5000);

        //verify new value was not overwritten by old stored value
        assertEquals("value_2_updated", cache1.get("2"));

        stopNode("n2");
        stopNode("n1");
    }

    private UUID startNode(String name) throws GridException {
        GridConfiguration cfg = new GridConfiguration();
        cfg.setDataCenterId((byte) 1);
        cfg.setGridName(name);
        GridCacheConfiguration cacheCfg = cache(name);
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDiscoverySpi(discoverySpi());
        cfg.setMarshaller(new GridJdkMarshaller());
        cfg.setPeerClassLoadingEnabled(true);
        Grid grid = GridGain.start(cfg);
        final AtomicInteger counter = new AtomicInteger();
        grid.cache(CACHE_NAME).loadCache(new GridBiPredicate<Object, Object>() {
            @Override
            public boolean apply(Object key, Object value) {
                counter.incrementAndGet();
                return true;
            }
        }, 0);
        System.out.println("Loaded from store: " + counter);
        return grid.node().id();
    }

    private GridCacheConfiguration cache(String name) {
        GridCacheConfiguration cache = new GridCacheConfiguration();
        cache.setName(CACHE_NAME);
        cache.setQueryIndexEnabled(true);
        cache.setCacheMode(GridCacheMode.REPLICATED);
        GridCacheFileLocalStore<Object, Object> store = new GridCacheFileLocalStore<>();
        store.setRootPath("store_file" + name);
        cache.setStore(store);
        GridDrReceiverCacheConfiguration drReceiverCfg = new GridDrReceiverCacheConfiguration();
        drReceiverCfg.setConflictResolverMode(GridDrReceiverCacheConflictResolverMode.DR_AUTO);
        cache.setDrReceiverConfiguration(drReceiverCfg);
        return cache;
    }

    private void stopNode(String name) throws GridException {
        GridGain.stop(name, true);
    }

    private static GridTcpDiscoverySpi discoverySpi() throws GridSpiException {
        GridTcpDiscoverySpi discoverySpi = new GridTcpDiscoverySpi();
        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("localhost"));
        discoverySpi.setIpFinder(ipFinder);
        return discoverySpi;
    }

}
