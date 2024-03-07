package redis.stats;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.ArrayList;
import java.util.List;

public interface OfStats {
    List<StatKV> stats();

    public static void cacheStatsToList(ArrayList<StatKV> list, CacheStats stats, String prefix) {
        list.add(new StatKV(prefix + "hit count", stats.hitCount()));
        list.add(new StatKV(prefix + "miss count", stats.missCount()));
        list.add(new StatKV(prefix + "hit rate", stats.hitRate()));
        list.add(new StatKV(prefix + "miss rate", stats.missRate()));
        list.add(new StatKV(prefix + "eviction count", stats.evictionCount()));
        list.add(new StatKV(prefix + "load success count", stats.loadSuccessCount()));
        list.add(new StatKV(prefix + "load failure count", stats.loadFailureCount()));
        list.add(new StatKV(prefix + "total load time", stats.totalLoadTime()));
        list.add(new StatKV(prefix + "average load penalty micros", stats.averageLoadPenalty() / 1000));
    }
}
