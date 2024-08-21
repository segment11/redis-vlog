package redis.repl.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class JedisPoolHolder {
    // singleton
    private JedisPoolHolder() {
    }

    private static final JedisPoolHolder instance = new JedisPoolHolder();

    public static JedisPoolHolder getInstance() {
        return instance;
    }

    private final Map<String, JedisPool> cached = new HashMap<>();

    private final Logger log = LoggerFactory.getLogger(JedisPoolHolder.class);

    public synchronized JedisPool create(String host, int port, String password, int timeoutMills) {
        var key = host + ":" + port;
        var client = cached.get(key);
        if (client != null) {
            return client;
        }

        var conf = new JedisPoolConfig();
        conf.setMaxTotal(10);
        conf.setMaxIdle(5);
        conf.setMaxWait(Duration.ofMillis(5000));

        conf.setTestOnBorrow(true);
        conf.setTestOnCreate(true);
        conf.setTestOnReturn(true);
        conf.setTestWhileIdle(true);

        var one = new JedisPool(conf, host, port, timeoutMills, password);
        log.info("Create jedis pool for {}:{}", host, port);
        cached.put(key, one);
        return one;
    }

    public void closeAll() {
        for (var pool : cached.values()) {
            pool.close();
            log.info("Close jedis pool");
        }
        cached.clear();
    }

    public static Object useRedisPool(JedisPool jedisPool, JedisCallback callback) {
        Jedis jedis = jedisPool.getResource();
        try {
            return callback.call(jedis);
        } finally {
            jedis.close();
        }
    }
}
