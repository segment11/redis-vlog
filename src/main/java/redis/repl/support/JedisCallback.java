package redis.repl.support;

import redis.clients.jedis.Jedis;

public interface JedisCallback {
    Object call(Jedis jedis);
}
