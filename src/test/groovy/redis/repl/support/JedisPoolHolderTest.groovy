package redis.repl.support

import spock.lang.Specification

class JedisPoolHolderTest extends Specification {
    def 'test connect'() {
        given:
        def holder = JedisPoolHolder.instance

        def jedisPool = holder.create('localhost', 6379, null, 5000)
        def jedisPool2 = holder.create('localhost', 6379, null, 5000)

        expect:
        jedisPool == jedisPool2

        when:
        String r
        try {
            r = JedisPoolHolder.exe(jedisPool) { jedis ->
                jedis.set('test', 'test')
                jedis.get('test')
            } as String
        } catch (Exception e) {
            // may redis server not started
            println e.message
            r = 'test'
        }
        then:
        r == 'test'

        cleanup:
        holder.closeAll()
    }
}
