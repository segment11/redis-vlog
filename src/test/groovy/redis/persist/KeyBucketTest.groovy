package redis.persist


import redis.SnowFlake
import spock.lang.Specification

class KeyBucketTest extends Specification {
    def 'del then put corner case'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a'.bytes, 97L, 0L, 1L, 'a'.bytes)
        keyBucket.put('b'.bytes, 98L, 0L, 2L, 'b'.bytes)
        keyBucket.put('c'.bytes, 99L, 0L, 3L, 'c'.bytes)

        then:
        keyBucket.size == 3
        keyBucket.del('a'.bytes, 97L, true)
        keyBucket.size == 2

        when:
        keyBucket.put('b'.bytes, 98L, 0L, 2L, 'bb'.bytes)

        then:
        keyBucket.size == 2

        when:
        keyBucket.put('c'.bytes, 99L, 0L, 3L, 'cc'.bytes)

        then:
        keyBucket.size == 2
    }

    def 'shared bytes'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def k1 = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake)
        def k2 = new KeyBucket((byte) 0, 1, (byte) 0, (byte) 1, null, snowFlake)

        and:
        k1.put('a'.bytes, 97L, 0L, 1L, 'a'.bytes)
        def k1Bytes = k1.encode(false)

        k2.put('a'.bytes, 97L, 0L, 1L, 'a'.bytes)
        def k2Bytes = k2.encode(false)

        def sharedBytes = new byte[4096 * 2]
        System.arraycopy(k1Bytes, 0, sharedBytes, 0, k1Bytes.length)
        System.arraycopy(k2Bytes, 0, sharedBytes, 4096, k2Bytes.length)

        when:
        def k11 = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, sharedBytes, 0, snowFlake)
        def k22 = new KeyBucket((byte) 0, 1, (byte) 0, (byte) 1, sharedBytes, 4096, snowFlake)
        def k33 = new KeyBucket((byte) 0, 1, (byte) 0, (byte) 1, sharedBytes, sharedBytes.length, snowFlake)

        then:
        k11.size == 1
        k22.size == 1
        k33.size == 0

        k11.getValueByKey('a'.bytes, 97L).valueBytes == 'a'.bytes
        k22.getValueByKey('a'.bytes, 97L).valueBytes == 'a'.bytes
    }

    def 'multi cell count'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a'.bytes, 97L, 0L, 1L, 'a'.bytes)

        then:
        keyBucket.size == 1
        keyBucket.cellCost == 1

        when:
        var longKeyBytes = 'a'.padRight(100, 'a').bytes
        keyBucket.put(longKeyBytes, 9797L, 0L, 1L, 'long a'.bytes)

        then:
        keyBucket.size == 2
        keyBucket.cellCost == 3

        keyBucket.getValueByKey(longKeyBytes, 9797L).valueBytes == 'long a'.bytes

        when:
        keyBucket.put('b'.bytes, 98L, System.currentTimeMillis() - 1, 2L, 'b'.bytes)

        then:
        keyBucket.size == 3
        keyBucket.getValueByKey('b'.bytes, 98L) != null

        when:
        keyBucket.clearAllExpired()

        then:
        keyBucket.size == 2
        keyBucket.getValueByKey('b'.bytes, 98L) == null

        when:
        keyBucket.clearAll()

        then:
        keyBucket.size == 0
        keyBucket.cellCost == 0
        keyBucket.getValueByKey('a'.bytes, 97L) == null
        keyBucket.getValueByKey(longKeyBytes, 9797L) == null
    }
}
