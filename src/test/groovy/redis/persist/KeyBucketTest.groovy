package redis.persist

import redis.CompressedValue
import redis.KeyHash
import redis.SnowFlake
import spock.lang.Specification

class KeyBucketTest extends Specification {
    def 'put and split'() {
        given:
//        ConfForSlot.global.confBucket.isCompress = true

        def snowFlake = new SnowFlake(1, 1)

        def keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake)

        and:
        int number = 100

        List<Wal.V> list = []
        number.times {
            // like redis-benchmark key generator
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes
            def putValueBytes = ("value" + it).bytes

            def keyHash = KeyHash.hash(keyBytes)

            if (it % 10 == 0) {
                // set expire now
                def pvm = new PersistValueMeta()
                pvm.segmentOffset = it
                def encode = pvm.encode()
                def v = new Wal.V((byte) 0, 0L, 0, keyHash, CompressedValue.EXPIRE_NOW,
                        key, pvm.encode(), encode.length)
                list << v
            } else {
                def v = new Wal.V((byte) 0, 0L, 0, keyHash, 0L,
                        key, putValueBytes, putValueBytes.length)
                list << v
            }
        }

        and:
        List<KeyBucket> afterSplitKeyBucketList = []

        var targetKeyBucket = keyBucket
        for (v in list) {
            var keyBytes = v.key.bytes

            if (afterSplitKeyBucketList) {
                // already split
                var targetSplitIndex = (int) Math.abs(v.keyHash % 3)
                targetKeyBucket = afterSplitKeyBucketList.find { it.splitIndex == targetSplitIndex }
            }

            var kbArr = afterSplitKeyBucketList ? null : new KeyBucket[3]
            boolean isPutDone = targetKeyBucket.put(keyBytes, v.keyHash, v.expireAt(), v.cvEncoded, kbArr)

            if (kbArr && kbArr[0] != null) {
                println 'after split key buckets: '
                println kbArr[0]
                println kbArr[1]
                println kbArr[2]

                afterSplitKeyBucketList << kbArr[0]
                afterSplitKeyBucketList << kbArr[1]
                afterSplitKeyBucketList << kbArr[2]
            }
            if (isPutDone) {
                if (afterSplitKeyBucketList) {
                    // already split
                    var targetSplitIndex = (int) Math.abs(v.keyHash % 3)
                    targetKeyBucket = afterSplitKeyBucketList.find { it.splitIndex == targetSplitIndex }
                }

                var valueBytesWithExpireAt = targetKeyBucket.getValueByKey(keyBytes, v.keyHash)
                if (valueBytesWithExpireAt == null) {
                    def isClearExpired = (v.key[-3..-1] as int) % 10 == 0
                    if (isClearExpired) {
                        println 'clear expired when put or split: ' + v.key
                    } else {
                        throw new RuntimeException("value not found after put for key: " + v.key)
                    }
                } else {
                    if (!Arrays.equals(valueBytesWithExpireAt.valueBytes(), v.cvEncoded)) {
                        throw new RuntimeException("value not match after put for key: " + v.key)
                    }
                }
            } else {
                throw new RuntimeException("put failed for key: " + v.key)
            }
        }

        and:

        println '-------------------'
        println 'raw key bucket: '
        println keyBucket
        println '-------------------'

        println '-------------------'
        println 'after put all key bucket[1]: '
        println afterSplitKeyBucketList[1]
        println '-------------------'

        println '-------------------'
        println 'after put all key bucket[2]: '
        println afterSplitKeyBucketList[2]
        println '-------------------'

        expect:
        afterSplitKeyBucketList.size() == 3
    }

    def 'del then put corner case'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a'.bytes, 97L, 0L, 'a'.bytes, null)
        keyBucket.put('b'.bytes, 98L, 0L, 'b'.bytes, null)
        keyBucket.put('c'.bytes, 99L, 0L, 'c'.bytes, null)

        then:
        keyBucket.size == 3
        keyBucket.del('a'.bytes, 97L)
        keyBucket.size == 2

        when:
        keyBucket.put('b'.bytes, 98L, 0L, 'bb'.bytes, null)

        then:
        keyBucket.size == 2

        when:
        keyBucket.put('c'.bytes, 99L, 0L, 'cc'.bytes, null)

        then:
        keyBucket.size == 2
    }

    def 'shared bytes'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def k1 = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, snowFlake)
        def k2 = new KeyBucket((byte) 0, 1, (byte) 0, (byte) 1, null, snowFlake)

        and:
        k1.put('a'.bytes, 97L, 0L, 'a'.bytes, null)
        def k1Bytes = k1.encode()

        k2.put('a'.bytes, 97L, 0L, 'a'.bytes, null)
        def k2Bytes = k2.encode()

        def sharedBytes = new byte[4096 * 2]
        System.arraycopy(k1Bytes, 0, sharedBytes, 0, k1Bytes.length)
        System.arraycopy(k2Bytes, 0, sharedBytes, 4096, k2Bytes.length)

        when:
        def k11 = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, sharedBytes, 0, snowFlake)
        def k22 = new KeyBucket((byte) 0, 1, (byte) 0, (byte) 1, sharedBytes, 4096, snowFlake)

        then:
        k11.size == 1
        k22.size == 1

        k11.getValueByKey('a'.bytes, 97L).valueBytes() == 'a'.bytes
        k22.getValueByKey('a'.bytes, 97L).valueBytes() == 'a'.bytes
    }
}
