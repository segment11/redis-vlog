package redis.persist

import redis.*
import spock.lang.Specification

class KeyBucketTest extends Specification {
    def 'put and split'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        byte[] emptyBytes = []
        def keyBucket = new KeyBucket((byte) 0, (short) 0, (byte) 0, (byte) 1, emptyBytes, snowFlake)
        keyBucket.initWithCompressStats new CompressStats('test')

        and:
        int number = 100

        List<Wal.V> list = []
        number.times {
            // like redis-benchmark key generator
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes
            def putValueBytes = ("value" + it).bytes

            def keyHash = KeyHash.hash(keyBytes)

            def v = new Wal.V((byte) 0, 0L, 0, keyHash, 0L,
                    key, putValueBytes, putValueBytes.length)
            if (it % 10 == 0) {
                // set expire now
                def pvm = new PersistValueMeta()
                pvm.expireAt = CompressedValue.EXPIRE_NOW
                v.cvEncoded = pvm.encode()
            }

            list << v
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
            boolean isPutDone = targetKeyBucket.put(keyBytes, v.keyHash, v.cvEncoded, kbArr)

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
                var valueBytes = targetKeyBucket.getValueByKey(keyBytes, v.keyHash)
                if (valueBytes == null || !Arrays.equals(valueBytes, v.cvEncoded)) {
                    def isClearExpired = (v.key[-3..-1] as int) % 10 == 0
                    if (isClearExpired) {
                        println 'clear expired when split: ' + v.key
                    } else {
                        throw new RuntimeException("value not found after put for key: " + v.key)
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

        when:

        def decompressBytes = keyBucket.decompressBytes

        def compressedData = keyBucket.compress()
        println 'compressed data length: ' + compressedData.length
        var keyBucket2 = new KeyBucket((byte) 0, (short) 0, (byte) 0, (byte) afterSplitKeyBucketList.size(), compressedData, snowFlake)
        keyBucket2.initWithCompressStats new CompressStats('test')

        println '-------------------'
        println 'decompressed key bucket: '
        println keyBucket2
        println keyBucket2.allPrint()
        println '-------------------'

        KeyBucket keyBucket22
        KeyBucket keyBucket222
        if (afterSplitKeyBucketList) {
            keyBucket22 = new KeyBucket((byte) 0, (short) 0, (byte) 1, (byte) afterSplitKeyBucketList.size(),
                    afterSplitKeyBucketList[1].compress(), snowFlake)
            keyBucket222 = new KeyBucket((byte) 0, (short) 0, (byte) 2, (byte) afterSplitKeyBucketList.size(),
                    afterSplitKeyBucketList[2].compress(), snowFlake)
            keyBucket22.initWithCompressStats keyBucket2.compressStats
            keyBucket222.initWithCompressStats keyBucket2.compressStats

            println '-------------------'
            println 'decompressed key bucket 22: '
            println keyBucket22
            println keyBucket22.allPrint()
            println '-------------------'

            println '-------------------'
            println 'decompressed key bucket 222: '
            println keyBucket222
            println keyBucket222.allPrint()
            println '-------------------'
        }

        // test decompress cost
        100.times {
            var bucketX = new KeyBucket((byte) 0, (short) 0, (byte) 0, (byte) 1, compressedData, snowFlake)
            bucketX.initWithCompressStats keyBucket2.compressStats
        }

        def stats = keyBucket2.compressStats.stats()
        println Utils.padStats(stats, 60)

        for (v in list) {
            var keyBytes = v.key.bytes
            var splitIndex = (int) Math.abs(v.keyHash % 3)
            var targetKeyBucket2 = afterSplitKeyBucketList.find { it.splitIndex == splitIndex }
            var valueBytes = targetKeyBucket2.getValueByKey(keyBytes, v.keyHash)
            if (valueBytes == null || !Arrays.equals(valueBytes, v.cvEncoded)) {
                def isClearExpired = (v.key[-3..-1] as int) % 10 == 0
                if (isClearExpired) {
                    println 'clear expired when split: ' + v.key
                } else {
                    throw new RuntimeException("value not found for key: " + v.key)
                }
            }
        }

        def decompressBytes2 = keyBucket2.decompressBytes

        then:
        Arrays.equals(decompressBytes, decompressBytes2)
    }
}
