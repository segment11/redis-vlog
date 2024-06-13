package redis.persist

import redis.CompressedValue
import redis.KeyHash

class Mock {
    static List<Wal.V> prepareShortValueList(int n, int bucketIndex = 0) {
        List<Wal.V> shortValueList = []
        n.times {
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes
            def valueBytes = ("value" + it).bytes

            def keyHash = KeyHash.hash(keyBytes)

            def v = new Wal.V(it, bucketIndex, keyHash, redis.CompressedValue.NO_EXPIRE,
                    key, valueBytes, valueBytes.length, false)

            shortValueList << v
        }
        shortValueList
    }

    static List<Wal.V> prepareValueList(int n, int bucketIndex = 0) {
        List<Wal.V> valueList = []
        n.times {
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes

            def keyHash = KeyHash.hash(keyBytes)

            def cv = new CompressedValue()
            cv.seq = it
            cv.dictSeqOrSpType = 1
            cv.keyHash = keyHash
            cv.compressedData = new byte[10]
            cv.compressedLength = 10
            cv.uncompressedLength = 10

            def v = new Wal.V(it, bucketIndex, keyHash, redis.CompressedValue.NO_EXPIRE,
                    key, cv.encode(), cv.encodedLength(), false)

            valueList << v
        }
        valueList
    }

    static List<CompressedValue> prepareCompressedValueList(int n) {
        List<CompressedValue> compressedValueList = []
        n.times {
            def cv = new CompressedValue()
            cv.seq = it
            cv.keyHash = it
            cv.compressedData = new byte[10]
            cv.compressedLength = 10
            cv.uncompressedLength = 10
            compressedValueList << cv
        }
        compressedValueList
    }

    // tuple2: key, keyHash
    static Map<Integer, List<Tuple2<String, Long>>> prepareKeyHashIndexByKeyBucketList(int n, int bucketsPerSlot) {
        Map<Integer, List<Tuple2<String, Long>>> keyHashByBucketIndex = [:]
        n.times {
            def key = 'key:' + it.toString().padLeft(12, '0')
            def keyHash = KeyHash.hash(key.bytes)
            def bucketIndex = (int) Math.abs((keyHash % bucketsPerSlot).intValue())
            def subList = keyHashByBucketIndex[bucketIndex]
            if (subList == null) {
                subList = []
                keyHashByBucketIndex[bucketIndex] = subList
            }
            subList << new Tuple2(key, keyHash)
        }
        keyHashByBucketIndex
    }
}
