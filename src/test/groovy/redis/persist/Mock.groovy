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

            def v = new Wal.V((byte) 0, it, bucketIndex, keyHash, redis.CompressedValue.NO_EXPIRE,
                    key, valueBytes, valueBytes.length)

            shortValueList << v
        }
        shortValueList
    }

    static List<Wal.V> prepareValueList(int n, int bucketIndex = 0) {
        List<Wal.V> valuList = []
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

            def v = new Wal.V((byte) 0, it, bucketIndex, keyHash, redis.CompressedValue.NO_EXPIRE,
                    key, cv.encode(), cv.compressedLength())

            valuList << v
        }
        valuList
    }
}
