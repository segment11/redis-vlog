package redis.persist

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
}
