package redis.persist

import redis.KeyHash
import spock.lang.Specification

class KeyBucketsInOneWalGroupTest extends Specification {
    def 'test put all'() {
        given:
        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        and:
        def inner = new KeyBucketsInOneWalGroup((byte) 0, 0, keyLoader)
        inner.readBeforePutBatch();

        and:
        List<Wal.V> shortValueList = []
        10.times {
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes
            def putValueBytes = ("value" + it).bytes

            def keyHash = KeyHash.hash(keyBytes)

            def v = new Wal.V((byte) 0, 0L, 0, keyHash, 0L,
                    key, putValueBytes, putValueBytes.length)

            shortValueList << v
        }

        when:
        inner.putAll(shortValueList)

        then:
        shortValueList.every {
            def splitNumber = inner.splitNumberTmp[it.bucketIndex]
            def splitInder = splitNumber == 1 ? 0 : (int) Math.abs(it.keyHash % splitNumber)
            inner.getKeyBucket(it.bucketIndex, (byte) splitInder, splitNumber, it.keyHash).getValueByKey(it.key.bytes, it.keyHash).valueBytes() == it.cvEncoded()
        }
        !inner.isSplit

        when:
        def sharedBytesList = inner.writeAfterPutBatch()
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        keyLoader.updateBatchSplitNumber(inner.splitNumberTmp, inner.beginBucketIndex)

        then:

        def firstShortValue = shortValueList[0]
        def valueBytesWithExpireAt = keyLoader.getValueByKey(firstShortValue.bucketIndex, firstShortValue.key.bytes, firstShortValue.keyHash)

        then:
        valueBytesWithExpireAt.valueBytes() == firstShortValue.cvEncoded()

        cleanup:
        keyLoader.cleanUp()
    }
}
