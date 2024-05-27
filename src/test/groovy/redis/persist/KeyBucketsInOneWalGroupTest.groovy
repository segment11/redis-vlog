package redis.persist


import spock.lang.Specification

class KeyBucketsInOneWalGroupTest extends Specification {
    def 'test put all'() {
        given:
        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        and:
        def inner = new KeyBucketsInOneWalGroup((byte) 0, 0, keyLoader)
        inner.readBeforePutBatch();

        and:
        def n = KeyBucket.INIT_CAPACITY + 1
        def shortValueList = Mock.prepareShortValueList(n)

        when:
        inner.putAll(shortValueList)

        then:
        shortValueList.every {
            def splitNumber = inner.splitNumberTmp[it.bucketIndex]
            def splitIndex = splitNumber == 1 ? 0 : (int) Math.abs(it.keyHash % splitNumber)
            inner.getKeyBucket(it.bucketIndex, (byte) splitIndex, splitNumber, it.keyHash).getValueByKey(it.key.bytes, it.keyHash).valueBytes() == it.cvEncoded()
        }
        inner.isSplit == (n > KeyBucket.INIT_CAPACITY)

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
