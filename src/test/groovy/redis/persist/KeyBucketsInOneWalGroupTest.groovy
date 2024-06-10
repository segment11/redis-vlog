package redis.persist

import redis.CompressedValue
import redis.SnowFlake
import spock.lang.Specification

class KeyBucketsInOneWalGroupTest extends Specification {
    def 'test put all'() {
        given:
        def keyLoader = KeyLoaderTest.prepareKeyLoader()

        and:
        def inner = new KeyBucketsInOneWalGroup((byte) 0, 0, keyLoader)
        inner.readBeforePutBatch()

        and:
        def n = KeyBucket.INIT_CAPACITY + 1
        def shortValueList = Mock.prepareShortValueList(n)
        def shortValueList2 = Mock.prepareShortValueList(n, (byte) 1)
        shortValueList.addAll(shortValueList2)

        when:
        inner.putAll(shortValueList)

        then:
        shortValueList.every {
            inner.getValue(it.bucketIndex, it.key.bytes, it.keyHash).valueBytes() == it.cvEncoded
        }
        inner.isSplit == (n > KeyBucket.INIT_CAPACITY)

        when:
        def sharedBytesList = inner.writeAfterPutBatch()
        keyLoader.writeSharedBytesList(sharedBytesList, inner.beginBucketIndex)
        def isSplitNumberChanged = keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(inner.beginBucketIndex, inner.splitNumberTmp)

        def firstShortValue = shortValueList[0]
        def valueBytesWithExpireAt = keyLoader.getValueByKey(firstShortValue.bucketIndex, firstShortValue.key.bytes, firstShortValue.keyHash)

        then:
        isSplitNumberChanged == inner.isSplit
        valueBytesWithExpireAt.valueBytes == firstShortValue.cvEncoded

        cleanup:
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test check if need split'() {
        given:
        def inner = new KeyBucketsInOneWalGroup((byte) 0, 0, null)
        def snowFlake = new SnowFlake(1, 1)

        and:
        def n = 10
        def shortValueList10 = Mock.prepareShortValueList(n)

        List<PersistValueMeta> needAddNewList = []
        List<PersistValueMeta> needUpdateList = []
        List<PersistValueMeta> needDeleteList = []

        def pvmList10ThisBucket = shortValueList10.collect { v ->
            KeyBucketsInOneWalGroup.transferWalV(v)
        }

        when:
        int bucketIndex = 0
        byte currentSplitNumber = 1

        inner.listList = [[null]]
        var splitMultiStep = inner.checkIfNeedSplit(pvmList10ThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)

        then:
        splitMultiStep == 1
        needAddNewList.size() == 10

        when:
        var keyBucket = new KeyBucket((byte) 0, bucketIndex, (byte) 0, (byte) 1, null, snowFlake)
        pvmList10ThisBucket[0..<5].each {
            keyBucket.put(it.keyBytes, it.keyHash, 0L, 0L, it.extendBytes)
        }
        inner.listList = [[keyBucket]]

        needAddNewList.clear()
        var splitMultiStep2 = inner.checkIfNeedSplit(pvmList10ThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)

        then:
        splitMultiStep2 == 1
        needAddNewList.size() == 5
        needUpdateList.size() == 5

        when:
        pvmList10ThisBucket[-1].expireAt = CompressedValue.EXPIRE_NOW
        pvmList10ThisBucket[4].expireAt = CompressedValue.EXPIRE_NOW
        needAddNewList.clear()
        needUpdateList.clear()
        var splitMultiStep3 = inner.checkIfNeedSplit(pvmList10ThisBucket, needAddNewList, needUpdateList, needDeleteList,
                bucketIndex, currentSplitNumber)

        then:
        splitMultiStep3 == 1
        needAddNewList.size() == 4
        needUpdateList.size() == 4
        needDeleteList.size() == 1
    }
}
