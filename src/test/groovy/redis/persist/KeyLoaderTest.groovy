package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.KeyHash
import redis.SnowFlake
import spock.lang.Specification

import java.nio.ByteBuffer

class KeyLoaderTest extends Specification {
    static KeyLoader prepareKeyLoader(boolean deleteFiles = true) {
        if (deleteFiles && Consts.slotDir.exists()) {
            for (f in Consts.slotDir.listFiles()) {
                if (f.name.startsWith('key-bucket-split-') || f.name.startsWith('meta_key_bucket_split_number')) {
                    f.delete()
                }
            }
        }

        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')

        def snowFlake = new SnowFlake(1, 1)

        byte slot = 0
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir, snowFlake)
        // do nothing, just for test coverage
        keyLoader.cleanUp()

        keyLoader.initFds(libC)
        keyLoader.initFds((byte) 1)
        keyLoader
    }

    final byte slot = 0
    final byte splitIndex = 0

    def 'test base'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()
        def bucketsPerSlot = keyLoader.bucketsPerSlot
        def oneKeyBucketLength = KeyLoader.KEY_BUCKET_ONE_COST_SIZE
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber
        println keyLoader
        keyLoader.keyLoaderInnerGauge.collect()

        expect:
        !keyLoader.isBytesValidAsKeyBucket(null, 0)
        !keyLoader.isBytesValidAsKeyBucket(new byte[8], 0)
        keyLoader.keyCount == 0
        keyLoader.getKeyCountInBucketIndex(0) == 0
        keyLoader.statKeyCountInBucketsBytesToSlaveExists.length == bucketsPerSlot * 2
        keyLoader.maxSplitNumberForRepl() == 1
        KeyLoader.getPositionInSharedBytes(0) == 0
        KeyLoader.getPositionInSharedBytes(1) == oneKeyBucketLength
        KeyLoader.getPositionInSharedBytes(oneChargeBucketNumber) == 0
        KeyLoader.getPositionInSharedBytes(oneChargeBucketNumber + 2) == oneKeyBucketLength * 2

        when:
        def statKeyCountBytes = new byte[bucketsPerSlot * 2]
        ByteBuffer.wrap(statKeyCountBytes).putShort(0, (short) 1)
        keyLoader.overwriteStatKeyCountInBucketsBytesFromMasterExists(statKeyCountBytes)
        then:
        keyLoader.keyCount == 1
        keyLoader.getKeyCountInBucketIndex(0) == 1

        when:
        def exception = false
        try {
            keyLoader.getKeyCountInBucketIndex(-1)
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.getKeyCountInBucketIndex(bucketsPerSlot)
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        when:
        def splitNumberBytes = keyLoader.getMetaKeyBucketSplitNumberBatch(0, oneChargeBucketNumber)
        then:
        splitNumberBytes.length == oneChargeBucketNumber

        when:
        exception = false
        try {
            keyLoader.getMetaKeyBucketSplitNumberBatch(-1, 1)
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.getMetaKeyBucketSplitNumberBatch(bucketsPerSlot, 1)
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        when:
        def metaSplitNumberBytes = keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()
        keyLoader.overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(metaSplitNumberBytes)
        then:
        metaSplitNumberBytes != null
        metaSplitNumberBytes == keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()

        when:
        exception = false
        try {
            keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(-1, new byte[0])
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(bucketsPerSlot, new byte[0])
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        when:
        def splitNumberArray = new byte[1]
        splitNumberArray[0] = (byte) 3
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(0, splitNumberArray)
        then:
        keyLoader.metaKeyBucketSplitNumber.get(0) == (byte) 3
        !keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(0, splitNumberArray)

        when:
        keyLoader.setMetaKeyBucketSplitNumberForTest(0, (byte) 1)
        then:
        keyLoader.metaKeyBucketSplitNumber.get(0) == (byte) 1

        when:
        exception = false
        try {
            keyLoader.setMetaKeyBucketSplitNumberForTest(-1, (byte) 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.setMetaKeyBucketSplitNumberForTest(keyLoader.bucketsPerSlot, (byte) 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        int[] keyCountArray = new int[2]
        keyCountArray[0] = 1
        keyCountArray[1] = 2
        keyLoader.updateKeyCountBatchCached(keyCountArray, 0)
        then:
        keyLoader.getKeyCountInBucketIndex(0) == 1
        keyLoader.getKeyCountInBucketIndex(1) == 2

        when:
        exception = false
        try {
            keyLoader.updateKeyCountBatchCached(new int[1], -1)
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyLoader.updateKeyCountBatchCached(new int[1], bucketsPerSlot)
        } catch (IllegalArgumentException e) {
            exception = true
        }
        then:
        exception

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'test repl'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()
        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber

        expect:
        keyLoader.maxSplitNumberForRepl() == (byte) 1

        when:
        def metaSplitNumberBytes = keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()
        keyLoader.overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(metaSplitNumberBytes)
        then:
        metaSplitNumberBytes != null
        metaSplitNumberBytes == keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()

        when:
        ConfForSlot.global.pureMemory = false
        // split index byte + split number byte + begin bucket index int + key buckets encoded bytes
        def position = 6
        def contentBytes = new byte[KeyLoader.BATCH_ONCE_KEY_BUCKET_COUNT_READ_FOR_REPL * KeyLoader.KEY_BUCKET_ONE_COST_SIZE + position]
        // begin bucket index 0
        def buffer = ByteBuffer.wrap(contentBytes)
        // begin bucket index 0
        buffer.putInt(2, 0)
        def keyBucket = new KeyBucket(slot, 0, splitIndex, (byte) 1, null, keyLoader.snowFlake)
        keyBucket.put('a'.bytes, 97L, 0L, 1L, 'a'.bytes)
        buffer.put(position, keyBucket.encode(true))
        keyLoader.writeKeyBucketsBytesBatchFromMasterExists(contentBytes)
        def k0 = keyLoader.readKeyBucketForSingleKey(0, splitIndex, (byte) 1, 10L, false)
        then:
        k0.getValueByKey('a'.bytes, 97L).valueBytes() == 'a'.bytes

        when:
        ConfForSlot.global.pureMemory = true
        def rawFdReadWrite = keyLoader.fdReadWriteArray[0]
        def walGroupNumber = Wal.calcWalGroupNumber()
        rawFdReadWrite.resetAllBytesByOneWalGroupIndexForKeyBucketForTest(walGroupNumber)
        keyLoader.writeKeyBucketsBytesBatchFromMasterExists(contentBytes)
        k0 = keyLoader.readKeyBucketForSingleKey(0, splitIndex, (byte) 1, 10L, false)
        def k1 = keyLoader.readKeyBucketForSingleKey(1, splitIndex, (byte) 1, 10L, false)
        then:
        k0.getValueByKey('a'.bytes, 97L).valueBytes() == 'a'.bytes
        k1 == null

        when:
        keyLoader.fdReadWriteArray = new FdReadWrite[2]
        keyLoader.fdReadWriteArray[0] = rawFdReadWrite
        def splitNumberArray = new byte[oneChargeBucketNumber]
        splitNumberArray[0] = (byte) 3
        keyLoader.metaKeyBucketSplitNumber.setBatch(0, splitNumberArray)
        def contentBytes2 = new byte[position]
        def buffer2 = ByteBuffer.wrap(contentBytes2)
        // update split index to 1
        buffer2.put(0, (byte) 1)
        keyLoader.writeKeyBucketsBytesBatchFromMasterExists(contentBytes2)
        then:
        keyLoader.fdReadWriteArray[1].clearOneWalGroupToMemoryForTest(0)

        when:
        // update split index to 2
        buffer2.put(0, (byte) 2)
        keyLoader.writeKeyBucketsBytesBatchFromMasterExists(contentBytes2)
        then:
        keyLoader.fdReadWriteArray[2].clearOneWalGroupToMemoryForTest(0)

        when:
        boolean exception = false
        def contentBytes3 = new byte[position + 1]
        try {
            keyLoader.writeKeyBucketsBytesBatchFromMasterExists(contentBytes3)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        ConfForSlot.global.pureMemory = false
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test write and read one key'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        def keyLoader = prepareKeyLoader()

        when:
        keyLoader.putValueByKeyForTest(0, 'a'.bytes, 10L, 0L, 1L, 'a'.bytes)
        def valueBytesWithExpireAt = keyLoader.getValueByKey(0, 'a'.bytes, 10L)
        then:
        valueBytesWithExpireAt.valueBytes() == 'a'.bytes

        when:
        def k0 = keyLoader.readKeyBucketForSingleKey(0, splitIndex, (byte) 1, 10L, false)
        k0.splitNumber = (byte) 2
        def bytes = k0.encode(true)
        keyLoader.fdReadWriteArray[0].writeOneInner(0, bytes, false)
        keyLoader.setMetaKeyBucketSplitNumberForTest(0, (byte) 2)
        keyLoader.putValueByKeyForTest(0, 'b'.bytes, 11L, 0L, 1L, 'b'.bytes)
        def keyBuckets = keyLoader.readKeyBuckets(0)
        println keyLoader.readKeyBucketsToStringForDebug(0)
        then:
        keyBuckets.size() == 2
        keyBuckets.count {
            if (!it) {
                return false
            }
            it.getValueByKey('b'.bytes, 11L)?.valueBytes() == 'b'.bytes
        } == 1

        when:
        def isRemoved = keyLoader.removeSingleKeyForTest(0, 'a'.bytes, 10L)
        then:
        isRemoved
        keyLoader.getValueByKey(0, 'a'.bytes, 10L) == null

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test some branches'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1

        def keyLoader = prepareKeyLoader()
        def oneKeyBucketLength = KeyLoader.KEY_BUCKET_ONE_COST_SIZE
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber

        when:
        // never write yet
        def keyBucketsOverFdWriteIndex = keyLoader.readKeyBuckets(0)
        then:
        keyBucketsOverFdWriteIndex[0] == null

        when:
        def rawFdReadWrite = keyLoader.fdReadWriteArray[0]
        keyLoader.fdReadWriteArray[0] = null
        def keyBuckets = keyLoader.readKeyBuckets(0)
        def valueBytesWithExpireAt0 = keyLoader.getValueByKey(0, 'a'.bytes, 10L)
        def bytesToSlaveExists0 = keyLoader.readKeyBucketBytesBatchToSlaveExists(splitIndex, 0)
        def bytesBatch0 = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved0 = keyLoader.removeSingleKeyForTest(0, 'a'.bytes, 10L)
        then:
        keyBuckets[0] == null
        valueBytesWithExpireAt0 == null
        bytesToSlaveExists0 == null
        bytesBatch0 == null
        !isRemoved0

        when:
        keyLoader.fdReadWriteArray[0] = rawFdReadWrite
        def keyBucket = new KeyBucket(slot, 0, splitIndex, (byte) 1, null, keyLoader.snowFlake)
        rawFdReadWrite.writeOneInner(0, keyBucket.encode(true), false)
        keyLoader.putValueByKeyForTest(0, 'a'.bytes, 10L, 0L, 1L, 'a'.bytes)
        def valueBytesWithExpireAt = keyLoader.getValueByKey(0, 'a'.bytes, 10L)
        def bytesToSlaveExists = keyLoader.readKeyBucketBytesBatchToSlaveExists(splitIndex, 0)
        def bytesBatch = keyLoader.readBatchInOneWalGroup(splitIndex, 0)
        def isRemoved = keyLoader.removeSingleKeyForTest(0, 'a'.bytes, 10L)
        def isRemoved2 = keyLoader.removeSingleKeyForTest(0, 'b'.bytes, 11L)
        then:
        valueBytesWithExpireAt.valueBytes() == 'a'.bytes
        bytesToSlaveExists.length == oneKeyBucketLength
        bytesBatch != null
        isRemoved
        !isRemoved2

        when:
        keyLoader.fdReadWriteArray = new FdReadWrite[2]
        keyLoader.fdReadWriteArray[0] = rawFdReadWrite
        def sharedBytesListBySplitIndex = new byte[3][]
        def sharedBytes0 = new byte[oneChargeBucketNumber * oneKeyBucketLength]
        def sharedBytes2 = new byte[oneChargeBucketNumber * oneKeyBucketLength]
        // mock split index = 2, bucket index 0, is valid key bucket
        ByteBuffer.wrap(sharedBytes2).putLong(3L)
        sharedBytesListBySplitIndex[0] = sharedBytes0
        sharedBytesListBySplitIndex[1] = null
        sharedBytesListBySplitIndex[2] = sharedBytes2
        keyLoader.writeSharedBytesList(sharedBytesListBySplitIndex, 0)
        then:
        keyLoader.fdReadWriteArray[1] != null

        when:
        ConfForSlot.global.pureMemory = true
        def walGroupNumber = Wal.calcWalGroupNumber()
        def splitNumberArray = new byte[oneChargeBucketNumber]
        splitNumberArray[0] = (byte) 3
        keyLoader.metaKeyBucketSplitNumber.setBatch(0, splitNumberArray)
        keyLoader.fdReadWriteArray[0].resetAllBytesByOneWalGroupIndexForKeyBucketForTest(walGroupNumber)
        keyLoader.fdReadWriteArray[1].resetAllBytesByOneWalGroupIndexForKeyBucketForTest(walGroupNumber)
        keyLoader.fdReadWriteArray[2].resetAllBytesByOneWalGroupIndexForKeyBucketForTest(walGroupNumber)
        keyLoader.writeSharedBytesList(sharedBytesListBySplitIndex, 0)
        def keyBucketListFromMemory = keyLoader.readKeyBuckets(0)
        then:
        keyBucketListFromMemory.size() == 3
        keyBucketListFromMemory[0] == null
        keyBucketListFromMemory[1] == null
        keyBucketListFromMemory[2] != null

        cleanup:
        ConfForSlot.global.pureMemory = false
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'persist short value list'() {
        given:
        def keyLoader = prepareKeyLoader()

        and:
        def shortValueList = Mock.prepareShortValueList(10)

        when:
        keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        then:
        shortValueList.every {
            keyLoader.getValueByKey(0, it.key.bytes, it.keyHash).valueBytes() == it.cvEncoded()
        }

        when:
        final byte slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        def keyLoader2 = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir2, keyLoader.snowFlake, oneSlot)
        keyLoader2.initFds(keyLoader.libC)
        keyLoader2.initFds((byte) 1)
        keyLoader2.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        then:
        1 == 1

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
        keyLoader2.flush()
        keyLoader2.cleanUp()
    }

    def 'persist pvm list'() {
        given:
        def keyLoader = prepareKeyLoader()

        and:
        List<PersistValueMeta> pvmList = []
        10.times {
            def key = "key:" + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes

            def keyHash = KeyHash.hash(keyBytes)

            def pvm = new PersistValueMeta()
            pvm.keyBytes = keyBytes
            pvm.keyHash = keyHash
            pvm.bucketIndex = 0
            pvm.segmentOffset = it
            pvmList << pvm
        }

        when:
        keyLoader.updatePvmListBatchAfterWriteSegments(0, pvmList)
        then:
        pvmList.every {
            keyLoader.getValueByKey(0, it.keyBytes, it.keyHash).valueBytes() == it.encode()
        }

        when:
        final byte slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        def keyLoader2 = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir2, keyLoader.snowFlake, oneSlot)
        keyLoader2.initFds(keyLoader.libC)
        keyLoader2.initFds((byte) 1)
        keyLoader2.updatePvmListBatchAfterWriteSegments(0, pvmList)
        then:
        1 == 1

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
        keyLoader2.flush()
        keyLoader2.cleanUp()
    }
}
