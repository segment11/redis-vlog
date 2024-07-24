package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.KeyHash
import redis.SnowFlake
import spock.lang.Specification

import java.nio.ByteBuffer

import static redis.persist.Consts.getSlotDir

class KeyLoaderTest extends Specification {
    static KeyLoader prepareKeyLoader(boolean deleteFiles = true) {
        if (deleteFiles && slotDir.exists()) {
            for (f in slotDir.listFiles()) {
                if (f.name.startsWith('key-bucket-split-') || f.name.startsWith('meta_key_bucket_split_number')) {
                    f.delete()
                }
            }
        }

        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')

        def snowFlake = new SnowFlake(1, 1)

        byte slot = 0
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake)
        // do nothing, just for test coverage
        keyLoader.cleanUp()

        keyLoader.initFds(libC)
        keyLoader
    }

    def 'test write and read one key'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1

        def keyLoader = prepareKeyLoader()

        expect:
        keyLoader.isBytesValidAsKeyBucket(null) == false
        keyLoader.isBytesValidAsKeyBucket(new byte[8]) == false
        keyLoader.getKeyCountInBucketIndex(0) == 0
        keyLoader.getKeyCount() == 0

        when:
        keyLoader.putValueByKeyForTest(0, 'a'.bytes, 10L, 0L, 1L, 'a'.bytes)
        def valueBytesWithExpireAt = keyLoader.getValueByKey(0, 'a'.bytes, 10L)

        then:
        valueBytesWithExpireAt.valueBytes() == 'a'.bytes

        when:
        def k0 = keyLoader.readKeyBucketForSingleKey(0, (byte) 0, (byte) 1, 10L, false)
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

        when:
        // never write yet
        def keyBucketsOverFdWriteIndex = keyLoader.readKeyBuckets(0)

        then:
        keyBucketsOverFdWriteIndex[0] == null

        when:
        var rawFdReadWrite = keyLoader.fdReadWriteArray[0]
        keyLoader.fdReadWriteArray[0] = null
        def keyBuckets = keyLoader.readKeyBuckets(0)
        def valueBytesWithExpireAt0 = keyLoader.getValueByKey(0, 'a'.bytes, 10L)
        def bytesToSlaveExists0 = keyLoader.readKeyBucketBytesBatchToSlaveExists((byte) 0, 0)
        def bytesBatch0 = keyLoader.readBatchInOneWalGroup((byte) 0, 0)
        def isRemoved0 = keyLoader.removeSingleKeyForTest(0, 'a'.bytes, 10L)

        then:
        keyBuckets[0] == null
        valueBytesWithExpireAt0 == null
        bytesToSlaveExists0 == null
        bytesBatch0 == null
        !isRemoved0

        when:
        keyLoader.fdReadWriteArray[0] = rawFdReadWrite
        def keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, keyLoader.snowFlake)
        rawFdReadWrite.writeOneInner(0, keyBucket.encode(true), false)

        keyLoader.putValueByKeyForTest(0, 'a'.bytes, 10L, 0L, 1L, 'a'.bytes)
        def valueBytesWithExpireAt = keyLoader.getValueByKey(0, 'a'.bytes, 10L)
        def bytesToSlaveExists = keyLoader.readKeyBucketBytesBatchToSlaveExists((byte) 0, 0)
        def bytesBatch = keyLoader.readBatchInOneWalGroup((byte) 0, 0)
        def isRemoved = keyLoader.removeSingleKeyForTest(0, 'a'.bytes, 10L)
        def isRemoved2 = keyLoader.removeSingleKeyForTest(0, 'b'.bytes, 11L)

        then:
        valueBytesWithExpireAt.valueBytes() == 'a'.bytes
        bytesToSlaveExists.length == KeyLoader.KEY_BUCKET_ONE_COST_SIZE
        bytesBatch != null
        isRemoved
        !isRemoved2

        when:
        boolean exception = false
        try {
            keyLoader.getMetaKeyBucketSplitNumberBatch(-1, 1)
        } catch (IllegalArgumentException e) {
            exception = true
        }

        then:
        exception

        when:
        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot
        exception = false
        try {
            keyLoader.getMetaKeyBucketSplitNumberBatch(bucketsPerSlot, 1)
        } catch (IllegalArgumentException e) {
            exception = true
        }

        then:
        exception

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
        exception = false
        try {
            keyLoader.setMetaKeyBucketSplitNumberForTest(-1, (byte) 1)
        } catch (IllegalArgumentException e) {
            exception = true
        }

        then:
        exception

        when:
        exception = false
        try {
            keyLoader.setMetaKeyBucketSplitNumberForTest(bucketsPerSlot, (byte) 1)
        } catch (IllegalArgumentException e) {
            exception = true
        }

        then:
        exception

        when:
        exception = false
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

        when:
        byte[] splitNumberArray = new byte[1]
        splitNumberArray[0] = (byte) 3
        keyLoader.updateMetaKeyBucketSplitNumberBatchIfChanged(0, splitNumberArray)

        then:
        keyLoader.metaKeyBucketSplitNumber.get(0) == (byte) 3

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'persist short value list'() {
        given:
        def keyLoader = prepareKeyLoader()

        and:
        var shortValueList = Mock.prepareShortValueList(10)

        when:
        keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList)

        then:
        shortValueList.every {
            keyLoader.getValueByKey(0, it.key.bytes, it.keyHash).valueBytes() == it.cvEncoded()
        }

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
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

        cleanup:
        keyLoader.flush()
        keyLoader.cleanUp()
    }

    def 'test repl'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1

        def keyLoader = prepareKeyLoader()

        expect:
        keyLoader.maxSplitNumberForRepl() == (byte) 1

        when:
        def metaSplitNumberBytes = keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()
        keyLoader.overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(metaSplitNumberBytes)

        then:
        metaSplitNumberBytes != null
        metaSplitNumberBytes == keyLoader.getMetaKeyBucketSplitNumberBytesToSlaveExists()

        when:
        // split index byte + split number byte + begin bucket index int + key buckets encoded bytes
        def position = 6
        def contentBytes = new byte[KeyLoader.BATCH_ONCE_KEY_BUCKET_COUNT_READ_FOR_REPL * KeyLoader.KEY_BUCKET_ONE_COST_SIZE + position]
        // begin bucket index 0
        def buffer = ByteBuffer.wrap(contentBytes)
        buffer.putInt(2, 0)

        def keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, keyLoader.snowFlake)
        keyBucket.put('a'.bytes, 97L, 0L, 1L, 'a'.bytes)
        buffer.put(position, keyBucket.encode(true))

        ConfForSlot.global.pureMemory = false
        keyLoader.writeKeyBucketsBytesBatchFromMasterExists(contentBytes)

        var k0 = keyLoader.readKeyBucketForSingleKey(0, (byte) 0, (byte) 1, 10L, false)

        then:
        k0.getValueByKey('a'.bytes, 97L).valueBytes() == 'a'.bytes

        when:
        ConfForSlot.global.pureMemory = true
        def fdReadWrite = keyLoader.fdReadWriteArray[0]
        def walGroupNumber = Wal.calcWalGroupNumber()
        fdReadWrite.allBytesByOneWalGroupIndexForKeyBucket = new byte[walGroupNumber][]
        def maxSegmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd;
        fdReadWrite.allBytesByOneInnerIndexForChunk = new byte[maxSegmentNumberPerFd][];

        keyLoader.writeKeyBucketsBytesBatchFromMasterExists(contentBytes)

        then:
        1 == 1

        cleanup:
        ConfForSlot.global.pureMemory = false
        keyLoader.flush()
        keyLoader.cleanUp()
    }
}
