package redis.persist

import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import redis.ConfForSlot
import redis.KeyHash
import redis.SnowFlake
import spock.lang.Specification

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
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, slotDir, snowFlake, null, null)
        keyLoader.initFds(libC)
        keyLoader
    }

    def 'test write and read one key'() {
        given:
        def keyLoader = prepareKeyLoader()

        and:
        // clear files
        3.times { splitIndex ->
            def file = new File(slotDir, "key-bucket-split-" + splitIndex + ".dat")
            if (file.exists()) {
                file.delete()
            }
        }

        when:
        keyLoader.putValueByKeyForTest(0, 'a'.getBytes(), 3L, 0L, 1L, 'a'.bytes)
        def valueBytesWithExpireAt = keyLoader.getValueByKey(0, 'a'.bytes, 3L)

        then:
        valueBytesWithExpireAt.valueBytes() == 'a'.bytes

        when:
        keyLoader.setMetaKeyBucketSplitNumberFromMasterNewly(0, (byte) 3)
        keyLoader.putValueByKeyForTest(0, 'b'.getBytes(), 2L, 0L, 1L, 'b'.bytes)

        def keyBuckets = keyLoader.readKeyBuckets(0)

        then:
        keyBuckets.size() == 3
        keyBuckets[2].getValueByKey('b'.bytes, 2L).valueBytes() == 'b'.bytes

        when:
        def isRemoved = keyLoader.remove(0, 'a'.getBytes(), 3L)

        then:
        isRemoved
        keyLoader.getValueByKey(0, 'a'.bytes, 3L) == null

        cleanup:
        keyLoader.cleanUp()
    }

    def 'persist short value list'() {
        given:
        def keyLoader = prepareKeyLoader()

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
        keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList)

        then:
        shortValueList.every {
            keyLoader.getValueByKey(0, it.key.bytes, it.keyHash).valueBytes() == it.cvEncoded()
        }

        cleanup:
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
        keyLoader.cleanUp()
    }
}
