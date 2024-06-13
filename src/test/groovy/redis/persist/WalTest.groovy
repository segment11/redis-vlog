package redis.persist

import io.netty.buffer.Unpooled
import org.apache.commons.io.FileUtils
import redis.CompressedValue
import redis.KeyHash
import redis.SnowFlake
import spock.lang.Specification

class WalTest extends Specification {

    def 'put and get'() {
        given:
        def file = new File('test-raf.wal')
        def fileShortValue = new File('test-raf-short-value.wal')
        if (file.exists()) {
            file.delete()
        }
        if (fileShortValue.exists()) {
            fileShortValue.delete()
        }

        FileUtils.touch(file)
        FileUtils.touch(fileShortValue)

        println file.absolutePath
        println fileShortValue.absolutePath

        and:
        def raf = new RandomAccessFile(file, 'rw')
        def rafShortValue = new RandomAccessFile(fileShortValue, 'rw')
        def snowFlake = new SnowFlake(1, 1)
        def wal = new Wal((byte) 0, 0, raf, rafShortValue, snowFlake)

        when:
        10.times {
            def key = 'short-key-' + it
            def value = 'short-value-' + it

            def cv = new CompressedValue()
            cv.keyHash = KeyHash.hash(key.bytes)
            cv.compressedData = value.bytes
            cv.compressedLength = cv.compressedData.length

            def cvEncoded = cv.encode()

            def v = new Wal.V(snowFlake.nextId(), 0, cv.keyHash, CompressedValue.NO_EXPIRE,
                    key, cvEncoded, cvEncoded.length, false)

            wal.put(true, key, v)

            def bytes = wal.get(key)
            def cv2 = CompressedValue.decode(Unpooled.wrappedBuffer(bytes), key.bytes, cv.keyHash)
            def value2 = new String(cv2.compressedData)
            println "key: $key, cv2: $cv2, value2: $value2"
        }

        HashMap<String, Wal.V> toMap = [:]
        wal.readWal(rafShortValue, toMap, true)
        println toMap.keySet().join(',')

        then:
        toMap.size() == 10

        cleanup:
        raf.close()
        rafShortValue.close()
    }

    def 'test value change to short value'() {
        given:
        def wal = new Wal((byte) 0, 0, null, null, null)
        def key = 'test-key'
        def shortV = new Wal.V(1, 0, 0, 0, key, 'short-value'.bytes, 10, false)
        def v = new Wal.V(2, 0, 0, 0, key, 'value'.bytes, 30, false)
        def shortV2 = new Wal.V(3, 0, 0, 0, key, 'short-value-x'.bytes, 10, false)

        expect:
        wal.get(key) == null

        when:
        wal.delayToKeyBucketShortValues.put(key, shortV)

        then:
        wal.get(key) == 'short-value'.bytes

        when:
        wal.delayToKeyBucketValues.put(key, v)

        then:
        wal.get(key) == 'value'.bytes

        when:
        wal.delayToKeyBucketShortValues.put(key, shortV2)

        then:
        wal.get(key) == 'short-value-x'.bytes

        when:
        wal.delayToKeyBucketShortValues.remove(key)

        then:
        wal.get(key) == 'value'.bytes

        when:
        wal.delayToKeyBucketValues.remove(key)
        wal.delayToKeyBucketShortValues.remove(key)

        then:
        wal.get(key) == null
    }
}
