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
        def wal = new Wal((byte) 0, 0, (byte) 0, raf, rafShortValue, snowFlake)

        when:
        10.times {
            def key = 'short-key-' + it
            def value = 'short-value-' + it

            def cv = new CompressedValue()
            cv.keyHash = KeyHash.hash(key.bytes)
            cv.compressedData = value.bytes
            cv.compressedLength = cv.compressedData.length

            def cvEncoded = cv.encode()

            def v = new Wal.V((byte) 0, snowFlake.nextId(), 0, cv.keyHash, CompressedValue.NO_EXPIRE,
                    key, cvEncoded, cvEncoded.length)

            wal.put(true, key, v)

            def bytes = wal.get(key)
            def cv2 = CompressedValue.decode(Unpooled.wrappedBuffer(bytes), key.bytes, cv.keyHash, false)
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
}
