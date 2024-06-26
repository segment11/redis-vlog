package redis.persist

import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

class BigStringFilesTest extends Specification {
    def 'test write and read'() {
        given:
        def bigString = 'a' * 10000

        def tmpSlotDir1 = new File('/tmp/tmp-slot-dir')
        def tmpSlotDir2 = new File('/tmp/tmp-slot-dir2')
        if (tmpSlotDir1.exists()) {
            tmpSlotDir1.deleteDir()
        }
        if (!tmpSlotDir2.exists()) {
            tmpSlotDir2.mkdirs()
        }

        def bigStringFiles1 = new BigStringFiles((byte) 0, tmpSlotDir1)
        def bigStringFiles2 = new BigStringFiles((byte) 0, tmpSlotDir2)

        when:
        def isWriteOk = bigStringFiles1.writeBigStringBytes(1L, 'a', bigString.bytes)

        then:
        isWriteOk
        bigStringFiles1.getBigStringBytesFromCache(1L) == bigString.bytes
        bigStringFiles1.getBigStringBytesFromCache(1L) == bigString.bytes
        bigStringFiles1.getBigStringFileUuidList().size() == 1

        bigStringFiles2.getBigStringBytesFromCache(1L) == null

        when:
        bigStringFiles1.deleteBigStringFileIfExist(1L)
        bigStringFiles2.deleteBigStringFileIfExist(1L)

        then:
        bigStringFiles1.getBigStringFileUuidList().size() == 0
    }

    def 'test write io exception'() {
        given:
        def noPermitDir = new File('/usr/tmp-slot-dir')

        when:
        boolean exception = false

        try {
            new BigStringFiles((byte) 0, noPermitDir)
        } catch (IOException e) {
            exception = true
        }

        then:
        exception

        when:
        def permitDir = new File('/tmp/tmp-slot-dir-x')
        def bigStringFiles = new BigStringFiles((byte) 0, permitDir)
        def bigStringDir = new File(permitDir, 'big-string')
        bigStringDir.mkdirs()

        def targetFile = new File(bigStringDir, '1')
        FileUtils.touch(targetFile)

        Files.setAttribute(bigStringDir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('r--r--r--'))

        def bigString = 'a' * 10000
        def isWriteOk = bigStringFiles.writeBigStringBytes(1L, 'a', bigString.bytes)

        then:
        !isWriteOk

        cleanup:
        // delete dir
        Files.setAttribute(bigStringDir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('rwxrwxrwx'))
        bigStringDir.deleteDir()
    }

    def 'test read io exception'() {
        given:
        def permitDir = new File('/tmp/tmp-slot-dir-x2')
        def bigStringFiles = new BigStringFiles((byte) 0, permitDir)
        def bigStringDir = new File(permitDir, 'big-string')
        bigStringDir.mkdirs()

        def targetFile = new File(bigStringDir, '1')
        FileUtils.touch(targetFile)

        when:
        Files.setAttribute(targetFile.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('-w--w--w-'))

        then:
        bigStringFiles.getBigStringBytesFromCache(1L) == null

        cleanup:
        // delete dir
        Files.setAttribute(bigStringDir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('rwxrwxrwx'))
        bigStringDir.deleteDir()
    }
}
