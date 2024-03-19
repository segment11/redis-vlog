package redis.persist;

import com.kenai.jffi.MemoryIO;
import jnr.constants.platform.OpenFlags;
import jnr.posix.LibC;
import net.openhft.posix.MMapFlag;
import net.openhft.posix.MMapProt;
import net.openhft.posix.MSyncFlag;
import net.openhft.posix.PosixAPI;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static redis.persist.LocalPersist.PAGE_SIZE;

public class MmapBuffer {
    private final int capacity;
    private final int pageCount;
    private final File file;

    public MmapBuffer(File file, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (capacity % PAGE_SIZE != 0) {
            throw new IllegalArgumentException("Capacity must be a multiple of " + PAGE_SIZE);
        }

        this.pageCount = capacity / PAGE_SIZE;
        this.capacity = capacity;
        this.file = file;
    }

    private final PosixAPI posixApi = PosixAPI.posix();

    private LibC libC;

    public void setLibC(LibC libC) {
        this.libC = libC;
    }

    ByteBuffer buffer;
    private long addr;

    private int fd;

    public void init(byte initByte) throws IOException {
        if (!file.exists()) {
            FileUtils.touch(file);
            // prepend with 0
            var zero = new byte[PAGE_SIZE];
            if (initByte != 0) {
                Arrays.fill(zero, initByte);
            }

            for (int i = 0; i < pageCount; i++) {
                FileUtils.writeByteArrayToFile(file, zero, true);
            }
        }

        this.fd = libC.open(file.getAbsolutePath(), OpenFlags.O_RDWR.value(), 0644);
        this.addr = posixApi.mmap(0, capacity, MMapProt.PROT_READ.value() | MMapProt.PROT_WRITE.value(),
                MMapFlag.SHARED.value(), fd, 0);

        var m = MemoryIO.getInstance();
        this.buffer = m.newDirectByteBuffer(addr, capacity);
    }

    public byte getByte(int offset) {
        return buffer.get(offset);
    }

    public short getShort(int offset) {
        return buffer.getShort(offset);
    }

    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    public long getLong(int offset) {
        return buffer.getLong(offset);
    }

    public byte[] getBytes(int offset, int length) {
        var bytes = new byte[length];
        buffer.position(offset);
        buffer.get(bytes);
        return bytes;
    }

    public void write(int offset, byte[] data, boolean isSync) {
        buffer.position(offset);
        buffer.put(data);

        if (isSync) {
            msync(offset, data.length);
        }
    }

    void msync(int offset, int dataLength) {
        var beginPage = offset / PAGE_SIZE;
        var endPage = (offset + dataLength) / PAGE_SIZE;
        if ((offset + dataLength) % PAGE_SIZE != 0) {
            endPage++;
        }

        var length = (endPage - beginPage) * PAGE_SIZE;
        var start = beginPage * PAGE_SIZE;
        if (start + length > capacity) {
            throw new IllegalArgumentException("Out of bound, offset: " + offset + ", length: " + length + ", capacity: " + capacity);
        }

        var n = posixApi.msync(addr + start, length, MSyncFlag.MS_SYNC.value());
        if (n != 0) {
            throw new RuntimeException("Failed to sync memory to file: " + file.getAbsolutePath());
        }
    }

    public void cleanUp() {
        if (this.addr != 0) {
            var posixApi = PosixAPI.posix();
            posixApi.munmap(addr, capacity);
            System.out.println("Unmapped file: " + file.getAbsolutePath() + ", capacity: " + capacity);
        }

        if (this.fd != 0) {
            libC.close(fd);
            System.out.println("Closed file: " + file.getAbsolutePath());
        }
    }

}
