package redis.repl;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.persist.DynConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

// before slave start receive data from master, master need start this binlog for slave catch up
public class Binlog {

    private final byte slot;
    private final File binlogDir;
    private final DynConfig dynConfig;
    private RandomAccessFile raf;

    // old files, read and send to slave when catch up
    private final HashMap<Integer, RandomAccessFile> prevRafByFileIndex = new HashMap<>();

    // for cache
    private final LinkedList<BytesWithFileIndexAndOffset> latestAppendForReadCacheSegmentBytesSet = new LinkedList<>();
    private final short forReadCacheSegmentMaxCount;

    private static final String BINLOG_DIR_NAME = "binlog";

    record BytesWithFileIndexAndOffset(byte[] bytes, int fileIndex,
                                       long offset) implements Comparable<BytesWithFileIndexAndOffset> {
        @Override
        public String toString() {
            return "BytesWithFileIndexAndOffset{" +
                    "fileIndex=" + fileIndex +
                    ", offset=" + offset +
                    ", bytes.length=" + bytes.length +
                    '}';
        }

        @Override
        public int compareTo(@NotNull Binlog.BytesWithFileIndexAndOffset o) {
            if (fileIndex != o.fileIndex) {
                return Integer.compare(fileIndex, o.fileIndex);
            }
            return Long.compare(offset, o.offset);
        }
    }

    public record FileIndexAndOffset(int fileIndex, long offset) {
        @Override
        public String toString() {
            return "FileIndexAndOffset{" +
                    "fileIndex=" + fileIndex +
                    ", offset=" + offset +
                    '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            FileIndexAndOffset that = (FileIndexAndOffset) obj;
            return fileIndex == that.fileIndex && offset == that.offset;
        }
    }

    private final Logger log = LoggerFactory.getLogger(Binlog.class);

    // already sorted
    private ArrayList<File> listFiles() {
        ArrayList<File> list = new ArrayList<>();
        var files = binlogDir.listFiles();
        for (var file : files) {
            if (file.getName().startsWith(FILE_NAME_PREFIX)) {
                list.add(file);
            }
        }
        list.sort((o1, o2) -> Integer.compare(fileIndex(o1), fileIndex(o2)));
        return list;
    }

    public Binlog(byte slot, File slotDir, DynConfig dynConfig) throws IOException {
        this.slot = slot;
        this.binlogDir = new File(slotDir, BINLOG_DIR_NAME);
        if (!binlogDir.exists()) {
            if (!binlogDir.mkdirs()) {
                throw new IOException("Create binlog dir error, slot: " + slot);
            }
        }
        this.dynConfig = dynConfig;

        File latestFile;
        var files = listFiles();
        if (!files.isEmpty()) {
            latestFile = files.getLast();
            this.currentFileIndex = fileIndex(latestFile);
            this.currentFileOffset = latestFile.length();
        } else {
            // begin from 0
            latestFile = new File(binlogDir, fileName());
            FileUtils.touch(latestFile);
        }
        this.raf = new RandomAccessFile(latestFile, "rw");

        this.forReadCacheSegmentMaxCount = ConfForSlot.global.confRepl.binlogForReadCacheSegmentMaxCount;
        this.tempAppendSegmentBytes = new byte[ConfForSlot.global.confRepl.binlogOneSegmentLength];
        this.tempAppendSegmentBuffer = ByteBuffer.wrap(tempAppendSegmentBytes);
    }

    int currentFileIndex = 0;
    long currentFileOffset = 0;

    public FileIndexAndOffset currentFileIndexAndOffset() {
        return new FileIndexAndOffset(currentFileIndex, currentFileOffset);
    }

    public FileIndexAndOffset earliestFileIndexAndOffset() {
        // at least have one file, self created
        var files = listFiles();
        var file = files.get(0);
        return new FileIndexAndOffset(fileIndex(file), 0);
    }

    private static final String FILE_NAME_PREFIX = "binlog-";

    private String fileName() {
        return FILE_NAME_PREFIX + currentFileIndex;
    }

    private int fileIndex(File file) {
        return Integer.parseInt(file.getName().substring(FILE_NAME_PREFIX.length()));
    }

    private final byte[] tempAppendSegmentBytes;
    private final ByteBuffer tempAppendSegmentBuffer;

    private void addForReadCacheSegmentBytes(int fileIndex, long offset) {
        if (latestAppendForReadCacheSegmentBytesSet.size() >= forReadCacheSegmentMaxCount) {
            latestAppendForReadCacheSegmentBytesSet.removeFirst();
        }

        // copy one
        var bytes = new byte[tempAppendSegmentBytes.length];
        System.arraycopy(tempAppendSegmentBytes, 0, bytes, 0, tempAppendSegmentBytes.length);
        latestAppendForReadCacheSegmentBytesSet.add(new BytesWithFileIndexAndOffset(bytes, fileIndex, offset));
    }

    public static int oneFileMaxSegmentCount() {
        return ConfForSlot.global.confRepl.binlogOneFileMaxLength / ConfForSlot.global.confRepl.binlogOneSegmentLength;
    }

    public static int marginFileOffset(long fileOffset) {
        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        return (int) (fileOffset - fileOffset % oneSegmentLength);
    }

    public void append(BinlogContent content) {
        if (!dynConfig.isBinlogOn()) {
            return;
        }

        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;

        var encoded = content.encodeWithType();

        var beforeAppendFileOffset = currentFileOffset;
        var beforeAppendSegmentIndex = beforeAppendFileOffset / oneSegmentLength;
        var afterAppendFileOffset = beforeAppendFileOffset + encoded.length;
        var afterAppendSegmentIndex = afterAppendFileOffset / oneSegmentLength;

        var isCrossSegment = beforeAppendSegmentIndex != afterAppendSegmentIndex;
        if (isCrossSegment) {
            // need padding
            var padding = new byte[(int) (oneSegmentLength - beforeAppendFileOffset % oneSegmentLength)];
            try {
                raf.seek(currentFileOffset);
                raf.write(padding);
                currentFileOffset += padding.length;

                tempAppendSegmentBuffer.put(padding);
                addForReadCacheSegmentBytes(currentFileIndex, currentFileOffset - oneSegmentLength);
                tempAppendSegmentBuffer.clear();
                Arrays.fill(tempAppendSegmentBytes, (byte) 0);
            } catch (IOException e) {
                log.error("Write padding to binlog file error, slot: " + slot, e);
                throw new RuntimeException("Write padding to binlog file error: " + e.getMessage() + ", slot: " + slot);
            }

            beforeAppendFileOffset = currentFileOffset;
            afterAppendFileOffset = beforeAppendFileOffset + encoded.length;
        }

        try {
            if (afterAppendFileOffset > oneFileMaxLength) {
                // new file
                try {
                    raf.close();
                } catch (IOException e) {
                    log.error("Close binlog raf error, file index: " + currentFileIndex + ", slot: " + slot, e);
                }

                currentFileIndex++;
                var nextFile = new File(binlogDir, fileName());
                FileUtils.touch(nextFile);
                log.info("Create new binlog file, file: {}, slot: {}", nextFile.getName(), slot);
                raf = new RandomAccessFile(nextFile, "rw");

                currentFileOffset = 0;
                beforeAppendFileOffset = 0;
            }

            raf.seek(currentFileOffset);
            raf.write(encoded);
            currentFileOffset += encoded.length;

            tempAppendSegmentBuffer.put(encoded);
        } catch (IOException e) {
            log.error("Write to binlog file error, slot: " + slot, e);
            throw new RuntimeException("Write to binlog file error: " + e.getMessage() + ", slot: " + slot);
        }
    }

    RandomAccessFile prevRaf(int fileIndex) {
        return prevRafByFileIndex.computeIfAbsent(fileIndex, k -> {
            var file = new File(binlogDir, FILE_NAME_PREFIX + fileIndex);
            if (!file.exists()) {
                return null;
            }
            try {
                return new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                // never happen
                throw new RuntimeException(e);
            }
        });
    }

    byte[] getLatestAppendForReadCacheSegmentBytes(int fileIndex, long offset) {
        var one = Collections.binarySearch(latestAppendForReadCacheSegmentBytesSet,
                new BytesWithFileIndexAndOffset(null, fileIndex, offset));
        return one >= 0 ? latestAppendForReadCacheSegmentBytesSet.get(one).bytes : null;
    }

    public byte[] readPrevRafOneSegment(int fileIndex, long offset) throws IOException {
        if (fileIndex < 0) {
            return null;
        }

        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var modGiven = offset % oneSegmentLength;
        if (modGiven != 0) {
            throw new IllegalArgumentException("Repl read binlog segment bytes, offset must be multiple of one segment length, offset: " + offset);
        }

        // get from cache first
        // may be cross file
        // refer to ConfForSlot.global.confRepl.binlogOneFileMaxLength = 256 * 1024 * 1024
        if ((currentFileIndex - fileIndex) * 1024 < forReadCacheSegmentMaxCount) {
            // check cache
            var segmentBytes = getLatestAppendForReadCacheSegmentBytes(fileIndex, offset);
            if (segmentBytes != null) {
                return segmentBytes;
            }
        }

        // need not close
        var prevRaf = prevRaf(fileIndex);
        if (prevRaf == null) {
            return null;
        }

        if (prevRaf.length() <= offset) {
            return null;
        }

        var bytes = new byte[oneSegmentLength];
        prevRaf.seek(offset);
        var n = prevRaf.read(bytes);
        if (n < 0) {
            throw new RuntimeException("Read binlog one segment error, file index: " + fileIndex + ", offset: " + offset + ", slot: " + slot);
        }
        if (n < oneSegmentLength) {
            var readBytes = new byte[n];
            System.arraycopy(bytes, 0, readBytes, 0, n);
            return readBytes;
        }
        return bytes;
    }

    byte[] readCurrentRafOneSegment(long offset) throws IOException {
        if (raf.length() <= offset) {
            return null;
        }

        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var modGiven = offset % oneSegmentLength;
        if (modGiven != 0) {
            throw new IllegalArgumentException("Repl read binlog segment bytes, offset must be multiple of one segment length, offset: " + offset);
        }

        // check cache
        // current append segment
        long currentFileOffsetMarginSegmentOffset;
        var mod = currentFileOffset % oneSegmentLength;
        if (mod != 0) {
            currentFileOffsetMarginSegmentOffset = currentFileOffset - mod;
        } else {
            currentFileOffsetMarginSegmentOffset = currentFileOffset;
        }
        if (offset == currentFileOffsetMarginSegmentOffset) {
            return tempAppendSegmentBytes;
        }

        var segmentBytes = getLatestAppendForReadCacheSegmentBytes(currentFileIndex, offset);
        if (segmentBytes != null) {
            return segmentBytes;
        }

        var bytes = new byte[oneSegmentLength];

        raf.seek(offset);
        var n = raf.read(bytes);
        if (n < 0) {
            throw new RuntimeException("Read binlog one segment error, file index: " + currentFileIndex + ", offset: " + offset + ", slot: " + slot);
        }

        if (n == oneSegmentLength) {
            return bytes;
        } else {
            var readBytes = new byte[n];
            System.arraycopy(bytes, 0, readBytes, 0, n);
            return readBytes;
        }
    }

    public static int decodeAndApply(byte slot, byte[] oneSegmentBytes, int skipBytesN, ReplPair replPair) {
        var byteBuffer = ByteBuffer.wrap(oneSegmentBytes);
        byteBuffer.position(skipBytesN);

        var n = 0;
        while (true) {
            if (byteBuffer.remaining() == 0) {
                break;
            }

            var code = byteBuffer.get();
            if (code == 0) {
                break;
            }

            var type = BinlogContent.Type.fromCode(code);
            var content = type.decodeFrom(byteBuffer);
            content.apply(slot, replPair);
            n++;
        }
        return n;
    }

    public void clear() {
        // truncate
        try {
            raf.setLength(0);
        } catch (IOException e) {
            log.error("clear binlog error, slot: " + slot, e);
        }

        var it = prevRafByFileIndex.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            var prevRaf = entry.getValue();
            try {
                prevRaf.setLength(0);
                prevRaf.close();
            } catch (IOException e) {
                log.error("clear binlog old raf error, slot: " + slot + ", file index: " + entry.getKey(), e);
            }
            it.remove();
        }

        var files = listFiles();
        for (var file : files) {
            if (file.getName().equals(fileName())) {
                continue;
            }

            if (!file.delete()) {
                log.error("Delete binlog file error, file: {}, slot: {}", file.getName(), slot);
            } else {
                log.info("Delete binlog file success, file: {}, slot: {}", file.getName(), slot);
            }
        }
    }

    public void close() {
        try {
            raf.close();
            System.out.println("Close binlog current raf success, slot: " + slot);
        } catch (IOException e) {
            System.err.println("Close binlog current raf error, slot: " + slot);
        }

        for (var entry : prevRafByFileIndex.entrySet()) {
            var prevRaf = entry.getValue();
            try {
                prevRaf.close();
                System.out.println("Close binlog old raf success, slot: " + slot + ", file: " + entry.getKey());
            } catch (IOException e) {
                System.err.println("Close binlog old raf error, slot: " + slot + ", file: " + entry.getKey());
            }
        }
    }
}
