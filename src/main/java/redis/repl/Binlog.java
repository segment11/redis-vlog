package redis.repl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.ForTestMethod;
import redis.persist.DynConfig;
import redis.persist.InMemoryEstimate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

// before slave start receive data from master, master need start this binlog for slave catch up
public class Binlog implements InMemoryEstimate {

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

    // return sorted by file index
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
                throw new IOException("Repl create binlog dir error, slot: " + slot);
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

    @Override
    public long estimate() {
        long size = 0;
        size += tempAppendSegmentBytes.length;
        for (var one : latestAppendForReadCacheSegmentBytesSet) {
            size += one.bytes.length;
        }
        return size;
    }

    @ForTestMethod
    int getCurrentFileIndex() {
        return currentFileIndex;
    }

    @ForTestMethod
    long getCurrentFileOffset() {
        return currentFileOffset;
    }

    private int currentFileIndex = 0;

    private long currentFileOffset = 0;

    public FileIndexAndOffset currentFileIndexAndOffset() {
        return new FileIndexAndOffset(currentFileIndex, currentFileOffset);
    }

    public FileIndexAndOffset earliestFileIndexAndOffset() {
        // at least have one file, self created
        var files = listFiles();
        var file = files.get(0);
        return new FileIndexAndOffset(fileIndex(file), 0);
    }

    void resetCurrentFileOffsetForTest(long offset) {
        this.currentFileOffset = offset;
        this.clearByteBuffer();
        this.tempAppendSegmentBuffer.position((int) (offset % ConfForSlot.global.confRepl.binlogOneSegmentLength));
    }

    static class PaddingBinlogContent implements BinlogContent {
        private final byte[] paddingBytes;

        public PaddingBinlogContent(byte[] paddingBytes) {
            this.paddingBytes = paddingBytes;
        }

        @Override
        public Type type() {
            // need not decode
            return null;
        }

        @Override
        public int encodedLength() {
            return paddingBytes.length;
        }

        @Override
        public byte[] encodeWithType() {
            return paddingBytes;
        }

        @Override
        public void apply(byte slot, ReplPair replPair) {
            // do nothing
        }
    }

    public void moveToNextSegment() throws IOException {
        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var mod = currentFileOffset % oneSegmentLength;
        if (mod != 0) {
            var paddingN = oneSegmentLength - mod;
            var paddingBytes = new byte[(int) paddingN];
            var paddingContent = new PaddingBinlogContent(paddingBytes);
            append(paddingContent);
        } else {
            currentFileOffset += oneSegmentLength;
            clearByteBuffer();

            var oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
            var isLastSegment = currentFileOffset == oneFileMaxLength;
            if (isLastSegment) {
                createAndUseNextFile();
            }
        }
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

    private void clearByteBuffer() {
        var position = tempAppendSegmentBuffer.position();
        if (position > 0) {
            tempAppendSegmentBuffer.clear();

            Arrays.fill(tempAppendSegmentBytes, 0, position, (byte) 0);
        }
    }

    private void addForReadCacheSegmentBytes(int fileIndex, long offset, byte[] givenBytes) {
        // copy one
        byte[] bytes;
        if (givenBytes != null) {
            bytes = givenBytes;
        } else {
            bytes = new byte[tempAppendSegmentBytes.length];
            System.arraycopy(tempAppendSegmentBytes, 0, bytes, 0, tempAppendSegmentBytes.length);
        }

        if (latestAppendForReadCacheSegmentBytesSet.size() >= forReadCacheSegmentMaxCount) {
            latestAppendForReadCacheSegmentBytesSet.removeFirst();
        }
        latestAppendForReadCacheSegmentBytesSet.add(new BytesWithFileIndexAndOffset(bytes, fileIndex, offset));
    }

    public static int oneFileMaxSegmentCount() {
        return ConfForSlot.global.confRepl.binlogOneFileMaxLength / ConfForSlot.global.confRepl.binlogOneSegmentLength;
    }

    public static int marginFileOffset(long fileOffset) {
        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        return (int) (fileOffset - fileOffset % oneSegmentLength);
    }

    public void append(BinlogContent content) throws IOException {
        if (!dynConfig.isBinlogOn()) {
            return;
        }

        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;

        var encoded = content.encodeWithType();
        if (encoded.length >= oneSegmentLength) {
            throw new IllegalArgumentException("Repl append binlog content error, encoded length must be less than one segment length, slot: " +
                    slot + ", encoded length: " + encoded.length);
        }

        var isCrossSegment = tempAppendSegmentBuffer.position() + encoded.length > oneSegmentLength;
        if (isCrossSegment) {
            // need padding
            var padding = new byte[(int) (oneSegmentLength - currentFileOffset % oneSegmentLength)];

            raf.seek(currentFileOffset);
            raf.write(padding);
            currentFileOffset += padding.length;

            tempAppendSegmentBuffer.put(padding);
            addForReadCacheSegmentBytes(currentFileIndex, currentFileOffset - oneSegmentLength, null);
            clearByteBuffer();

            if (currentFileOffset == oneFileMaxLength) {
                createAndUseNextFile();
            }
        }

        raf.seek(currentFileOffset);
        raf.write(encoded);
        currentFileOffset += encoded.length;

        tempAppendSegmentBuffer.put(encoded);

        if (currentFileOffset == oneFileMaxLength) {
            addForReadCacheSegmentBytes(currentFileIndex, currentFileOffset - oneSegmentLength, null);
            clearByteBuffer();

            createAndUseNextFile();
        }
    }

    // self as slave, but also as master to another slave, need do binlog just same as master
    public void writeFromMasterOneSegmentBytes(byte[] oneSegmentBytes, int toFileIndex, long toFileOffset) throws IOException {
        if (!dynConfig.isBinlogOn()) {
            return;
        }

        int binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        if (oneSegmentBytes.length > binlogOneSegmentLength) {
            throw new IllegalArgumentException("Repl write binlog one segment bytes error, length must be less than " + binlogOneSegmentLength);
        }

        var oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;

        if (currentFileIndex == toFileIndex) {
            raf.seek(toFileOffset);
            raf.write(oneSegmentBytes);

            // because when master reset, self will write binlog from wal
            // need margin so can use a new beginning segment
            currentFileOffset = toFileOffset + binlogOneSegmentLength;
            clearByteBuffer();

            var isLastSegment = currentFileOffset == oneFileMaxLength;
            if (isLastSegment) {
                createAndUseNextFile();
            }
        } else {
            var prevRaf = prevRaf(toFileIndex, true);

            prevRaf.seek(toFileOffset);
            prevRaf.write(oneSegmentBytes);

            if (currentFileIndex < toFileIndex) {
                IOUtils.closeQuietly(raf);
                var rafRemoved = prevRafByFileIndex.remove(currentFileIndex);
                if (rafRemoved != null) {
                    if (rafRemoved != raf) {
                        IOUtils.closeQuietly(rafRemoved);
                    }
                }

                // set current to latest, because when master reset, self will write binlog from wal
                // need margin so can use a new beginning segment
                currentFileIndex = toFileIndex;
                currentFileOffset = toFileOffset + binlogOneSegmentLength;
                clearByteBuffer();
                raf = prevRaf;

                var isLastSegment = currentFileOffset == oneFileMaxLength;
                if (isLastSegment) {
                    createAndUseNextFile();
                }
            }
        }

        addForReadCacheSegmentBytes(toFileIndex, toFileOffset, oneSegmentBytes);
    }

    private void createAndUseNextFile() throws IOException {
        raf.close();
        log.info("Repl close current binlog file as overflow, file: {}, slot: {}", fileName(), slot);

        var prevRaf = prevRafByFileIndex.remove(currentFileIndex);
        if (prevRaf != null) {
            if (prevRaf != raf) {
                IOUtils.closeQuietly(prevRaf);
            }
        }

        currentFileIndex++;
        var nextFile = new File(binlogDir, fileName());
        FileUtils.touch(nextFile);
        log.info("Repl create new binlog file, file: {}, slot: {}", nextFile.getName(), slot);
        raf = new RandomAccessFile(nextFile, "rw");

        currentFileOffset = 0;

        // check file keep max count
        var files = listFiles();
        if (files.size() > ConfForSlot.global.confRepl.binlogFileKeepMaxCount) {
            // already sorted
            var firstFile = files.get(0);
            var firstFileIndex = fileIndex(firstFile);
            var rafRemoved = prevRafByFileIndex.remove(firstFileIndex);
            if (rafRemoved != null) {
                rafRemoved.close();
                log.info("Repl close binlog old raf success, slot: " + slot + ", file index: " + firstFileIndex);
            }

            if (!firstFile.delete()) {
                log.error("Repl delete binlog file error, file: {}, slot: {}", firstFile.getName(), slot);
            } else {
                log.info("Repl delete binlog file success, file: {}, slot: {}", firstFile.getName(), slot);
            }
        }
    }

    RandomAccessFile prevRaf(int fileIndex) {
        return prevRaf(fileIndex, false);
    }

    RandomAccessFile prevRaf(int fileIndex, boolean createIfNotExists) {
        return prevRafByFileIndex.computeIfAbsent(fileIndex, k -> {
            var file = new File(binlogDir, FILE_NAME_PREFIX + fileIndex);
            if (!file.exists()) {
                if (!createIfNotExists) {
                    return null;
                } else {
                    try {
                        FileUtils.touch(file);
                    } catch (IOException e) {
                        throw new RuntimeException("Repl touch new binlog file error, slot: " + slot + ", file index: " + fileIndex, e);
                    }
                }
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

        // get from cache first, only consider current file or previous file
        if (currentFileIndex == fileIndex || currentFileIndex - 1 == fileIndex) {
            // check cache
            var segmentBytes = getLatestAppendForReadCacheSegmentBytes(fileIndex, offset);
            if (segmentBytes != null) {
                return segmentBytes;
            }
        }

        // need not close
        var prevRaf = prevRaf(fileIndex);
        if (prevRaf == null) {
            // keep max count = 10 or 100, if write too fast, may be lost some files
            // so slave will get error repl reply, then need re-fetch all exists data from master and re-catch up
            throw new IOException("Repl read binlog segment bytes, file not exist, file index: " + fileIndex);
        }

        if (prevRaf.length() <= offset) {
            return null;
        }

        var bytes = new byte[oneSegmentLength];
        prevRaf.seek(offset);
        var n = prevRaf.read(bytes);
        if (n < 0) {
            throw new IOException("Repl read binlog segment bytes error, file index: " + fileIndex + ", offset: " + offset + ", slot: " + slot);
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
            throw new RuntimeException("Repl read binlog segment bytes error, file index: " + currentFileIndex + ", offset: " + offset + ", slot: " + slot);
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
            log.error("Repl clear binlog raf error, slot: " + slot + ", file index: " + currentFileIndex, e);
        }

        var it = prevRafByFileIndex.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            var prevRaf = entry.getValue();
            IOUtils.closeQuietly(prevRaf);
            it.remove();
        }

        var files = listFiles();
        for (var file : files) {
            if (file.getName().equals(fileName())) {
                continue;
            }

            if (!file.delete()) {
                log.error("Repl delete binlog file error, file: {}, slot: {}", file.getName(), slot);
            } else {
                log.info("Repl delete binlog file success, file: {}, slot: {}", file.getName(), slot);
            }
        }
    }

    public void close() {
        try {
            raf.close();
            System.out.println("Repl close binlog current raf success, slot: " + slot);
        } catch (IOException e) {
            System.err.println("Repl close binlog current raf error, slot: " + slot);
        }

        for (var entry : prevRafByFileIndex.entrySet()) {
            var prevFileIndex = entry.getKey();
            var prevRaf = entry.getValue();
            try {
                prevRaf.close();
                System.out.println("Repl close binlog old raf success, slot: " + slot + ", file index: " + prevFileIndex);
            } catch (IOException e) {
                System.err.println("Repl close binlog old raf error, slot: " + slot + ", file index: " + prevFileIndex);
            }
        }
    }
}
