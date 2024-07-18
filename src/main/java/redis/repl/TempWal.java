package redis.repl;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;
import redis.persist.Wal;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

// after all exists data received by slave, before wal (is like memory sst) use this append file for slave catch up
public class TempWal {
    private final byte slot;
    private final File tempWalDir;
    private RandomAccessFile raf;
    private final File metaF;

    // old files, read and send to slave when catch up
    private final HashMap<Integer, RandomAccessFile> prevRafByFileIndex = new HashMap<>();

    private static final String TEMP_WAL_DIR_NAME = "temp-wal";
    private static final String META_FILE_NAME = "meta.dat";

    public record OffsetV(int fileIndex, long offset, Wal.V v) {
        @Override
        public String toString() {
            return "OffsetV{" +
                    ", fileIndex=" + fileIndex +
                    ", offset=" + offset +
                    ", key='" + v.key() + '\'' +
                    ", encoded.length=" + v.encodeLength() +
                    '}';
        }
    }

    record FileIndexAndOffset(int fileIndex, long offset) {
        @Override
        public String toString() {
            return "FileIndexAndOffset{" +
                    "fileIndex=" + fileIndex +
                    ", offset=" + offset +
                    '}';
        }
    }

    private final HashMap<String, OffsetV> map = new HashMap<>();

    void addForTest(String key, OffsetV offsetV) {
        map.put(key, offsetV);
    }

    public int size() {
        return map.size();
    }

    public byte[] getCvEncoded(String key) {
        var offsetV = map.get(key);
        if (offsetV == null) {
            return null;
        }

        return offsetV.v.cvEncoded();
    }

    private final Logger log = LoggerFactory.getLogger(TempWal.class);

    private ArrayList<File> listFiles() {
        ArrayList<File> list = new ArrayList<>();
        var files = tempWalDir.listFiles();
        if (files == null) {
            return list;
        }

        for (var file : files) {
            if (file.getName().startsWith(FILE_NAME_PREFIX)) {
                list.add(file);
            }
        }
        list.sort((o1, o2) -> Integer.compare(fileIndex(o1), fileIndex(o2)));
        return list;
    }

    public TempWal(byte slot, File slotDir) throws IOException {
        this.slot = slot;
        this.tempWalDir = new File(slotDir, TEMP_WAL_DIR_NAME);
        if (!tempWalDir.exists()) {
            if (!tempWalDir.mkdirs()) {
                throw new IOException("Create temp wal dir error, slot: " + slot);
            }
        }

        boolean needRead = false;

        File latestFile;
        var files = listFiles();
        if (!files.isEmpty()) {
            latestFile = files.getLast();
            this.currentFileIndex = fileIndex(latestFile);
            this.currentFileOffset = latestFile.length();

            needRead = true;
        } else {
            // begin from 0
            latestFile = new File(tempWalDir, fileName());
            FileUtils.touch(latestFile);
        }
        this.raf = new RandomAccessFile(latestFile, "rw");

        // read tmp wal meta files to map in memory as cache
        FileIndexAndOffset fileIndexAndOffset;
        this.metaF = new File(tempWalDir, META_FILE_NAME);
        if (!metaF.exists()) {
            FileUtils.touch(metaF);
            fileIndexAndOffset = new FileIndexAndOffset(0, 0);
        } else {
            fileIndexAndOffset = getMetaFileIndexAndOffset();
        }

        if (needRead) {
            // already sorted
            for (var file : files) {
                var fileIndex = fileIndex(file);
                if (fileIndex < fileIndexAndOffset.fileIndex) {
                    continue;
                }

                var n = readWal(file, fileIndex == fileIndexAndOffset.fileIndex ? fileIndexAndOffset.offset : 0);
                log.info("Read temp wal success, slot: {}, file: {}, begin offset: {}, count: {}",
                        slot, file.getName(), fileIndexAndOffset.offset, n);
            }
        }
    }

    int currentFileIndex = 0;
    long currentFileOffset = 0;

    private static final String FILE_NAME_PREFIX = "tmp-wal-";

    private String fileName() {
        return FILE_NAME_PREFIX + currentFileIndex;
    }

    private int fileIndex(File file) {
        return Integer.parseInt(file.getName().substring(FILE_NAME_PREFIX.length()));
    }

    int readWal(File targetFile, long beginFileOffset) throws IOException {
        var oneSegmentLength = ConfForSlot.global.confRepl.tempWalOneSegmentLength;

        var fileIndex = fileIndex(targetFile);
        var fis = new FileInputStream(targetFile);
        if (beginFileOffset > 0) {
            var mod = beginFileOffset % oneSegmentLength;
            if (mod != 0) {
                // begin with prev segment
                beginFileOffset -= mod;
            }
            if (beginFileOffset > 0) {
                fis.skip(beginFileOffset);
            }
        }

        var oneSegmentBufferBytes = new byte[oneSegmentLength];

        long i = 0;
        while (true) {
            var n = fis.read(oneSegmentBufferBytes);
            if (n < 0) {
                break;
            }

            var fileOffset = beginFileOffset + i * oneSegmentLength;
            var is = new DataInputStream(new ByteArrayInputStream(oneSegmentBufferBytes, 0, n));
            while (true) {
                var v = Wal.V.decode(is);
                if (v == null) {
                    break;
                }

                var encodeLength = v.encodeLength();
                map.put(v.key(), new OffsetV(fileIndex, fileOffset, v));
                fileOffset += encodeLength;
            }
            i++;
        }

        fis.close();
        return map.size();
    }

    public void append(Wal.V v) {
        var oneSegmentLength = ConfForSlot.global.confRepl.tempWalOneSegmentLength;
        var oneFileMaxLength = ConfForSlot.global.confRepl.tempWalOneFileMaxLength;

        byte[] encoded = v.encode();

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
            } catch (IOException e) {
                log.error("Write padding to temp wal file error", e);
                throw new RuntimeException("Write padding to temp wal file error: " + e.getMessage());
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
                    log.error("Close temp wal raf error", e);
                }

                currentFileIndex++;
                var nextFile = new File(tempWalDir, fileName());
                FileUtils.touch(nextFile);
                log.info("Create new temp wal file, slot: {}, file: {}", slot, nextFile.getName());
                raf = new RandomAccessFile(nextFile, "rw");

                currentFileOffset = 0;
                beforeAppendFileOffset = 0;
            }

            raf.seek(currentFileOffset);
            raf.write(encoded);
            currentFileOffset += encoded.length;
        } catch (IOException e) {
            log.error("Write to temp wal file error", e);
            throw new RuntimeException("Write to temp wal file error: " + e.getMessage());
        }

        map.put(v.key(), new OffsetV(currentFileIndex, beforeAppendFileOffset, v));
    }

    public interface CatchUpRemovedCallback {
        void handle(ArrayList<Wal.V> removedList);
    }

    int removeAfterCatchUp(int toFileIndex, long toFileOffset, CatchUpRemovedCallback callback) {
        var size = map.size();
        if (size >= 100_000) {
            log.warn("Temp wal size too large, slot: {}, size: {}", slot, size);
        }

        ArrayList<Wal.V> removedList = new ArrayList<>();

        var it = map.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            var offsetV = entry.getValue();

            if (offsetV.fileIndex < toFileIndex || (offsetV.fileIndex == toFileIndex && offsetV.offset < toFileOffset)) {
                removedList.add(offsetV.v);
                it.remove();
            }
        }

        // put back if handle failed ?
        callback.handle(removedList);

        updateMetaFileIndexAndOffset(toFileIndex, toFileOffset);
        return removedList.size();
    }

    void updateMetaFileIndexAndOffset(int fileIndex, long fileOffset) {
        var bytes = new byte[12];
        var buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(fileIndex);
        buffer.putLong(fileOffset);

        try {
            FileUtils.writeByteArrayToFile(metaF, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    FileIndexAndOffset getMetaFileIndexAndOffset() {
        if (metaF.length() == 0) {
            return new FileIndexAndOffset(0, 0);
        }

        try {
            var metaBytes = FileUtils.readFileToByteArray(metaF);
            var buffer = ByteBuffer.wrap(metaBytes);
            return new FileIndexAndOffset(buffer.getInt(), buffer.getLong());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    RandomAccessFile prevRaf(int fileIndex) {
        return prevRafByFileIndex.computeIfAbsent(fileIndex, k -> {
            var file = new File(tempWalDir, FILE_NAME_PREFIX + fileIndex);
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

    byte[] readPrevRafOneSegment(int fileIndex, long offset) throws IOException {
        // need not close
        var prevRaf = prevRaf(fileIndex);
        if (prevRaf == null) {
            return null;
        }

        if (prevRaf.length() <= offset) {
            return null;
        }

        var length = ConfForSlot.global.confRepl.tempWalOneSegmentLength;
        var bytes = new byte[length];

        prevRaf.seek(offset);
        var n = prevRaf.read(bytes);
        if (n != length) {
            throw new RuntimeException("Read temp wal one segment error, fileIndex: " + fileIndex + ", offset: " + offset);
        }
        return bytes;
    }

    byte[] readCurrentRafOneSegment(long offset) throws IOException {
        if (raf.length() <= offset) {
            return null;
        }

        var length = ConfForSlot.global.confRepl.tempWalOneSegmentLength;
        var bytes = new byte[length];

        raf.seek(offset);
        var n = raf.read(bytes);
        if (n == length) {
            return bytes;
        }

        if (n < 0) {
            return null;
        } else {
            var readBytes = new byte[n];
            System.arraycopy(bytes, 0, readBytes, 0, n);
            return readBytes;
        }
    }

    void clear() {
        map.clear();

        // truncate
        if (raf != null) {
            try {
                raf.setLength(0);
            } catch (Exception e) {
                log.error("clear temp wal error", e);
            }
        }

        if (!prevRafByFileIndex.isEmpty()) {
            var it = prevRafByFileIndex.entrySet().iterator();
            while (it.hasNext()) {
                var entry = it.next();
                var prevRaf = entry.getValue();
                try {
                    prevRaf.setLength(0);
                    prevRaf.close();
                } catch (Exception e) {
                    log.error("clear temp wal old raf error", e);
                }
                it.remove();
            }
        }

        var files = listFiles();
        for (var file : files) {
            if (file.getName().equals(fileName())) {
                continue;
            }

            if (!file.delete()) {
                log.error("Delete temp wal file error, file: {}", file.getName());
            } else {
                log.info("Delete temp wal file success, file: {}", file.getName());
            }
        }

        if (metaF != null) {
            updateMetaFileIndexAndOffset(0, 0);
        }
    }

    void close() {
        if (raf != null) {
            try {
                raf.close();
                System.out.println("Close temp wal raf success, slot: " + slot);
            } catch (IOException e) {
                System.err.println("Close temp wal raf error, slot: " + slot);
            }
        }

        for (var entry : prevRafByFileIndex.entrySet()) {
            var prevRaf = entry.getValue();
            try {
                prevRaf.close();
                System.out.println("Close temp wal old raf success, slot: " + slot + ", file: " + entry.getKey());
            } catch (IOException e) {
                System.err.println("Close temp wal old raf error, slot: " + slot + ", file: " + entry.getKey());
            }
        }
    }
}
