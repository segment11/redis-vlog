package redis;

import com.github.luben.zstd.Zstd;
import io.activej.net.socket.tcp.ITcpSocket;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.command.AGroup;
import redis.decode.Request;
import redis.mock.ByPassGetSet;
import redis.persist.LocalPersist;
import redis.persist.OneSlot;
import redis.persist.SegmentOverflowException;
import redis.reply.Reply;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static redis.CompressedValue.NO_EXPIRE;
import static redis.CompressedValue.NULL_DICT_SEQ;
import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

public abstract class BaseCommand {
//    protected interface IsKeyBytes {
//        boolean isKeyBytes(int i);
//    }
//
//    protected static final class GivenKeyIndex implements IsKeyBytes {
//        private final int index;
//
//        public GivenKeyIndex(int index) {
//            this.index = index;
//        }
//
//        @Override
//        public boolean isKeyBytes(int i) {
//            return i == index;
//        }
//    }
//
//    protected static final class FromToKeyIndex implements IsKeyBytes {
//        private final int from;
//        private final int to;
//        private final int step;
//
//        public FromToKeyIndex(int from, int to, int step) {
//            this.from = from;
//            this.to = to;
//            this.step = step;
//        }
//
//        @Override
//        public boolean isKeyBytes(int i) {
//            return i >= from && (to == -1 || i <= to) && (step == 1 || (i - from) % step == 0);
//        }
//    }
//
//    protected static final IsKeyBytes KeyIndex1 = new GivenKeyIndex(1);
//    protected static final IsKeyBytes KeyIndex2 = new GivenKeyIndex(2);
//    protected static final IsKeyBytes KeyIndexBegin1 = new FromToKeyIndex(1, -1, 1);
//    protected static final IsKeyBytes KeyIndexBegin1Step2 = new FromToKeyIndex(1, -1, 2);
//    protected static final IsKeyBytes KeyIndexBegin2 = new FromToKeyIndex(2, -1, 1);
//    protected static final IsKeyBytes KeyIndexBegin2Step2 = new FromToKeyIndex(2, -1, 2);
//
//    protected static void addToSlotWithKeyHashList(ArrayList<SlotWithKeyHash> slotWithKeyHashList,
//                                                   byte[][] data, int slotNumber, IsKeyBytes isKeyBytes) {
//        for (int i = 1; i < data.length; i++) {
//            if (isKeyBytes.isKeyBytes(i)) {
//                var keyBytes = data[i];
//                var slotWithKeyHash = slot(keyBytes, slotNumber);
//                slotWithKeyHashList.add(slotWithKeyHash);
//            }
//        }
//    }

    public record SlotWithKeyHashWithKeyBytes(SlotWithKeyHash slotWithKeyHash, byte[] keyBytes) {
        @Override
        public String toString() {
            return "SlotWithKeyHashWithKeyBytes{" +
                    "slotWithKeyHash=" + slotWithKeyHash +
                    ", key=" + new String(keyBytes) +
                    '}';
        }
    }

    // for dyn script execute
    public String getCmd() {
        return cmd;
    }

    public byte[][] getData() {
        return data;
    }

    public ITcpSocket getSocket() {
        return socket;
    }

    // need final, for unit test, can change
    protected String cmd;
    protected byte[][] data;
    protected ITcpSocket socket;

    @TestOnly
    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    @TestOnly
    public void setData(byte[][] data) {
        this.data = data;
    }

    @TestOnly
    public void setSocket(ITcpSocket socket) {
        this.socket = socket;
    }

    public BaseCommand(String cmd, byte[][] data, ITcpSocket socket) {
        this.cmd = cmd;
        this.data = data;
        this.socket = socket;
    }

    protected final DictMap dictMap = DictMap.getInstance();

    protected byte workerId;
    protected byte netWorkers;

    @TestOnly
    public short getSlotNumber() {
        return slotNumber;
    }

    protected short slotNumber;
    protected CompressStats compressStats;

    protected int compressLevel = Zstd.defaultCompressionLevel();
    protected int trainSampleListMaxSize = 1000;

    protected SnowFlake snowFlake;
    protected TrainSampleJob trainSampleJob;
    protected List<TrainSampleJob.TrainSampleKV> sampleToTrainList;

    @TestOnly
    public void setLocalTest(boolean localTest) {
        this.localTest = localTest;
    }

    @TestOnly
    public void setLocalTestRandomValueList(ArrayList<byte[]> localTestRandomValueList) {
        this.localTestRandomValueList = localTestRandomValueList;
    }

    @TestOnly
    public ArrayList<byte[]> getLocalTestRandomValueList() {
        return localTestRandomValueList;
    }

    @TestOnly
    protected boolean localTest;
    @TestOnly
    protected int localTestRandomValueListSize;
    @TestOnly
    protected ArrayList<byte[]> localTestRandomValueList;

    protected ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed;

    @TestOnly
    public ArrayList<SlotWithKeyHash> getSlotWithKeyHashListParsed() {
        return slotWithKeyHashListParsed;
    }

    @TestOnly
    public void setSlotWithKeyHashListParsed(ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed) {
        this.slotWithKeyHashListParsed = slotWithKeyHashListParsed;
    }

    protected boolean isCrossRequestWorker;

    public void setCrossRequestWorker(boolean crossRequestWorker) {
        isCrossRequestWorker = crossRequestWorker;
    }

    public static AGroup mockAGroup() {
        return BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1);
    }

    public static AGroup mockAGroup(byte workerId, byte netWorkers, short slotNumber) {
        return mockAGroup(workerId, netWorkers, slotNumber, new CompressStats("mock", "net_"),
                Zstd.defaultCompressionLevel(), 100, new SnowFlake(1, 1),
                new TrainSampleJob(workerId), new ArrayList<>(),
                false, 0, new ArrayList<>(),
                new ArrayList<>(), false);
    }

    public static AGroup mockAGroup(byte workerId, byte netWorkers, short slotNumber, CompressStats compressStats,
                                    int compressLevel, int trainSampleListMaxSize, SnowFlake snowFlake,
                                    TrainSampleJob trainSampleJob, List<TrainSampleJob.TrainSampleKV> sampleToTrainList,
                                    boolean localTest, int localTestRandomValueListSize, ArrayList<byte[]> localTestRandomValueList,
                                    ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed, boolean isCrossRequestWorker) {
        var aGroup = new AGroup("append", new byte[][]{new byte[0], new byte[0], new byte[0]}, null);
        aGroup.workerId = workerId;
        aGroup.netWorkers = netWorkers;
        aGroup.slotNumber = slotNumber;

        aGroup.compressStats = compressStats;

        aGroup.compressLevel = compressLevel;
        aGroup.trainSampleListMaxSize = trainSampleListMaxSize;

        aGroup.snowFlake = snowFlake;

        aGroup.trainSampleJob = trainSampleJob;
        aGroup.sampleToTrainList = sampleToTrainList;

        aGroup.localTest = localTest;
        aGroup.localTestRandomValueListSize = localTestRandomValueListSize;
        aGroup.localTestRandomValueList = localTestRandomValueList;

        aGroup.slotWithKeyHashListParsed = slotWithKeyHashListParsed;
        aGroup.isCrossRequestWorker = isCrossRequestWorker;
        return aGroup;
    }

    public void from(BaseCommand other) {
        this.netWorkers = other.netWorkers;
        this.slotNumber = other.slotNumber;

        this.compressStats = other.compressStats;

        this.compressLevel = other.compressLevel;
        this.trainSampleListMaxSize = other.trainSampleListMaxSize;

        this.snowFlake = other.snowFlake;

        this.trainSampleJob = other.trainSampleJob;
        this.sampleToTrainList = other.sampleToTrainList;

        this.localTest = other.localTest;
        this.localTestRandomValueListSize = other.localTestRandomValueListSize;
        this.localTestRandomValueList = other.localTestRandomValueList;

        this.slotWithKeyHashListParsed = other.slotWithKeyHashListParsed;
        this.isCrossRequestWorker = other.isCrossRequestWorker;

        if (other.byPassGetSet != null) {
            this.byPassGetSet = other.byPassGetSet;
        }
    }

    public BaseCommand init(RequestHandler requestHandler, Request request) {
        this.netWorkers = requestHandler.netWorkers;
        this.slotNumber = requestHandler.slotNumber;

        this.compressStats = requestHandler.compressStats;

        this.compressLevel = requestHandler.compressLevel;
        this.trainSampleListMaxSize = requestHandler.trainSampleListMaxSize;

        this.snowFlake = requestHandler.snowFlake;

        this.trainSampleJob = requestHandler.trainSampleJob;
        this.sampleToTrainList = requestHandler.sampleToTrainList;

        this.localTest = requestHandler.localTest;
        this.localTestRandomValueListSize = requestHandler.localTestRandomValueListSize;
        this.localTestRandomValueList = requestHandler.localTestRandomValueList;

        this.slotWithKeyHashListParsed = request.getSlotWithKeyHashList();
        this.isCrossRequestWorker = request.isCrossRequestWorker();
        return this;
    }

    public abstract Reply handle();

    protected final LocalPersist localPersist = LocalPersist.getInstance();

    protected static final Logger log = LoggerFactory.getLogger(BaseCommand.class);

    public record SlotWithKeyHash(short slot, int bucketIndex, long keyHash) {
        @Override
        public String toString() {
            return "SlotWithKeyHash{" +
                    "slot=" + slot +
                    ", bucketIndex=" + bucketIndex +
                    ", keyHash=" + keyHash +
                    '}';
        }
    }

    public static SlotWithKeyHash slot(byte[] keyBytes, int slotNumber) {
        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

        var keyHash = KeyHash.hash(keyBytes);
        final int halfSlotNumber = slotNumber / 2;
        final int x = halfSlotNumber * bucketsPerSlot;

        // check hash tag
        int hashTagBeginIndex = -1;
        int hashTagEndIndex = -1;
        for (int i = 0; i < keyBytes.length; i++) {
            if (keyBytes[i] == '{') {
                hashTagBeginIndex = i;
            } else if (keyBytes[i] == '}') {
                hashTagEndIndex = i;
            }
        }
        if (hashTagBeginIndex >= 0 && hashTagEndIndex > hashTagBeginIndex) {
            // hash tag
            var tagHash = KeyHash.hashOffset(keyBytes, hashTagBeginIndex + 1, hashTagEndIndex - hashTagBeginIndex - 1);

            // same slot, but not same bucket index
            var slotPositive = slotNumber == 1 ? 0 : Math.abs((tagHash / x) % halfSlotNumber);
            var slot = tagHash > 0 ? slotPositive : halfSlotNumber + slotPositive;
            var bucketIndex = Math.abs(keyHash % bucketsPerSlot);
            return new SlotWithKeyHash((short) slot, (int) bucketIndex, keyHash);
        }

        var slotPositive = slotNumber == 1 ? 0 : Math.abs((keyHash / x) % halfSlotNumber);
        var slot = keyHash > 0 ? slotPositive : halfSlotNumber + slotPositive;
        var bucketIndex = Math.abs(keyHash % bucketsPerSlot);
        return new SlotWithKeyHash((short) slot, (int) bucketIndex, keyHash);
    }

    protected SlotWithKeyHash slot(byte[] keyBytes) {
        return slot(keyBytes, slotNumber);
    }

    // for mock test
    private ByPassGetSet byPassGetSet;

    public void setByPassGetSet(ByPassGetSet byPassGetSet) {
        this.byPassGetSet = byPassGetSet;
    }

    public Long getExpireAt(byte[] keyBytes, @NotNull SlotWithKeyHash slotWithKeyHash) {
        if (byPassGetSet != null) {
            var cv = getCv(keyBytes, slotWithKeyHash);
            if (cv == null) {
                return null;
            }
            return cv.getExpireAt();
        } else {
            var slot = slotWithKeyHash.slot();
            var oneSlot = localPersist.oneSlot(slot);
            return oneSlot.getExpireAt(keyBytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash);
        }
    }

    public CompressedValue getCv(byte[] keyBytes) {
        return getCv(keyBytes, null);
    }

    public CompressedValue getCv(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse) {
        var slotWithKeyHash = slotWithKeyHashReuse != null ? slotWithKeyHashReuse : slot(keyBytes);
        var slot = slotWithKeyHash.slot();

        OneSlot.BufOrCompressedValue bufOrCompressedValue;
        if (byPassGetSet != null) {
            bufOrCompressedValue = byPassGetSet.getBuf(slot, keyBytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash);
        } else {
            var oneSlot = localPersist.oneSlot(slot);
            bufOrCompressedValue = oneSlot.get(keyBytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash);
        }

        if (bufOrCompressedValue == null) {
            return null;
        }

        var cv = bufOrCompressedValue.cv() != null ? bufOrCompressedValue.cv() :
                CompressedValue.decode(bufOrCompressedValue.buf(), keyBytes, slotWithKeyHash.keyHash());
        if (cv.isExpired()) {
            return null;
        }

        if (cv.isBigString()) {
            var oneSlot = localPersist.oneSlot(slot);

            var buffer = ByteBuffer.wrap(cv.getCompressedData());
            var uuid = buffer.getLong();
            var realDictSeq = buffer.getInt();

            var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid, true);
            if (bigStringBytes == null) {
                return null;
            }

            cv.setCompressedData(bigStringBytes);
            cv.setCompressedLength(bigStringBytes.length);
            cv.setDictSeqOrSpType(realDictSeq);
        }

        return cv;
    }

    public byte[] getValueBytesByCv(CompressedValue cv) {
        if (cv.isTypeNumber()) {
            return String.valueOf(cv.numberValue()).getBytes();
        }

        if (cv.isCompressed()) {
            Dict dict = null;
            if (cv.isUseDict()) {
                var dictSeqOrSpType = cv.getDictSeqOrSpType();
                if (dictSeqOrSpType == Dict.SELF_ZSTD_DICT_SEQ) {
                    dict = Dict.SELF_ZSTD_DICT;
                } else if (dictSeqOrSpType == Dict.GLOBAL_ZSTD_DICT_SEQ) {
                    if (!Dict.GLOBAL_ZSTD_DICT.hasDictBytes()) {
                        throw new DictMissingException("Global dict bytes not set");
                    }
                    dict = Dict.GLOBAL_ZSTD_DICT;
                } else {
                    dict = dictMap.getDictBySeq(dictSeqOrSpType);
                    if (dict == null) {
                        throw new DictMissingException("Dict not found, dict seq: " + dictSeqOrSpType);
                    }
                }
            }

            var beginT = System.nanoTime();
            var decompressed = cv.decompress(dict);
            var costT = (System.nanoTime() - beginT) / 1000;

            // stats
            compressStats.decompressedCount++;
            compressStats.decompressedCostTimeTotalUs += costT;

            return decompressed;
        } else {
            return cv.getCompressedData();
        }
    }

    public byte[] get(byte[] keyBytes) {
        return get(keyBytes, null);
    }

    public byte[] get(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse) {
        return get(keyBytes, slotWithKeyHashReuse, false);
    }

    private static boolean contain(Integer[] expectSpTypeArray, int spType) {
        for (var expectSpType : expectSpTypeArray) {
            if (expectSpType == spType) {
                return true;
            }
        }
        return false;
    }

    public byte[] get(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse, boolean expectTypeString, Integer... expectSpTypeArray) {
        var cv = getCv(keyBytes, slotWithKeyHashReuse);
        if (cv == null) {
            return null;
        }
        if (expectTypeString && !cv.isTypeString()) {
            throw new TypeMismatchException("Expect type string, but got sp type: " + cv.getDictSeqOrSpType());
        }
        if (expectSpTypeArray.length > 0 && !contain(expectSpTypeArray, cv.getDictSeqOrSpType())) {
            throw new TypeMismatchException("Expect sp type array: " + Arrays.toString(expectSpTypeArray) + ", but got sp type: " + cv.getDictSeqOrSpType());
        }
        return getValueBytesByCv(cv);
    }

    public void setNumber(byte[] keyBytes, Number value, SlotWithKeyHash slotWithKeyHashReuse) {
        setNumber(keyBytes, value, slotWithKeyHashReuse, NO_EXPIRE);
    }

    public void setNumber(byte[] keyBytes, Number value, SlotWithKeyHash slotWithKeyHashReuse, long expireAt) {
        if (value instanceof Short) {
            short shortValue = value.shortValue();
            if (shortValue <= Byte.MAX_VALUE && shortValue >= Byte.MIN_VALUE) {
                var newValueBytes = new byte[1];
                newValueBytes[0] = (byte) shortValue;
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_BYTE, expireAt);
            } else {
                var newValueBytes = new byte[2];
                ByteBuffer.wrap(newValueBytes).putShort(shortValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_SHORT, expireAt);
            }
        } else if (value instanceof Integer) {
            int intValue = value.intValue();
            if (intValue <= Byte.MAX_VALUE && intValue >= Byte.MIN_VALUE) {
                var newValueBytes = new byte[1];
                newValueBytes[0] = (byte) intValue;
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_BYTE, expireAt);
            } else if (intValue <= Short.MAX_VALUE && intValue >= Short.MIN_VALUE) {
                var newValueBytes = new byte[2];
                ByteBuffer.wrap(newValueBytes).putShort((short) intValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_SHORT, expireAt);
            } else {
                var newValueBytes = new byte[4];
                ByteBuffer.wrap(newValueBytes).putInt(intValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_INT, expireAt);
            }
        } else if (value instanceof Long) {
            long longValue = value.longValue();
            if (longValue <= Byte.MAX_VALUE && longValue >= Byte.MIN_VALUE) {
                var newValueBytes = new byte[1];
                newValueBytes[0] = (byte) longValue;
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_BYTE, expireAt);
            } else if (longValue <= Short.MAX_VALUE && longValue >= Short.MIN_VALUE) {
                var newValueBytes = new byte[2];
                ByteBuffer.wrap(newValueBytes).putShort((short) longValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_SHORT, expireAt);
            } else if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                var newValueBytes = new byte[4];
                ByteBuffer.wrap(newValueBytes).putInt((int) longValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_INT, expireAt);
            } else {
                var newValueBytes = new byte[8];
                ByteBuffer.wrap(newValueBytes).putLong(longValue);
                set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_LONG, expireAt);
            }
        } else if (value instanceof Double) {
            double doubleValue = value.doubleValue();
            var newValueBytes = new byte[8];
            ByteBuffer.wrap(newValueBytes).putDouble(doubleValue);
            set(keyBytes, newValueBytes, slotWithKeyHashReuse, CompressedValue.SP_TYPE_NUM_DOUBLE, expireAt);
        } else {
            throw new IllegalArgumentException("Not support number type: " + value.getClass());
        }
    }

    public void setCv(byte[] keyBytes, CompressedValue cv, SlotWithKeyHash slotWithKeyHashReuse) {
        var slotWithKeyHash = slotWithKeyHashReuse != null ? slotWithKeyHashReuse : slot(keyBytes);
        if (cv.isTypeNumber()) {
            setNumber(keyBytes, cv.numberValue(), slotWithKeyHash);
        } else {
            // update to new seq
            cv.setSeq(snowFlake.nextId());
            // update key hash
            cv.setKeyHash(slotWithKeyHash.keyHash());

            if (cv.isCompressed()) {
                // just set cv
                var dstKey = new String(keyBytes);
                var slot = slotWithKeyHash.slot();

                if (byPassGetSet != null) {
                    byPassGetSet.put(slot, dstKey, slotWithKeyHash.bucketIndex, cv);
                } else {
                    var oneSlot = localPersist.oneSlot(slot);
                    try {
                        oneSlot.put(dstKey, slotWithKeyHash.bucketIndex, cv);
                    } catch (SegmentOverflowException e) {
                        log.error("Set error, key: {}, message: {}", dstKey, e.getMessage());
                        throw e;
                    } catch (Exception e) {
                        var message = e.getMessage();
                        log.error("Set error, key: {}, message: {}", dstKey, message);
                        throw e;
                    }
                }
            } else {
                set(keyBytes, cv.getCompressedData(), slotWithKeyHash, 0, cv.getExpireAt());
            }
        }
    }

    public void set(byte[] keyBytes, byte[] valueBytes) {
        set(keyBytes, valueBytes, null, NULL_DICT_SEQ, NO_EXPIRE);
    }

    public void set(byte[] keyBytes, byte[] valueBytes, SlotWithKeyHash slotWithKeyHashReuse) {
        set(keyBytes, valueBytes, slotWithKeyHashReuse, NULL_DICT_SEQ, NO_EXPIRE);
    }

    public void set(byte[] keyBytes, byte[] valueBytes, SlotWithKeyHash slotWithKeyHashReuse, int spType) {
        set(keyBytes, valueBytes, slotWithKeyHashReuse, spType, NO_EXPIRE);
    }

    private static final int MAX_LONG_VALUE_IN_BYTES_LENGTH = String.valueOf(Long.MAX_VALUE).length();

    public void set(byte[] keyBytes, byte[] valueBytes, SlotWithKeyHash slotWithKeyHashReuse, int spType, long expireAt) {
        compressStats.rawTotalLength += valueBytes.length;

        // prefer store as number type
        boolean isTypeNumber = CompressedValue.isTypeNumber(spType);
        if (valueBytes.length <= MAX_LONG_VALUE_IN_BYTES_LENGTH && !isTypeNumber) {
            var value = new String(valueBytes);

            // check if value is a number
            long longValue;
            try {
                longValue = Long.parseLong(value);
                setNumber(keyBytes, longValue, slotWithKeyHashReuse, expireAt);
                return;
            } catch (NumberFormatException ignore) {
            }

            double doubleValue = 0;
            try {
                doubleValue = Double.parseDouble(value);
                setNumber(keyBytes, doubleValue, slotWithKeyHashReuse, expireAt);
                return;
            } catch (NumberFormatException ignore) {
            }
        }

        var slotWithKeyHash = slotWithKeyHashReuse != null ? slotWithKeyHashReuse : slot(keyBytes);
        var slot = slotWithKeyHash.slot;

        var key = new String(keyBytes);

        Dict dict = null;
        boolean isTypeString = CompressedValue.isTypeString(spType);
        if (isTypeString && ConfForGlobal.isValueSetUseCompression && valueBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH) {
            // use global dict first
            if (Dict.GLOBAL_ZSTD_DICT.hasDictBytes()) {
                dict = Dict.GLOBAL_ZSTD_DICT;
            } else {
                var keyPrefixOrSuffix = TrainSampleJob.keyPrefixOrSuffixGroup(key);
                dict = dictMap.getDict(keyPrefixOrSuffix);

                if (dict == null) {
                    dict = Dict.SELF_ZSTD_DICT;
                }
            }
        }

        if (ConfForGlobal.isValueSetUseCompression &&
                valueBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH &&
                dict != null) {
            var beginT = System.nanoTime();
            // dict may be null
            var cv = CompressedValue.compress(valueBytes, dict, compressLevel);
            var costT = (System.nanoTime() - beginT) / 1000;
            cv.setSeq(snowFlake.nextId());
            if (cv.isIgnoreCompression(valueBytes)) {
                cv.setDictSeqOrSpType(NULL_DICT_SEQ);
            } else {
                cv.setDictSeqOrSpType(dict != null ? dict.getSeq() : spType);
            }
            cv.setKeyHash(slotWithKeyHash.keyHash);
            cv.setExpireAt(expireAt);

            if (byPassGetSet != null) {
                byPassGetSet.put(slot, key, slotWithKeyHash.bucketIndex, cv);
            } else {
                var oneSlot = localPersist.oneSlot(slot);
                try {
                    oneSlot.put(key, slotWithKeyHash.bucketIndex, cv);
                } catch (SegmentOverflowException e) {
                    log.error("Set error, key: {}, message: {}", key, e.getMessage());
                    throw e;
                } catch (Exception e) {
                    var message = e.getMessage();
                    log.error("Set error, key: {}, message: {}", key, message);
                    throw e;
                }
            }

            // stats
            compressStats.compressedCount++;
            compressStats.compressedTotalLength += cv.getCompressedLength();
            compressStats.compressedCostTimeTotalUs += costT;

            if (ConfForGlobal.isOnDynTrainDictForCompression) {
                if (dict == Dict.SELF_ZSTD_DICT) {
                    // add train sample list
                    if (sampleToTrainList.size() < trainSampleListMaxSize) {
                        var kv = new TrainSampleJob.TrainSampleKV(key, null, cv.getSeq(), valueBytes);
                        sampleToTrainList.add(kv);
                    } else {
                        // no async train, latency sensitive
                        trainSampleJob.resetSampleToTrainList(sampleToTrainList);
                        handleTrainSampleResult(trainSampleJob.train());
                    }
                }
            }
        } else {
            var cvRaw = new CompressedValue();
            cvRaw.setSeq(snowFlake.nextId());
            cvRaw.setDictSeqOrSpType(spType);
            cvRaw.setKeyHash(slotWithKeyHash.keyHash);
            cvRaw.setExpireAt(expireAt);
            cvRaw.setUncompressedLength(valueBytes.length);
            cvRaw.setCompressedLength(valueBytes.length);
            cvRaw.setCompressedData(valueBytes);

            if (byPassGetSet != null) {
                byPassGetSet.put(slot, key, slotWithKeyHash.bucketIndex, cvRaw);
            } else {
                var oneSlot = localPersist.oneSlot(slot);
                try {
                    // uncompressed
                    oneSlot.put(key, slotWithKeyHash.bucketIndex, cvRaw);
                } catch (SegmentOverflowException e) {
                    log.error("Set error, key: {}, message: {}", key, e.getMessage());
                    throw e;
                } catch (IllegalStateException e) {
                    var message = e.getMessage();
                    log.error("Set error, key: {}, message: {}", key, message);
                    throw e;
                }
            }

            if (ConfForGlobal.isValueSetUseCompression && ConfForGlobal.isOnDynTrainDictForCompression) {
                // add train sample list
                if (sampleToTrainList.size() < trainSampleListMaxSize) {
                    if (valueBytes.length >= DictMap.TO_COMPRESS_MIN_DATA_LENGTH) {
                        var kv = new TrainSampleJob.TrainSampleKV(key, null, cvRaw.getSeq(), valueBytes);
                        sampleToTrainList.add(kv);
                    }
                } else {
                    // no async train, latency sensitive
                    trainSampleJob.resetSampleToTrainList(sampleToTrainList);
                    handleTrainSampleResult(trainSampleJob.train());
                }

                // stats
                compressStats.rawCount++;
                compressStats.compressedTotalLength += valueBytes.length;
            }
        }
    }

    public boolean remove(short slot, int bucketIndex, String key, long keyHash) {
        if (byPassGetSet != null) {
            return byPassGetSet.remove(slot, key);
        }

        var oneSlot = localPersist.oneSlot(slot);
        return oneSlot.remove(key, bucketIndex, keyHash);
    }

    public void removeDelay(short slot, int bucketIndex, String key, long keyHash) {
        if (byPassGetSet != null) {
            byPassGetSet.remove(slot, key);
            return;
        }

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.removeDelay(key, bucketIndex, keyHash);
    }

    public boolean exists(short slot, int bucketIndex, String key, long keyHash) {
        if (byPassGetSet != null) {
            var bufOrCompressedValue = byPassGetSet.getBuf(slot, key.getBytes(), bucketIndex, keyHash);
            return bufOrCompressedValue != null;
        }

        var oneSlot = localPersist.oneSlot(slot);
        return oneSlot.exists(key, bucketIndex, keyHash);
    }

    void handleTrainSampleResult(TrainSampleJob.TrainSampleResult trainSampleResult) {
        if (trainSampleResult == null) {
            return;
        }

        var trainSampleCacheDict = trainSampleResult.cacheDict();
        if (!trainSampleCacheDict.isEmpty()) {
            // train success, set dict in this worker
            for (var entry : trainSampleCacheDict.entrySet()) {
                var keyPrefix = entry.getKey();
                var dict = entry.getValue();
                dictMap.putDict(keyPrefix, dict);
//                var oldDict = dictMap.putDict(keyPrefixOrSuffix, dict);
//                if (oldDict != null) {
//                    // keep old dict in persist, because may be used by other worker
//                    // when start server, early dict will be overwritten by new dict with same key prefix, need not persist again?
//                    dictMap.putDict(keyPrefixOrSuffix + "_" + new Random().nextInt(10000), oldDict);
//                }
            }
        }

        var removedSampleKVSeqList = trainSampleResult.removedSampleKVSeqList();
        if (!removedSampleKVSeqList.isEmpty()) {
            // remove sample already trained
            sampleToTrainList.removeIf(kv -> removedSampleKVSeqList.contains(kv.seq()));
        }
    }
}
