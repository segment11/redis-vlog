package redis;

import com.github.luben.zstd.Zstd;
import io.activej.net.socket.tcp.ITcpSocket;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.command.AGroup;
import redis.decode.Request;
import redis.mock.ByPassGetSet;
import redis.persist.LocalPersist;
import redis.persist.SegmentOverflowException;
import redis.reply.Reply;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static redis.CompressedValue.NO_EXPIRE;
import static redis.CompressedValue.NULL_DICT_SEQ;
import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

public abstract class BaseCommand {
    protected final String cmd;
    protected final byte[][] data;
    protected final ITcpSocket socket;

    public BaseCommand(String cmd, byte[][] data, ITcpSocket socket) {
        this.cmd = cmd;
        this.data = data;
        this.socket = socket;
    }

    protected final DictMap dictMap = DictMap.getInstance();

    protected byte workerId;
    protected byte requestWorkers;
    protected short slotNumber;
    protected CompressStats compressStats;

    protected int compressLevel = Zstd.defaultCompressionLevel();
    protected int trainSampleListMaxSize = 1000;

    protected SnowFlake snowFlake;
    protected TrainSampleJob trainSampleJob;
    protected List<TrainSampleJob.TrainSampleKV> sampleToTrainList;

    protected boolean localTest;
    protected int localTestRandomValueListSize;
    protected ArrayList<byte[]> localTestRandomValueList;

    protected ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed;
    protected boolean isCrossRequestWorker;

    public static AGroup mockAGroup(byte workerId, byte requestWorkers, short slotNumber) {
        return mockAGroup(workerId, requestWorkers, slotNumber, new CompressStats(""),
                Zstd.defaultCompressionLevel(), 100, new SnowFlake(1, 1),
                new TrainSampleJob(workerId), new ArrayList<>(),
                false, 0, new ArrayList<>(),
                new ArrayList<>(), false);
    }

    public static AGroup mockAGroup(byte workerId, byte requestWorkers, short slotNumber, CompressStats compressStats,
                                    int compressLevel, int trainSampleListMaxSize, SnowFlake snowFlake,
                                    TrainSampleJob trainSampleJob, List<TrainSampleJob.TrainSampleKV> sampleToTrainList,
                                    boolean localTest, int localTestRandomValueListSize, ArrayList<byte[]> localTestRandomValueList,
                                    ArrayList<SlotWithKeyHash> slotWithKeyHashListParsed, boolean isCrossRequestWorker) {
        var aGroup = new AGroup("append", new byte[][]{new byte[0], new byte[0], new byte[0]}, null);
        aGroup.workerId = workerId;
        aGroup.requestWorkers = requestWorkers;
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
        this.workerId = other.workerId;
        this.requestWorkers = other.requestWorkers;
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
    }

    public BaseCommand init(RequestHandler requestHandler, Request request) {
        this.workerId = requestHandler.workerId;
        this.requestWorkers = requestHandler.requestWorkers;
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

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public record SlotWithKeyHash(byte slot, int bucketIndex, long keyHash) {
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
            return new SlotWithKeyHash((byte) slot, (int) bucketIndex, keyHash);
        }

        var slotPositive = slotNumber == 1 ? 0 : Math.abs((keyHash / x) % halfSlotNumber);
        var slot = keyHash > 0 ? slotPositive : halfSlotNumber + slotPositive;
        var bucketIndex = Math.abs(keyHash % bucketsPerSlot);
        return new SlotWithKeyHash((byte) slot, (int) bucketIndex, keyHash);
    }

    public SlotWithKeyHash slot(byte[] keyBytes) {
        return slot(keyBytes, slotNumber);
    }

    public SlotWithKeyHash slotPreferParsed(byte[] keyBytes, int index) {
        if (slotWithKeyHashListParsed != null && index < slotWithKeyHashListParsed.size()) {
            return slotWithKeyHashListParsed.get(index);
        }
        return slot(keyBytes, slotNumber);
    }

    public SlotWithKeyHash slotPreferParsed(byte[] keyBytes) {
        return slotPreferParsed(keyBytes, 0);
    }

    // for mock test
    private ByPassGetSet byPassGetSet;

    public void setByPassGetSet(ByPassGetSet byPassGetSet) {
        this.byPassGetSet = byPassGetSet;
    }

    public CompressedValue getCv(byte[] keyBytes) {
        return getCv(keyBytes, null);
    }

    public CompressedValue getCv(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse) {
        var slotWithKeyHash = slotWithKeyHashReuse != null ? slotWithKeyHashReuse : slot(keyBytes);
        var slot = slotWithKeyHash.slot();

        ByteBuf buf;
        if (byPassGetSet != null) {
            buf = byPassGetSet.getBuf(slot, keyBytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash);
        } else {
            var oneSlot = localPersist.oneSlot(slot);
            try {
                buf = oneSlot.get(keyBytes, slotWithKeyHash.bucketIndex, slotWithKeyHash.keyHash);
            } catch (Exception e) {
                log.error("Get error, key: {}, message: {}", new String(keyBytes), e.getMessage());
                return null;
            }
        }

        if (buf == null) {
            return null;
        }

        var cv = CompressedValue.decode(buf, keyBytes, slotWithKeyHash.keyHash(), false);
        if (cv.isExpired()) {
            return null;
        }

        if (cv.isBigString()) {
            var oneSlot = localPersist.oneSlot(slot);

            var buffer = ByteBuffer.wrap(cv.compressedData);
            var uuid = buffer.getLong();
            var realDictSeq = buffer.getInt();

            cv.dictSeqOrSpType = realDictSeq;
            cv.compressedData = oneSlot.getBigStringFiles().getBigStringBytesFromCache(uuid);
            if (cv.compressedData == null) {
                return null;
            }

            cv.compressedLength = cv.compressedData.length;
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
                if (cv.dictSeqOrSpType == Dict.SELF_ZSTD_DICT_SEQ) {
                    dict = Dict.SELF_ZSTD_DICT;
                } else {
                    dict = dictMap.getDictBySeq(cv.dictSeqOrSpType);
                    if (dict == null) {
                        throw new DictMissingException();
                    }
                }
            }

            long beginT = System.nanoTime();
            var decompressed = cv.decompress(dict);
            long costT = System.nanoTime() - beginT;

            // stats
            compressStats.decompressedCount++;
            compressStats.decompressedCostTotalTimeNanos += costT;

            return decompressed;
        } else {
            return cv.compressedData;
        }
    }

    public byte[] get(byte[] keyBytes) {
        return get(keyBytes, null);
    }

    public byte[] get(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse) {
        return get(keyBytes, slotWithKeyHashReuse, false);
    }

    public byte[] get(byte[] keyBytes, SlotWithKeyHash slotWithKeyHashReuse, boolean expectTypeString) {
        var cv = getCv(keyBytes, slotWithKeyHashReuse);
        if (cv == null) {
            return null;
        }
        if (expectTypeString && !cv.isTypeString()) {
            throw new TypeMismatchException("Expect type string, but got sp type: " + cv.getDictSeqOrSpType());
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
            // new seq
            cv.setSeq(snowFlake.nextId());
            // update key hash
            cv.setKeyHash(slotWithKeyHash.keyHash());

            if (cv.isCompressed()) {
                // just set cv
                var dstKey = new String(keyBytes);
                var slot = slotWithKeyHash.slot();

                var oneSlot = localPersist.oneSlot(slot);
                try {
                    oneSlot.put(workerId, dstKey, slotWithKeyHash.bucketIndex, cv);
                } catch (SegmentOverflowException e) {
                    log.error("Set error, key: {}, message: {}", dstKey, e.getMessage());
                    throw e;
                } catch (Exception e) {
                    var message = e.getMessage();
                    log.error("Set error, key: {}, message: {}", dstKey, message);
                    throw e;
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

    public void set(byte[] keyBytes, byte[] valueBytes, SlotWithKeyHash slotWithKeyHashReuse, int spType, long expireAt) {
        compressStats.rawValueBodyTotalLength += valueBytes.length;

        // prefer store as number type
        boolean isTypeNumber = CompressedValue.isTypeNumber(spType);
        if (valueBytes.length <= Long.SIZE && !isTypeNumber) {
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
        var keyPrefix = TrainSampleJob.keyPrefix(key);

        Dict dict = null;
        boolean isTypeString = CompressedValue.isTypeString(spType);
        if (isTypeString) {
            dict = dictMap.getDict(keyPrefix);

            if (dict == null && valueBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH) {
                dict = Dict.SELF_ZSTD_DICT;
            }
        }

        if (valueBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH && (CompressedValue.preferCompress(spType) || dict != null)) {
            long beginT = System.nanoTime();
            // dict may be null
            var cv = CompressedValue.compress(valueBytes, dict, compressLevel);
            long costT = System.nanoTime() - beginT;
            cv.seq = snowFlake.nextId();
            cv.dictSeqOrSpType = dict != null ? dict.seq : spType;
            cv.keyHash = slotWithKeyHash.keyHash;
            cv.expireAt = expireAt;

            if (byPassGetSet != null) {
                byPassGetSet.put(slot, workerId, key, slotWithKeyHash.bucketIndex, cv);
            } else {
                var oneSlot = localPersist.oneSlot(slot);
                try {
                    oneSlot.put(workerId, key, slotWithKeyHash.bucketIndex, cv);
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
            compressStats.compressedValueBodyTotalLength += cv.compressedLength;
            compressStats.compressedCostTotalTimeNanos += costT;

            if (dict != null && dict == Dict.SELF_ZSTD_DICT) {
                // add train sample list
                if (sampleToTrainList.size() < trainSampleListMaxSize) {
                    var kv = new TrainSampleJob.TrainSampleKV(key, null, cv.seq, valueBytes);
                    sampleToTrainList.add(kv);
                } else {
                    // no async train, latency sensitive
                    trainSampleJob.resetSampleToTrainList(sampleToTrainList);
                    handleTrainSampleResult(trainSampleJob.train());
                }
            }
        } else {
            var cvRaw = new CompressedValue();
            cvRaw.seq = snowFlake.nextId();
            cvRaw.dictSeqOrSpType = spType;
            cvRaw.keyHash = slotWithKeyHash.keyHash;
            cvRaw.expireAt = expireAt;
            cvRaw.uncompressedLength = valueBytes.length;
            cvRaw.compressedLength = cvRaw.uncompressedLength;
            cvRaw.compressedData = valueBytes;

            if (byPassGetSet != null) {
                byPassGetSet.put(slot, workerId, key, slotWithKeyHash.bucketIndex, cvRaw);
            } else {
                var oneSlot = localPersist.oneSlot(slot);
                try {
                    // uncompressed
                    oneSlot.put(workerId, key, slotWithKeyHash.bucketIndex, cvRaw);
                } catch (SegmentOverflowException e) {
                    log.error("Set error, key: {}, message: {}", key, e.getMessage());
                    throw e;
                } catch (IllegalStateException e) {
                    var message = e.getMessage();
                    log.error("Set error, key: {}, message: {}", key, message);
                    throw e;
                }
            }

            // add train sample list
            if (sampleToTrainList.size() < trainSampleListMaxSize) {
                var kv = new TrainSampleJob.TrainSampleKV(key, null, cvRaw.seq, valueBytes);
                sampleToTrainList.add(kv);
            } else {
                // no async train, latency sensitive
                trainSampleJob.resetSampleToTrainList(sampleToTrainList);
                handleTrainSampleResult(trainSampleJob.train());
            }

            // stats
            compressStats.rawCount++;
            compressStats.compressedValueBodyTotalLength += valueBytes.length;
        }
    }

    private synchronized void handleTrainSampleResult(TrainSampleJob.TrainSampleResult trainSampleResult) {
        if (trainSampleResult == null) {
            return;
        }

        var trainSampleCacheDict = trainSampleResult.cacheDict();
        if (!trainSampleCacheDict.isEmpty()) {
            // train success, set dict in this worker
            for (var entry : trainSampleCacheDict.entrySet()) {
                var dict = entry.getValue();
                var oldDict = dictMap.putDict(entry.getKey(), dict);
                if (oldDict != null) {
                    // keep old dict in persist, because may be used by other worker
                    dictMap.putDict(entry.getKey() + new Random().nextInt(100), oldDict);
                }
            }
        }

        var removedSampleKVSeqList = trainSampleResult.removedSampleKVSeqList();
        if (!removedSampleKVSeqList.isEmpty()) {
            // remove sample already trained
            sampleToTrainList.removeIf(kv -> removedSampleKVSeqList.contains(kv.seq()));
        }
    }
}
