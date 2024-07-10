package redis;

import com.github.luben.zstd.ZstdDictTrainer;
import com.github.luben.zstd.ZstdException;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TrainSampleJob {
    private final byte workerId;

    int trainCount = 0;

    public TrainSampleJob(byte workerId) {
        this.workerId = workerId;
    }

    private final Logger log = org.slf4j.LoggerFactory.getLogger(TrainSampleJob.class);

    public static final int MIN_TRAIN_SAMPLE_SIZE = 10;

    private final HashMap<String, Dict> cacheDict = new HashMap<>();
    private List<TrainSampleKV> sampleToTrainListCopy = new ArrayList<>();
    private final List<Long> removedSampleKVSeqList = new ArrayList<>();

    public void resetSampleToTrainList(List<TrainSampleKV> list) {
        sampleToTrainListCopy = new ArrayList<>(list);
        removedSampleKVSeqList.clear();
    }

    private int dictSize = 1024;

    public void setDictSize(int dictSize) {
        this.dictSize = dictSize;
    }

    private int trainSampleMinBodyLength = 4096;

    public void setTrainSampleMinBodyLength(int trainSampleMinBodyLength) {
        this.trainSampleMinBodyLength = trainSampleMinBodyLength;
    }

    // exclusive, e.g. 5 means 'abcdef'.substring(0, 5) == 'abcde'
    private static int dictKeyPrefixEndIndex = 5;

    public static void setDictKeyPrefixEndIndex(int dictKeyPrefixEndIndex) {
        TrainSampleJob.dictKeyPrefixEndIndex = dictKeyPrefixEndIndex;
    }

    private static ArrayList<String> keyPrefixGroupList = new ArrayList<>();

    // overwrite or add all
    public static void setKeyPrefixGroupList(ArrayList<String> keyPrefixGroupList) {
        TrainSampleJob.keyPrefixGroupList = keyPrefixGroupList;
    }

    private Dict trainNewDict(List<TrainSampleKV> list) {
        int sampleBodyLength = 0;
        int sampleNum = 0;
        List<TrainSampleKV> trainSampleList = new ArrayList<>();
        for (var one : list) {
            sampleBodyLength += one.valueBytes().length;
            sampleNum += 1;
            trainSampleList.add(one);

            if (sampleBodyLength >= trainSampleMinBodyLength && sampleNum > MIN_TRAIN_SAMPLE_SIZE) {
                break;
            }
        }

        // list is not empty, sampleBodyLength > 0
        var trainer = new ZstdDictTrainer(sampleBodyLength, dictSize);
        for (var one : trainSampleList) {
            var body = one.valueBytes();
            boolean isAddSampleOk = trainer.addSample(body);
            assert isAddSampleOk;
//            boolean isAddSampleOk = trainer.addSample(body);
//            if (!isAddSampleOk) {
//                log.warn("Train sample, w={}, train dict add sample fail, sample size: {}, add body size: {}",
//                        workerId, sampleBodyLength, body.length);
//            }
        }

        byte[] dictBytes;
        try {
            var beginT = System.currentTimeMillis();
            dictBytes = trainer.trainSamples();
            var costT = System.currentTimeMillis() - beginT;

            log.info("Train sample, w={} train dict ok, sample size: {}, dict size: {}, cost time: {} ms",
                    workerId, sampleBodyLength, dictBytes.length, costT);
        } catch (ZstdException ze) {
            log.error("Train sample, w={} train dict, sample size: {}, error: {}",
                    workerId, sampleBodyLength, ze.getMessage());
            return null;
        }

        return new Dict(dictBytes);
    }

    public static String keyPrefix(String key) {
        if (!keyPrefixGroupList.isEmpty()) {
            for (var keyPrefix : keyPrefixGroupList) {
                if (key.startsWith(keyPrefix)) {
                    return keyPrefix;
                }
            }
        }

        // todo, maybe not good
        // prefer to use last index of '.' or ':'
        var index = key.lastIndexOf('.');
        if (index != -1) {
            return key.substring(0, index);
        } else {
            var index2 = key.lastIndexOf(':');
            if (index2 != -1) {
                // include :
                return key.substring(0, index2 + 1);
            } else {
                return key.substring(0, Math.min(key.length(), dictKeyPrefixEndIndex));
            }
        }
    }

    public TrainSampleResult train() {
        trainCount++;
        if (trainCount % 100 == 0 || Debug.getInstance().logTrainDict) {
            log.info("Train sample, worker {} train sample list size: {}, dict size: {}, i am alive",
                    workerId, sampleToTrainListCopy.size(), cacheDict.size());
        }

        if (sampleToTrainListCopy.size() <= MIN_TRAIN_SAMPLE_SIZE) {
            return null;
        }

        var groupByKeyPrefixMap = sampleToTrainListCopy.stream().collect(Collectors.groupingBy(one -> {
            if (one.keyPrefixGiven != null) {
                return one.keyPrefixGiven;
            }

            var key = one.key();
            return keyPrefix(key);
        }));

        for (var entry : groupByKeyPrefixMap.entrySet()) {
            var keyPrefix = entry.getKey();
            var list = entry.getValue();
            var dict = cacheDict.get(keyPrefix);
            if (dict != null) {
                for (var one : list) {
                    removedSampleKVSeqList.add(one.seq());
                }
                continue;
            }

            if (list.size() <= MIN_TRAIN_SAMPLE_SIZE) {
                // for next time
                continue;
            }

            dict = trainNewDict(list);
            if (dict != null) {
                // in one thread, no need lock
                cacheDict.put(keyPrefix, dict);

                // remove trained sample
                for (var one : list) {
                    removedSampleKVSeqList.add(one.seq());
                }
                log.info("Train sample, worker {} train dict ok, key prefix: {}, dict size: {}, removed sample size: {}",
                        workerId, keyPrefix, dict.dictBytes.length, list.size());

                // need persist immediately, todo
            }
        }
        return new TrainSampleResult(new HashMap<>(cacheDict), new ArrayList<>(removedSampleKVSeqList));
    }

    public record TrainSampleResult(HashMap<String, Dict> cacheDict, ArrayList<Long> removedSampleKVSeqList) {
    }

    public record TrainSampleKV(String key, String keyPrefixGiven, Long seq, byte[] valueBytes) {
    }
}