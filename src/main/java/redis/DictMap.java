package redis;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.repl.Binlog;
import redis.repl.SlaveNeedReplay;
import redis.repl.SlaveReplay;
import redis.repl.incremental.XDict;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class DictMap implements NeedCleanUp {
    public static int TO_COMPRESS_MIN_DATA_LENGTH = 64;
    // singleton
    private static final DictMap instance = new DictMap();

    public static DictMap getInstance() {
        return instance;
    }

    private DictMap() {
    }

    private Binlog binlog;

    public void setBinlog(Binlog binlog) {
        this.binlog = binlog;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public Dict getDictBySeq(int seq) {
        return cacheDictBySeq.get(seq);
    }

    public Dict getDict(String keyPrefix) {
        return cacheDict.get(keyPrefix);
    }

    @SlaveNeedReplay
    @SlaveReplay
    public synchronized Dict putDict(String keyPrefixOrSuffix, Dict dict) {
        // check dict seq is already in cache
        var existDict = cacheDictBySeq.get(dict.seq);
        if (existDict != null) {
            // generate new seq
            dict.seq = Dict.generateRandomSeq();
            // check again
            var existDict2 = cacheDictBySeq.get(dict.seq);
            if (existDict2 != null) {
                throw new RuntimeException("Dict seq conflict, dict seq: " + dict.seq);
            }
        }

        synchronized (fos) {
            try {
                fos.write(dict.encode(keyPrefixOrSuffix));
            } catch (IOException e) {
                throw new RuntimeException("Write dict to file error", e);
            }
        }

        if (binlog != null) {
            try {
                binlog.append(new XDict(keyPrefixOrSuffix, dict));
            } catch (IOException e) {
                throw new RuntimeException("Append binlog error, dict key prefix: " + keyPrefixOrSuffix, e);
            }
        }

        TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefixOrSuffix);

        cacheDictBySeq.put(dict.seq, dict);
        return cacheDict.put(keyPrefixOrSuffix, dict);
    }

    public HashMap<String, Dict> getCacheDictCopy() {
        return new HashMap<>(cacheDict);
    }

    public TreeMap<Integer, Dict> getCacheDictBySeqCopy() {
        return new TreeMap<>(cacheDictBySeq);
    }

    // worker share dict, init on start, need persist
    // for compress
    private final ConcurrentHashMap<String, Dict> cacheDict = new ConcurrentHashMap<>();
    // can not be removed
    // for decompress
    // if dict retrain, and dict count is large, it will be a problem, need clean not used dict, todo
    private final ConcurrentHashMap<Integer, Dict> cacheDictBySeq = new ConcurrentHashMap<>();

    public int dictSize() {
        return cacheDictBySeq.size();
    }

    private FileOutputStream fos;

    @Override
    public void cleanUp() {
        if (fos != null) {
            synchronized (fos) {
                try {
                    fos.close();
                    System.out.println("Close dict fos");
                } catch (IOException e) {
                    System.err.println("Close dict fos error, message: " + e.getMessage());
                }
            }
        }
    }

    public void clearAll() {
        cacheDict.clear();
        cacheDictBySeq.clear();

        // truncate file
        synchronized (fos) {
            try {
                fos.getChannel().truncate(0);
                System.out.println("Truncate dict file");
            } catch (IOException e) {
                throw new RuntimeException("Truncate dict file error", e);
            }
        }
    }

    private static final String FILE_NAME = "dict-map.dat";
    private File dirFile;

    public void initDictMap(File dirFile) throws IOException {
        this.dirFile = dirFile;
        var file = new File(dirFile, FILE_NAME);
        if (!file.exists()) {
            FileUtils.touch(file);
        }

        this.fos = new FileOutputStream(file, true);

        int n = 0;
        ArrayList<Integer> loadedSeqList = new ArrayList<>();
        if (file.length() > 0) {
            var is = new DataInputStream(new FileInputStream(file));
            while (true) {
                var dictWithKey = Dict.decode(is);
                if (dictWithKey == null) {
                    break;
                }

                var dict = dictWithKey.dict();
                cacheDict.put(dictWithKey.keyPrefixOrSuffix(), dict);
                cacheDictBySeq.put(dict.seq, dict);

                loadedSeqList.add(dict.seq);
                n++;
            }
        }

        log.info("Dict map init, map size: {}, seq map size: {}, n: {}, loaded seq list: {}",
                cacheDict.size(), cacheDictBySeq.size(), n, loadedSeqList);

        // add exists dict key prefix as train sample key prefix group, so new request values can use exist dict directly
        for (var entry : cacheDict.entrySet()) {
            var keyPrefix = entry.getKey();
            TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefix);
        }

        Dict.resetGlobalDictBytesByFile(new File(dirFile, Dict.GLOBAL_DICT_FILE_NAME), false);
    }

    public void updateGlobalDictBytes(byte[] dictBytes) {
        Dict.GLOBAL_ZSTD_DICT.setDictBytes(dictBytes);
        log.warn("Dict global dict bytes updated, dict bytes length: {}", dictBytes.length);
        Dict.saveGlobalDictBytesToFile(new File(dirFile, Dict.GLOBAL_DICT_FILE_NAME));
    }
}
