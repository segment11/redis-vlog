package redis;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.repl.MasterUpdateCallback;

import java.io.*;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class DictMap {
    public static final int TO_COMPRESS_MIN_DATA_LENGTH = 64;
    public static final String ANONYMOUS_DICT_KEY = "x-anonymous";

    // singleton
    private static final DictMap instance = new DictMap();

    public static DictMap getInstance() {
        return instance;
    }

    private DictMap() {
    }

    private MasterUpdateCallback masterUpdateCallback;

    public void setMasterUpdateCallback(MasterUpdateCallback masterUpdateCallback) {
        if (this.masterUpdateCallback == null) {
            this.masterUpdateCallback = masterUpdateCallback;
        }
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    public Dict getDictBySeq(int seq) {
        return cacheDictBySeq.get(seq);
    }

    public Dict getDict(String keyPrefix) {
        return cacheDict.get(keyPrefix);
    }

    public Dict putDict(String keyPrefix, Dict dict) {
        synchronized (fos) {
            try {
                fos.write(dict.encode(keyPrefix));
            } catch (IOException e) {
                log.error("Write dict to file error", e);
            }
        }

        if (masterUpdateCallback != null) {
            masterUpdateCallback.onDictCreate(keyPrefix, dict);
        }

        cacheDictBySeq.put(dict.seq, dict);
        return cacheDict.put(keyPrefix, dict);
    }

    public HashMap<String, Dict> getCacheDictCopy() {
        return new HashMap<>(cacheDict);
    }

    public TreeMap<Integer, Dict> getCacheDictBySeqCopy() {
        return new TreeMap(cacheDictBySeq);
    }

    // worker share dict, init on start, need persist
    // for compress
    private ConcurrentHashMap<String, Dict> cacheDict = new ConcurrentHashMap<>();
    // can not be removed
    // for decompress
    // if dict retrain, and dict count is large, it will be a problem, need clean not used dict, todo
    private ConcurrentHashMap<Integer, Dict> cacheDictBySeq = new ConcurrentHashMap<>();

    public int dictSize() {
        return cacheDictBySeq.size();
    }

    private FileOutputStream fos;

    public void close() throws IOException {
        if (fos != null) {
            synchronized (fos) {
                fos.close();
                System.out.println("Close dict fos");
                fos = null;
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
                log.error("Truncate dict file error", e);
            }
        }
    }

    private static final String FILE_NAME = "dict-map.dat";

    public void initDictMap(File dirFile) throws IOException {
        var file = new File(dirFile, FILE_NAME);
        if (!file.exists()) {
            FileUtils.touch(file);
        }

        this.fos = new FileOutputStream(file, true);

        int n = 0;
        int maxSeq = 0;
        if (file.length() > 0) {
            var is = new DataInputStream(new FileInputStream(file));
            while (true) {
                var dictWithKey = Dict.decode(is);
                if (dictWithKey == null) {
                    break;
                }

                var dict = dictWithKey.dict();
                cacheDict.put(dictWithKey.keyPrefix(), dict);
                cacheDictBySeq.put(dict.seq, dict);

                if (dict.seq > maxSeq) {
                    maxSeq = dict.seq;
                }

                n++;
            }
        }

        log.info("Dict map init, map size: {}, seq map size: {}, n: {}, max seq: {}",
                cacheDict.size(), cacheDictBySeq.size(), n, maxSeq);
        Dict.seqGenerator.set(maxSeq + 1);
    }
}
