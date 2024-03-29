package redis;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.repl.MasterUpdateCallback;
import redis.stats.OfStats;
import redis.stats.StatKV;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class DictMap implements OfStats {
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

    public Dict getDict(String key) {
        return cacheDict.get(key);
    }

    public Dict putDict(String key, Dict dict) {
        synchronized (fos) {
            try {
                fos.write(dict.encode(key));
            } catch (IOException e) {
                log.error("Write dict to file error", e);
            }
        }

        if (masterUpdateCallback != null) {
            masterUpdateCallback.onDictCreate(key, dict);
        }

        cacheDictBySeq.put(dict.seq, dict);
        return cacheDict.put(key, dict);
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
                cacheDict.put(dictWithKey.key(), dict);
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

    @Override
    public List<StatKV> stats() {
        List<StatKV> list = new ArrayList<>();
        list.add(new StatKV("global dict size", cacheDictBySeq.size()));
        list.add(StatKV.split);
        return list;
    }
}
