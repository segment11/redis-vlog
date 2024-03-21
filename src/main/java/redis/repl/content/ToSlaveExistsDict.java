package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.Dict;
import redis.repl.ReplContent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import static redis.DictMap.ANONYMOUS_DICT_KEY;

public class ToSlaveExistsDict implements ReplContent {
    private final HashMap<Dict, String> cacheKeyByDict;
    private final TreeMap<Integer, Dict> toSendCacheDictBySeq;
    private final boolean isSendAllOnce;

    private static final int ONCE_SEND_DICT_COUNT = 1000;

    public ToSlaveExistsDict(HashMap<String, Dict> cacheDict, TreeMap<Integer, Dict> cacheDictBySeq, ArrayList<Integer> sentDictSeqList) {
        var cacheKeyByDict = new HashMap<Dict, String>();
        for (var entry : cacheDict.entrySet()) {
            cacheKeyByDict.put(entry.getValue(), entry.getKey());
        }
        this.cacheKeyByDict = cacheKeyByDict;

        var toSendCacheDictBySeq = new TreeMap<Integer, Dict>();
        // exclude sent dict
        for (var entry : cacheDictBySeq.entrySet()) {
            if (!sentDictSeqList.contains(entry.getKey())) {
                toSendCacheDictBySeq.put(entry.getKey(), entry.getValue());
            }
        }

        this.isSendAllOnce = toSendCacheDictBySeq.size() <= ONCE_SEND_DICT_COUNT;
        if (!isSendAllOnce) {
            toSendCacheDictBySeq = new TreeMap<>(toSendCacheDictBySeq.subMap(0, ONCE_SEND_DICT_COUNT));
        }

        this.toSendCacheDictBySeq = toSendCacheDictBySeq;
    }

    // 2 bytes as short for send dict count, 1 byte as flag for is sent all
    private static final int HEADER_LENGTH = 2 + 1;

    @Override
    public void encodeTo(ByteBuf toBuf) {
        if (toSendCacheDictBySeq.isEmpty()) {
            toBuf.writeShort((short) 0);
            toBuf.put((byte) 1);
            return;
        }

        toBuf.writeShort((short) toSendCacheDictBySeq.size());
        toBuf.put((byte) (isSendAllOnce ? 1 : 0));

        for (var entry : toSendCacheDictBySeq.entrySet()) {
            var dict = entry.getValue();
            var key = cacheKeyByDict.get(dict);

            var encodeLength = key == null ? dict.encodeLength(ANONYMOUS_DICT_KEY) : dict.encodeLength(key);
            toBuf.writeInt(encodeLength);
            toBuf.put(dict.encode(key == null ? ANONYMOUS_DICT_KEY : key));
        }
    }

    @Override
    public int encodeLength() {
        if (toSendCacheDictBySeq.isEmpty()) {
            return HEADER_LENGTH;
        }

        var length = HEADER_LENGTH;
        for (var entry : toSendCacheDictBySeq.entrySet()) {
            var dict = entry.getValue();
            var key = cacheKeyByDict.get(dict);

            var encodeLength = key == null ? dict.encodeLength(ANONYMOUS_DICT_KEY) : dict.encodeLength(key);
            length += 4 + encodeLength;
        }
        return length;
    }
}
