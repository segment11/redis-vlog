package redis.command;

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.AuxField;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.type.RedisHashKeys;
import redis.type.RedisList;
import redis.type.RedisZSet;

import java.util.Iterator;
import java.util.Map;

import static redis.CompressedValue.NO_EXPIRE;
import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

public class MyRDBVisitorEventListener implements EventListener {
    boolean isRDBBegun = false;
    boolean isRDBEnded = false;

    String ver;

    int keyCount = 0;

    private final Logger log = LoggerFactory.getLogger(MyRDBVisitorEventListener.class);

    private final LGroup lGroup;
    private final boolean onlyAnalysis;

    public MyRDBVisitorEventListener(LGroup lGroup, boolean onlyAnalysis) {
        this.lGroup = lGroup;
        this.onlyAnalysis = onlyAnalysis;
    }

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (isRDBBegun && !isRDBEnded) {
            if (event instanceof KeyValuePair) {
                var kv = (KeyValuePair) event;
                if (loadKv(kv)) {
                    keyCount++;
                }
            }
            return;
        }

        if (event instanceof PreRdbSyncEvent) {
            isRDBBegun = true;
            log.warn("RDB sync started");
        } else if (event instanceof PostRdbSyncEvent) {
            isRDBEnded = true;
            isRDBBegun = false;
            log.warn("RDB sync ended");
        } else if (event instanceof AuxField) {
            var auxField = (AuxField) event;
            if (auxField.getAuxKey().equals("redis-ver")) {
                ver = auxField.getAuxValue();
                log.info("redis version: {}", ver);
            }
        }
    }

    private void incrByBytes(byte[] keyBytes) {
        // todo
    }

    private boolean loadKv(KeyValuePair kv) {
        long expireAt = NO_EXPIRE;

        var expiredType = kv.getExpiredType();
        if (expiredType != ExpiredType.NONE) {
            var expiredValue = kv.getExpiredValue();

            if (expiredType == ExpiredType.MS) {
                expireAt = expiredValue;
            } else if (expiredType == ExpiredType.SECOND) {
                expireAt = expiredValue * 1000;
            }
        }

        var keyBytes = (byte[]) kv.getKey();
        if (onlyAnalysis) {
            incrByBytes(keyBytes);
            return true;
        }

        var key = new String(keyBytes);
        switch (kv.getValueRdbType()) {
            case RDB.RDBTypeString -> {
                var valueBytes = (byte[]) kv.getValue();
                lGroup.set(keyBytes, valueBytes, null, 0, expireAt);
                return true;
            }
            case RDB.RDBTypeList, RDB.RDBTypeListZipList, RDB.RDBTypeListQuickList, RDB.RDBTypeListQuickList2 -> {
                var it = (Iterator<byte[]>) kv.getValue();
                var rl = new RedisList();
                while (it.hasNext()) {
                    rl.addLast(it.next());
                }

                var encodedBytes = rl.encode();
                var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                var spType = needCompress ? CompressedValue.SP_TYPE_LIST_COMPRESSED : CompressedValue.SP_TYPE_LIST;

                lGroup.set(keyBytes, encodedBytes, null, spType, expireAt);
                return true;
            }
            case RDB.RDBTypeSet, RDB.RDBTypeSetIntSet, RDB.RDBTypeSetListPack -> {
                var it = (Iterator<byte[]>) kv.getValue();
                var rhk = new RedisHashKeys();
                while (it.hasNext()) {
                    rhk.add(new String(it.next()));
                }

                var encodedBytes = rhk.encode();
                var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                var spType = needCompress ? CompressedValue.SP_TYPE_SET_COMPRESSED : CompressedValue.SP_TYPE_SET;

                lGroup.set(keyBytes, encodedBytes, null, spType, expireAt);
                return true;
            }
            case RDB.RDBTypeZSet, RDB.RDBTypeZSet2, RDB.RDBTypeZSetZipList, RDB.RDBTypeZSetListPack -> {
                var it = (Iterator<ZSetEntry>) kv.getValue();
                var rz = new RedisZSet();
                while (it.hasNext()) {
                    var e = it.next();
                    rz.add(e.getScore(), new String(e.getElement()));
                }

                var encodedBytes = rz.encode();
                var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                var spType = needCompress ? CompressedValue.SP_TYPE_ZSET_COMPRESSED : CompressedValue.SP_TYPE_ZSET;

                lGroup.set(keyBytes, encodedBytes, null, spType, expireAt);
                return true;
            }
            case RDB.RDBTypeHash, RDB.RDBTypeHashZipMap, RDB.RDBTypeHashZipList, RDB.RDBTypeHashListPack -> {
                var keysKey = RedisHashKeys.keysKey(key);

                var it = (Iterator<Map.Entry<byte[], byte[]>>) kv.getValue();
                var rhk = new RedisHashKeys();
                while (it.hasNext()) {
                    var e = it.next();
                    var field = new String(e.getKey());
                    var fieldValueBytes = e.getValue();

                    rhk.add(field);

                    var fieldKey = RedisHashKeys.fieldKey(key, field);
                    lGroup.set(fieldKey.getBytes(), fieldValueBytes);
                }

                var encodedBytes = rhk.encode();
                var needCompress = encodedBytes.length >= TO_COMPRESS_MIN_DATA_LENGTH;
                var spType = needCompress ? CompressedValue.SP_TYPE_HASH_COMPRESSED : CompressedValue.SP_TYPE_HASH;

                lGroup.set(keysKey.getBytes(), encodedBytes, null, spType, expireAt);
                return true;
            }
            default -> {
                log.warn("unknown type: {}", kv.getValueRdbType());
                return false;
            }
        }
    }
}
