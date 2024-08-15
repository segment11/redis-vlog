package redis.repl.incremental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.CompressedValue;
import redis.Dict;
import redis.DictMap;
import redis.repl.BinlogContent;
import redis.repl.ReplPair;

import java.nio.ByteBuffer;

public class XDict implements BinlogContent {
    private final String keyPrefix;

    private final Dict dict;

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public Dict getDict() {
        return dict;
    }

    public XDict(String keyPrefix, Dict dict) {
        this.keyPrefix = keyPrefix;
        this.dict = dict;
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public Type type() {
        return BinlogContent.Type.dict;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 4 bytes for seq, 8 bytes for created time
        // 2 bytes for key prefix length, key prefix, 2 bytes for dict bytes length, dict bytes
        return 1 + 4 + 4 + 8 + 2 + keyPrefix.length() + 2 + dict.getDictBytes().length;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.putInt(dict.getSeq());
        buffer.putLong(dict.getCreatedTime());
        buffer.putShort((short) keyPrefix.length());
        buffer.put(keyPrefix.getBytes());
        buffer.putShort((short) dict.getDictBytes().length);
        buffer.put(dict.getDictBytes());

        return bytes;
    }

    public static XDict decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var seq = buffer.getInt();
        var createdTime = buffer.getLong();
        var keyPrefixLength = buffer.getShort();


        if (keyPrefixLength > CompressedValue.KEY_MAX_LENGTH || keyPrefixLength <= 0) {
            throw new IllegalStateException("Key prefix length error, key prefix length: " + keyPrefixLength);
        }

        var keyPrefixBytes = new byte[keyPrefixLength];
        buffer.get(keyPrefixBytes);
        var keyPrefix = new String(keyPrefixBytes);
        var dictBytesLength = buffer.getShort();
        var dictBytes = new byte[dictBytesLength];
        buffer.get(dictBytes);

        var dict = new Dict();
        dict.setSeq(seq);
        dict.setCreatedTime(createdTime);
        dict.setDictBytes(dictBytes);

        var r = new XDict(keyPrefix, dict);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length: " + encodedLength);
        }
        return r;
    }

    @Override
    public void apply(byte slot, ReplPair replPair) {
        log.warn("Repl slave get dict, key prefix: {}, seq: {}", keyPrefix, dict.getSeq());
        // ignore slot, need sync
        var dictMap = DictMap.getInstance();
        synchronized (dictMap) {
            dictMap.putDict(keyPrefix, dict);
        }
    }
}
