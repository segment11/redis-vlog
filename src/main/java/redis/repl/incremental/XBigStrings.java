package redis.repl.incremental;

import redis.CompressedValue;
import redis.repl.BinlogContent;

import java.nio.ByteBuffer;

public class XBigStrings implements BinlogContent<XBigStrings> {
    private final long uuid;

    private final String key;

    public long getUuid() {
        return uuid;
    }

    public String getKey() {
        return key;
    }

    public XBigStrings(long uuid, String key) {
        this.uuid = uuid;
        this.key = key;
    }

    @Override
    public Type type() {
        return Type.big_strings;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 8 bytes for uuid, 2 bytes for key length, key bytes
        return 1 + 4 + 8 + 2 + key.length();
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.putLong(uuid);
        buffer.putShort((short) key.length());
        buffer.put(key.getBytes());

        return bytes;
    }

    public static XBigStrings decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var uuid = buffer.getLong();
        var keyLength = buffer.getShort();

        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length: " + keyLength);
        }

        var keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        var key = new String(keyBytes);

        var r = new XBigStrings(uuid, key);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length: " + encodedLength);
        }
        return r;
    }

    @Override
    public void apply(byte slot) {
        // add pull job to job list
    }
}