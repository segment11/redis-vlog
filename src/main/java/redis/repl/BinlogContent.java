package redis.repl;

import redis.repl.incremental.*;

import java.nio.ByteBuffer;

public interface BinlogContent {
    enum Type {
        // code need > 0
        wal((byte) 1), one_wal_group_persist((byte) 2),
        big_strings((byte) 10), dict((byte) 100), dyn_config(Byte.MAX_VALUE);

        private final byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static Type fromCode(byte code) {
            for (var type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid binlog type code: " + code);
        }

        BinlogContent decodeFrom(ByteBuffer buffer) {
            switch (this) {
                case wal:
                    return XWalV.decodeFrom(buffer);
                case one_wal_group_persist:
                    return XOneWalGroupPersist.decodeFrom(buffer);
                case big_strings:
                    return XBigStrings.decodeFrom(buffer);
                case dict:
                    return XDict.decodeFrom(buffer);
                case dyn_config:
                    return XDynConfig.decodeFrom(buffer);
                default:
                    throw new IllegalArgumentException("Invalid binlog type: " + this);
            }
        }
    }

    Type type();

    int encodedLength();

    byte[] encodeWithType();

    void apply(byte slot, ReplPair replPair);
}
