package redis.repl;

public enum ReplType {
    error(true, false, (byte) -1),
    ping(true, true, (byte) 0),
    pong(true, false, (byte) 1),
    hello(true, true, (byte) 2),
    hi(true, false, (byte) 3),
    bye(true, true, (byte) 4),
    byeBye(true, false, (byte) 5),
    ok(true, false, (byte) 100),

    exists_chunk_segments(false, true, (byte) 20),
    exists_key_buckets(false, true, (byte) 21),
    meta_key_bucket_split_number(false, true, (byte) 22),
    exists_big_string(false, true, (byte) 23),
    exists_dict(false, true, (byte) 24),
    exists_all_done(false, true, (byte) 25),
    catch_up(true, true, (byte) 26),

    s_exists_chunk_segments(false, false, (byte) 30),
    s_exists_key_buckets(false, false, (byte) 31),
    s_meta_key_bucket_split_number(false, true, (byte) 32),
    s_exists_big_string(false, false, (byte) 33),
    s_exists_dict(false, false, (byte) 34),
    s_exists_all_done(false, false, (byte) 35),
    s_catch_up(true, false, (byte) 36),
    ;

    public final boolean newly;
    public final boolean isSlaveSend;
    public final byte code;

    ReplType(boolean newly, boolean isSlaveSend, byte code) {
        this.newly = newly;
        this.isSlaveSend = isSlaveSend;
        this.code = code;
    }

    public static ReplType fromCode(byte code) {
        for (var value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }
}
