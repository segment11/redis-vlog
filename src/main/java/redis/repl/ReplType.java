package redis.repl;

public enum ReplType {
    error(true, false, (byte) -1),
    ping(true, true, (byte) 0),
    pong(true, false, (byte) 1),
    hello(true, true, (byte) 2),
    hi(true, false, (byte) 3),
    ok(true, false, (byte) 4),
    bye(true, true, (byte) 5),
    byeBye(true, false, (byte) 6),

    wal_append_batch(true, false, (byte) 10),
    dict_create(true, false, (byte) 11),
    big_string_file_write(true, false, (byte) 12),

    exists_chunk_segments(false, true, (byte) 20),
    exists_key_buckets(false, true, (byte) 21),
    meta_chunk_segment_index(false, true, (byte) 22),
    meta_key_bucket_split_number(false, true, (byte) 25),
    exists_big_string(false, true, (byte) 26),
    exists_dict(false, true, (byte) 27),
    exists_all_done(false, true, (byte) 28),

    s_exists_chunk_segments(false, false, (byte) 30),
    s_exists_key_buckets(false, false, (byte) 31),
    s_meta_chunk_segment_index(false, false, (byte) 32),
    s_meta_key_bucket_split_number(false, true, (byte) 35),
    s_exists_big_string(false, false, (byte) 36),
    s_exists_dict(false, false, (byte) 37),
    s_exists_all_done(false, false, (byte) 38),
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
