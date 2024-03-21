package redis.repl;

public enum ReplType {
    error(true, false, (byte) -1),
    ping(true, true, (byte) 0),
    pong(true, false, (byte) 1),
    hello(true, true, (byte) 2),
    hi(true, false, (byte) 3),
    ok(true, false, (byte) 4),
    bye(true, true, (byte) 5),

    key_bucket_update(true, false, (byte) 10),
    wal_append_batch(true, false, (byte) 11),
    dict_create(true, false, (byte) 12),
    segment_write(true, false, (byte) 13),
    big_string_file_write(true, false, (byte) 14),

    exists_chunk_segments(false, true, (byte) 20),
    meta_chunk_segment_flag_seq(false, true, (byte) 21),
    meta_chunk_segment_index(false, true, (byte) 22),
    meta_top_chunk_segment_index(false, true, (byte) 23),
    meta_key_bucket_seq(false, true, (byte) 24),
    meta_key_bucket_split_number(false, true, (byte) 25),
    exists_big_string(false, true, (byte) 26),
    exists_dict(false, true, (byte) 27);

    public final boolean newly;
    public final boolean isClientSend;
    public final byte code;

    ReplType(boolean newly, boolean isClientSend, byte code) {
        this.newly = newly;
        this.isClientSend = isClientSend;
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
