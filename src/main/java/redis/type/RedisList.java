package redis.type;

import io.netty.buffer.Unpooled;
import redis.KeyHash;

import java.util.Arrays;
import java.util.LinkedList;

public class RedisList {
    // change here to limit list size
    // values encoded compressed length should <= 4KB, suppose ratio is 0.25, then 16KB
    // suppose value length is 32, then 16KB / 32 = 512
    public static final short LIST_MAX_SIZE = 1024;

    // list size short + crc int
    private static final int HEADER_LENGTH = 2 + 4;

    private final LinkedList<byte[]> list = new LinkedList<>();

    public LinkedList<byte[]> getList() {
        return list;
    }

    public int size() {
        return list.size();
    }

    public void addFirst(byte[] e) {
        list.addFirst(e);
    }

    public void addLast(byte[] e) {
        list.add(e);
    }

    public void addAt(int index, byte[] e) {
        list.add(index, e);
    }

    public void setAt(int index, byte[] e) {
        list.set(index, e);
    }

    public int indexOf(byte[] b) {
        int i = 0;
        for (var e : list) {
            if (Arrays.equals(e, b)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public byte[] get(int index) {
        return list.get(index);
    }

    public byte[] removeFirst() {
        return list.removeFirst();
    }

    public byte[] removeLast() {
        return list.removeLast();
    }

    public byte[] encode() {
        int len = 0;
        for (var e : list) {
            // list value length use 2 bytes
            len += 2 + e.length;
        }

        var buf = Unpooled.buffer(len + HEADER_LENGTH);
        buf.writeShort(list.size());
        // tmp crc
        buf.writeInt(0);
        for (var e : list) {
            buf.writeShort((short) e.length);
            buf.writeBytes(e);
        }

        // crc
        if (len > 0) {
            var hb = buf.array();
            int crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buf.setInt(2, crc);
        }

        return buf.array();
    }

    public static RedisList decode(byte[] data) {
        var buf = Unpooled.wrappedBuffer(data);
        int size = buf.readShort();
        int crc = buf.readInt();

        // check crc
        if (size > 0) {
            int crcCompare = KeyHash.hash32Offset(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

        var r = new RedisList();
        for (int i = 0; i < size; i++) {
            int len = buf.readShort();
            var bytes = new byte[len];
            buf.readBytes(bytes);
            r.list.add(bytes);
        }
        return r;
    }
}
