
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.ConfForSlot;
import redis.persist.Wal;
import redis.repl.Repl;
import redis.repl.ReplPair;
import redis.repl.ReplType;
import redis.repl.content.Hi;
import redis.repl.content.Pong;
import redis.reply.NilReply;
import redis.reply.Reply;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import static redis.repl.ReplType.hi;
import static redis.repl.ReplType.pong;

public class XGroup extends BaseCommand {
    public XGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public Reply handle() {
        return NilReply.INSTANCE;
    }

    public void setReplPair(ReplPair replPair) {
        this.replPair = replPair;
    }

    private ReplPair replPair;

    public Reply handleRepl() {
        var slaveUuid = ByteBuffer.wrap(data[0]).getLong();

        var slot = data[1][0];
        var replType = ReplType.fromCode(data[2][0]);
        if (replType == null) {
            log.error("Repl handle error: unknown repl type");
            return null;
        }
        var contentBytes = data[3];

        var oneSlot = localPersist.oneSlot(slot);

        // client side already set repl pair
        if (this.replPair == null) {
            this.replPair = oneSlot.getReplPair(slaveUuid);
        }

        return switch (replType) {
            case error -> {
                log.error("Repl handle error: {}", new String(contentBytes));
                yield null;
            }
            case ping -> {
                // server received ping from client
                var netListenAddresses = new String(contentBytes);
                var array = netListenAddresses.split(":");
                var host = array[0];
                var port = Integer.parseInt(array[1]);

                if (replPair == null) {
                    replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, host, port);
                }

                replPair.setLastPingGetTimestamp(System.currentTimeMillis());
                yield Repl.reply(slot, replPair, pong, new Pong(ConfForSlot.global.netListenAddresses));
            }
            case pong -> {
                // client received pong from server
                replPair.setLastPongGetTimestamp(System.currentTimeMillis());
                yield Repl.emptyReply();
            }
            case hello -> hello(slot, contentBytes);
            case hi -> hi(slot, contentBytes);
            case ok -> Repl.emptyReply();
            case bye -> {
                var netListenAddresses = new String(contentBytes);
                var array = netListenAddresses.split(":");
                var host = array[0];
                var port = Integer.parseInt(array[1]);

                log.warn("Repl handle bye: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);
                oneSlot.closeAndRemoveReplPair(slaveUuid, host, port);
                yield Repl.emptyReply();
            }
            case key_bucket_update -> key_bucket_update(slot, contentBytes);
            case wal_append_batch -> wal_append_batch(slot, contentBytes);
            case dict_create -> dict_create(slot, contentBytes);
            case segment_write -> segment_write(slot, contentBytes);
            case big_string_file_write -> big_string_file_write(slot, contentBytes);
            case exists_chunk_segments -> exists_chunk_segments(slot, contentBytes);
            case meta_chunk_segment_flag_seq -> meta_chunk_segment_flag_seq(slot, contentBytes);
            case meta_chunk_segment_index -> meta_chunk_segment_index(slot, contentBytes);
            case meta_top_chunk_segment_index -> meta_top_chunk_segment_index(slot, contentBytes);
            case meta_key_bucket_seq -> meta_key_bucket_seq(slot, contentBytes);
            case meta_key_bucket_split_number -> meta_key_bucket_split_number(slot, contentBytes);
            case exists_big_string -> exists_big_string(slot, contentBytes);
            case exists_dict -> exists_dict(slot, contentBytes);
        };
    }

    private Reply hello(byte slot, byte[] contentBytes) {
        // server received hello from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var slaveUuid = buffer.getLong();

        var b = new byte[contentBytes.length - 8];
        buffer.get(b);
        var netListenAddresses = new String(b);

        var array = netListenAddresses.split(":");
        var host = array[0];
        var port = Integer.parseInt(array[1]);

        var oneSlot = localPersist.oneSlot(slot);
        if (replPair == null) {
            replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, host, port);
        }

        log.warn("Repl handle hello: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);
        return Repl.reply(slot, replPair, hi, new Hi(slaveUuid, oneSlot.getMasterUuid()));
    }

    private Reply hi(byte slot, byte[] contentBytes) {
        // client received hi from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var slaveUuid = buffer.getLong();
        var masterUuid = buffer.getLong();

        if (slaveUuid != replPair.getSlaveUuid()) {
            log.error("Repl handle error: slave uuid not match, client slave uuid={}, server hi slave uuid={}",
                    replPair.getSlaveUuid(), slaveUuid);
            return null;
        }

        replPair.setMasterUuid(masterUuid);
        log.warn("Repl handle hi: slave uuid={}, master uuid={}", slaveUuid, masterUuid);

        // begin to fetch data from master, todo

        return Repl.emptyReply();
    }

    private Reply key_bucket_update(byte slot, byte[] contentBytes) {
        // client received hi from server
        return null;
    }

    private record ExtV(byte batchIndex, boolean isValueShort, int offset, Wal.V v) {
    }

    private Reply wal_append_batch(byte slot, byte[] contentBytes) {
        // client received hi from server
        // refer to ToSlaveWalAppendBatch.encodeTo
        var buffer = ByteBuffer.wrap(contentBytes);
        var batchCount = buffer.getShort();
        var dataLength = buffer.getInt();
        if (contentBytes.length != 2 + 4 + dataLength) {
            log.error("Repl handle error: wal append batch data length not match, batch count={}, data length={}, content bytes length={}",
                    batchCount, dataLength, contentBytes.length);
            return null;
        }

        // v already sorted by seq
        HashMap<Integer, ArrayList<ExtV>> groupByBucketIndex = new HashMap<>();
        try {
            for (int i = 0; i < batchCount; i++) {
                var batchIndex = buffer.get();
                var isValueShort = buffer.get() == 1;
                var offset = buffer.getInt();
                var vEncodeLength = buffer.getInt();
                var vEncodeBytes = new byte[vEncodeLength];
                buffer.get(vEncodeBytes);

                var is = new DataInputStream(new ByteArrayInputStream(vEncodeBytes));
                var v = Wal.V.decode(is);

                var bucketIndex = v.getBucketIndex();

                var vList = groupByBucketIndex.get(bucketIndex);
                if (vList == null) {
                    vList = new ArrayList<>();
                    groupByBucketIndex.put(bucketIndex, vList);
                }
                vList.add(new ExtV(batchIndex, isValueShort, offset, v));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var oneSlot = localPersist.oneSlot(slot);

        return null;
    }

    private Reply dict_create(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply segment_write(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply big_string_file_write(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply exists_chunk_segments(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_chunk_segment_flag_seq(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_chunk_segment_index(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_top_chunk_segment_index(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_key_bucket_seq(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_key_bucket_split_number(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply exists_big_string(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply exists_dict(byte slot, byte[] contentBytes) {
        return null;
    }
}
