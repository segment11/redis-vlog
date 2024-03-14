
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.ConfForSlot;
import redis.repl.Repl;
import redis.repl.ReplPair;
import redis.repl.ReplType;
import redis.repl.content.Hi;
import redis.repl.content.Pong;
import redis.reply.NilReply;
import redis.reply.Reply;

import java.nio.ByteBuffer;

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
            case chunk_segment -> chunk_segment(slot, contentBytes);
            case chunk_merge_flag_mmap -> chunk_merge_flag_mmap(slot, contentBytes);
            case chunk_segment_index_mmap -> chunk_segment_index_mmap(slot, contentBytes);
            case top_chunk_segment_index_mmap -> top_chunk_segment_index_mmap(slot, contentBytes);
            case big_string -> big_string(slot, contentBytes);
            case meta_chunk_segment -> meta_chunk_segment(slot, contentBytes);
            case meta_chunk_merge_flag_mmap -> meta_chunk_merge_flag_mmap(slot, contentBytes);
            case meta_chunk_segment_index_mmap -> meta_chunk_segment_index_mmap(slot, contentBytes);
            case meta_top_chunk_segment_index_mmap -> meta_top_chunk_segment_index_mmap(slot, contentBytes);
            case meta_big_string -> meta_big_string(slot, contentBytes);
            case key_bucket -> key_bucket(slot, contentBytes);
            case wal -> wal(slot, contentBytes);
            case dict -> dict(slot, contentBytes);
            case meta_key_bucket -> meta_key_bucket(slot, contentBytes);
            case meta_wal -> meta_wal(slot, contentBytes);
            case meta_dict -> meta_dict(slot, contentBytes);
        };
    }

    private Reply meta_dict(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_wal(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_key_bucket(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply dict(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply wal(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply key_bucket(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_big_string(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_top_chunk_segment_index_mmap(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_chunk_segment_index_mmap(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_chunk_merge_flag_mmap(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply meta_chunk_segment(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply big_string(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply top_chunk_segment_index_mmap(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply chunk_segment_index_mmap(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply chunk_merge_flag_mmap(byte slot, byte[] contentBytes) {
        return null;
    }

    private Reply chunk_segment(byte slot, byte[] contentBytes) {
        return null;
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
}
