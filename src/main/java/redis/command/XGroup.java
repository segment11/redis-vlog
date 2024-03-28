
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import org.apache.commons.io.FileUtils;
import redis.BaseCommand;
import redis.ConfForSlot;
import redis.Dict;
import redis.DictMap;
import redis.persist.OneSlot;
import redis.persist.Wal;
import redis.repl.Repl;
import redis.repl.ReplPair;
import redis.repl.ReplType;
import redis.repl.content.*;
import redis.reply.NilReply;
import redis.reply.Reply;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;

import static redis.repl.ReplType.hi;
import static redis.repl.ReplType.pong;

public class XGroup extends BaseCommand {
    public XGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        return null;
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
                // server received bye from client
                var netListenAddresses = new String(contentBytes);
                log.warn("Repl handle bye: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.reply(slot, replPair, ReplType.byeBye, new Ping(ConfForSlot.global.netListenAddresses));
            }
            case byeBye -> {
                // client received bye from server
                var netListenAddresses = new String(contentBytes);
                log.warn("Repl handle byeBye: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.emptyReply();
            }
            case key_bucket_update -> key_bucket_update(slot, contentBytes);
            case key_bucket_split -> key_bucket_split(slot, contentBytes);
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
            case exists_all_done -> exists_all_done(slot, contentBytes);
            case s_exists_chunk_segments -> s_exists_chunk_segments(slot, contentBytes);
            case s_meta_chunk_segment_flag_seq -> s_meta_chunk_segment_flag_seq(slot, contentBytes);
            case s_meta_chunk_segment_index -> s_meta_chunk_segment_index(slot, contentBytes);
            case s_meta_top_chunk_segment_index -> s_meta_top_chunk_segment_index(slot, contentBytes);
            case s_meta_key_bucket_seq -> s_meta_key_bucket_seq(slot, contentBytes);
            case s_meta_key_bucket_split_number -> s_meta_key_bucket_split_number(slot, contentBytes);
            case s_exists_big_string -> s_exists_big_string(slot, contentBytes);
            case s_exists_dict -> s_exists_dict(slot, contentBytes);
            case s_exists_all_done -> s_exists_all_done(slot, contentBytes);
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

        // begin to fetch exist data from master
        // first fetch dict
        var dictMap = DictMap.getInstance();
        var cacheDictBySeqCopy = dictMap.getCacheDictBySeqCopy();
        if (cacheDictBySeqCopy.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.exists_dict, new EmptyContent());
        } else {
            var rawBytes = new byte[4 * cacheDictBySeqCopy.size()];
            var rawBuffer = ByteBuffer.wrap(rawBytes);
            for (var entry : cacheDictBySeqCopy.entrySet()) {
                var seq = entry.getKey();
                rawBuffer.putInt(seq);
            }

            return Repl.reply(slot, replPair, ReplType.exists_dict, new RawBytesContent(rawBytes));
        }
    }

    private Reply key_bucket_update(byte slot, byte[] contentBytes) {
        // client received from server
        log.debug("Repl handle key bucket update, slot={}, slave uuid={}, {}:{}", slot,
                replPair.getSlaveUuid(), replPair.getHost(), replPair.getPort());

        // refer to ToSlaveKeyBucketUpdate.encodeTo
        var buffer = ByteBuffer.wrap(contentBytes);
        var bucketIndex = buffer.getInt();
        var splitIndex = buffer.get();
        var splitNumber = buffer.get();
        var seq = buffer.getLong();
        var bytesLength = buffer.getInt();
        var bytes = new byte[bytesLength];
        buffer.get(bytes);

        var oneSlot = localPersist.oneSlot(slot);
        // not necessary to submit task, key loader use synchronized block
        oneSlot.getKeyLoader().updateKeyBucketFromRepl(bucketIndex, splitIndex, splitNumber, seq, bytes);

        return Repl.emptyReply();
    }

    private Reply key_bucket_split(byte slot, byte[] contentBytes) {
        // client received from server
        log.debug("Repl handle key bucket split, slot={}, slave uuid={}, {}:{}", slot,
                replPair.getSlaveUuid(), replPair.getHost(), replPair.getPort());
        // refer to ToSlaveKeyBucketSplit.encodeTo
        var buffer = ByteBuffer.wrap(contentBytes);
        var bucketIndex = buffer.getInt();
        var splitNumber = buffer.get();

        var oneSlot = localPersist.oneSlot(slot);
        // not necessary to submit task, key loader use synchronized block
        oneSlot.getKeyLoader().setMetaKeyBucketSplitNumberFromRepl(bucketIndex, splitNumber);

        return Repl.emptyReply();
    }

    public record ExtV(byte batchIndex, boolean isValueShort, int offset, Wal.V v) {

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

            var oneSlot = localPersist.oneSlot(slot);
            oneSlot.addDelayNeedCloseReplPair(replPair);

            return Repl.reply(slot, replPair, ReplType.byeBye, new Ping(ConfForSlot.global.netListenAddresses));
        }

        // v already sorted by seq
        // sort by wal group index, debug better
        TreeMap<Integer, ArrayList<ExtV>> extVsGroupByWalGroupIndex = new TreeMap<>();
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

                var walGroupIndex = v.bucketIndex() / ConfForSlot.global.confWal.oneChargeBucketNumber;

                var vList = extVsGroupByWalGroupIndex.get(walGroupIndex);
                if (vList == null) {
                    vList = new ArrayList<>();
                    extVsGroupByWalGroupIndex.put(walGroupIndex, vList);
                }
                vList.add(new ExtV(batchIndex, isValueShort, offset, v));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var oneSlot = localPersist.oneSlot(slot);
        // need write to wal, perf too bad
        oneSlot.asSlaveOnMasterWalAppendBatchGet(extVsGroupByWalGroupIndex);

        return Repl.emptyReply();
    }

    private Reply dict_create(byte slot, byte[] contentBytes) {
        log.debug("Repl handle dict create, slot={}, slave uuid={}, {}:{}", slot,
                replPair.getSlaveUuid(), replPair.getHost(), replPair.getPort());
        // refer Dict.encode
        var is = new DataInputStream(new ByteArrayInputStream(contentBytes));
        try {
            var dictWithKey = Dict.decode(is);
            DictMap.getInstance().putDict(dictWithKey.key(), dictWithKey.dict());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Repl.emptyReply();
    }

    private Reply segment_write(byte slot, byte[] contentBytes) {
        log.debug("Repl handle segment write, slot={}, slave uuid={}, {}:{}", slot,
                replPair.getSlaveUuid(), replPair.getHost(), replPair.getPort());
        // refer to ToSlaveSegmentWrite.encodeTo
        var buffer = ByteBuffer.wrap(contentBytes);
        var workerId = buffer.get();
        var batchIndex = buffer.get();
        var segmentLength = buffer.getInt();
        var segmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();
        var segmentSeqList = new ArrayList<Long>();
        for (int i = 0; i < segmentCount; i++) {
            segmentSeqList.add(buffer.getLong());
        }
        var bytesLength = buffer.getInt();
        var bytes = new byte[bytesLength];
        buffer.get(bytes);
        var capacity = buffer.getInt();

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.submitPersistTaskFromRepl(workerId, batchIndex, segmentLength, segmentIndex, segmentCount,
                segmentSeqList, bytes, capacity);

        return Repl.emptyReply();
    }

    private Reply big_string_file_write(byte slot, byte[] contentBytes) {
        log.debug("Repl handle big string file write, slot={}, slave uuid={}, {}:{}", slot,
                replPair.getSlaveUuid(), replPair.getHost(), replPair.getPort());
        // refer to ToSlaveBigStringFileWrite.encodeTo
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();
        var encodeLength = buffer.remaining();
        var encodeBytes = new byte[encodeLength];
        buffer.get(encodeBytes);

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringDir = oneSlot.getBigStringDir();
        var uuidAsFileName = String.valueOf(uuid);

        var file = new File(bigStringDir, uuidAsFileName);
        try {
            FileUtils.writeByteArrayToFile(file, encodeBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Repl.emptyReply();
    }

    private Reply exists_chunk_segments(byte slot, byte[] contentBytes) {
        // server received from client, send back exists chunk segments to client, with flag can do next step
        return Repl.emptyReply();
    }

    private Reply s_exists_chunk_segments(byte slot, byte[] contentBytes) {
        // client received from server
        return Repl.emptyReply();
    }

    private Reply meta_chunk_segment_flag_seq(byte slot, byte[] contentBytes) {
        // server received from client
        return Repl.emptyReply();
    }

    private Reply s_meta_chunk_segment_flag_seq(byte slot, byte[] contentBytes) {
        // client received from server
        return Repl.emptyReply();
    }

    private Reply meta_chunk_segment_index(byte slot, byte[] contentBytes) {
        // server received from client
        return Repl.emptyReply();
    }

    private Reply s_meta_chunk_segment_index(byte slot, byte[] contentBytes) {
        // client received from server
        return Repl.emptyReply();
    }

    private Reply meta_top_chunk_segment_index(byte slot, byte[] contentBytes) {
        // server received from client
        return Repl.emptyReply();
    }

    private Reply s_meta_top_chunk_segment_index(byte slot, byte[] contentBytes) {
        // client received from server
        return Repl.emptyReply();
    }

    private Reply meta_key_bucket_seq(byte slot, byte[] contentBytes) {
        // server received from client
        return Repl.emptyReply();
    }

    private Reply s_meta_key_bucket_seq(byte slot, byte[] contentBytes) {
        // client received from server
        return Repl.emptyReply();
    }

    private Reply meta_key_bucket_split_number(byte slot, byte[] contentBytes) {
        // server received from client
        var oneSlot = localPersist.oneSlot(slot);
        var bytes = oneSlot.getKeyLoader().getMetaKeyBucketSplitNumberBytesForRepl();
        return Repl.reply(slot, replPair, ReplType.s_meta_key_bucket_split_number, new RawBytesContent(bytes));
    }

    private Reply s_meta_key_bucket_split_number(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getKeyLoader().overwriteMetaKeyBucketSplitNumberBytesFromRepl(contentBytes);
        return Repl.reply(slot, replPair, ReplType.meta_key_bucket_seq, new EmptyContent());
    }

    private Reply exists_big_string(byte slot, byte[] contentBytes) {
        // server received from client, send back exists big string to client, with flag can do next step
        // client already persisted big string uuid, send to client exclude sent big string
        ArrayList<Long> sentUuidList = new ArrayList<>();
        if (contentBytes.length >= 8) {
            var sentUuidCount = contentBytes.length / 8;

            var buffer = ByteBuffer.wrap(contentBytes);
            for (int i = 0; i < sentUuidCount; i++) {
                sentUuidList.add(buffer.getLong());
            }
        }

        var oneSlot = localPersist.oneSlot(slot);
        var uuidListInMaster = oneSlot.getBigStringFileUuidList();
        if (uuidListInMaster.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.s_exists_big_string, new EmptyContent());
        }

        var toSlaveExistsBigString = new ToSlaveExistsBigString(oneSlot.getBigStringDir(), uuidListInMaster, sentUuidList);
        return Repl.reply(slot, replPair, ReplType.s_exists_big_string, toSlaveExistsBigString);
    }

    // need delete local big string file if not exists in master, todo
    private Reply s_exists_big_string(byte slot, byte[] contentBytes) {
        // client received from server
        // empty content means no big string, next step
        if (contentBytes.length == 1) {
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, new EmptyContent());
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var bigStringCount = buffer.getShort();
        var isSendAllOnce = buffer.get() == 1;

        if (bigStringCount == 0) {
            // next step, fetch meta key bucket split number
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, new EmptyContent());
        }

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringDir = oneSlot.getBigStringDir();
        try {
            for (int i = 0; i < bigStringCount; i++) {
                var uuid = buffer.getLong();
                var encodeLength = buffer.getInt();
                var encodeBytes = new byte[encodeLength];
                buffer.get(encodeBytes);

                var uuidAsFileName = String.valueOf(uuid);
                var file = new File(bigStringDir, uuidAsFileName);
                FileUtils.writeByteArrayToFile(file, encodeBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (isSendAllOnce) {
            // next step, fetch big string
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, new EmptyContent());
        } else {
            return fetchExistsBigString(slot, oneSlot);
        }
    }

    private Reply fetchExistsBigString(byte slot, OneSlot oneSlot) {
        var uuidListLocal = oneSlot.getBigStringFileUuidList();
        if (uuidListLocal.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.exists_big_string, new EmptyContent());
        }

        var rawBytes = new byte[8 * uuidListLocal.size()];
        var rawBuffer = ByteBuffer.wrap(rawBytes);
        for (var uuid : uuidListLocal) {
            rawBuffer.putLong(uuid);
        }

        return Repl.reply(slot, replPair, ReplType.exists_big_string, new RawBytesContent(rawBytes));
    }

    private Reply exists_dict(byte slot, byte[] contentBytes) {
        // client already persisted dict seq, send to client exclude sent dict
        ArrayList<Integer> sentDictSeqList = new ArrayList<>();
        if (contentBytes.length >= 4) {
            var sentDictSeqCount = contentBytes.length / 4;

            var buffer = ByteBuffer.wrap(contentBytes);
            for (int i = 0; i < sentDictSeqCount; i++) {
                sentDictSeqList.add(buffer.getInt());
            }
        }

        // server received from client, send back exists dict to client, with flag can do next step
        var dictMap = DictMap.getInstance();
        var cacheDictCopy = dictMap.getCacheDictCopy();
        var cacheDictBySeqCopy = dictMap.getCacheDictBySeqCopy();

        if (cacheDictBySeqCopy.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.s_exists_dict, new EmptyContent());
        } else {
            var toSlaveExistsDict = new ToSlaveExistsDict(cacheDictCopy, cacheDictBySeqCopy, sentDictSeqList);
            return Repl.reply(slot, replPair, ReplType.s_exists_dict, toSlaveExistsDict);
        }
    }

    private Reply s_exists_dict(byte slot, byte[] contentBytes) {
        var oneSlot = localPersist.oneSlot(slot);

        // client received from server
        // empty content means no dict, next step
        if (contentBytes.length == 1) {
            return fetchExistsBigString(slot, oneSlot);
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var dictCount = buffer.getShort();
        var isSendAllOnce = buffer.get() == 1;

        if (dictCount == 0) {
            // next step, fetch big string
            return fetchExistsBigString(slot, oneSlot);
        }

        var dictMap = DictMap.getInstance();
        // decode
        try {
            for (int i = 0; i < dictCount; i++) {
                var encodeLength = buffer.getInt();
                var encodeBytes = new byte[encodeLength];
                buffer.get(encodeBytes);

                var is = new DataInputStream(new ByteArrayInputStream(encodeBytes));
                var dictWithKey = Dict.decode(is);

                var dict = dictWithKey.dict();
                dictMap.putDict(dictWithKey.key(), dict);

                log.warn("Repl handle s exists dict: dict with key={}", dictWithKey);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (isSendAllOnce) {
            // next step, fetch big string
            return fetchExistsBigString(slot, oneSlot);
        } else {
            var cacheDictBySeqCopyLocal = dictMap.getCacheDictBySeqCopy();
            if (cacheDictBySeqCopyLocal.isEmpty()) {
                return Repl.reply(slot, replPair, ReplType.exists_dict, new EmptyContent());
            }

            var rawBytes = new byte[4 * cacheDictBySeqCopyLocal.size()];
            var rawBuffer = ByteBuffer.wrap(rawBytes);
            for (var entry : cacheDictBySeqCopyLocal.entrySet()) {
                var seq = entry.getKey();
                rawBuffer.putInt(seq);
            }

            // continue fetch dict
            return Repl.reply(slot, replPair, ReplType.exists_dict, new RawBytesContent(rawBytes));
        }
    }

    private Reply exists_all_done(byte slot, byte[] contentBytes) {
        // server received from client
        log.warn("Slave exists/meta fetch all done, slot={}, slave uuid={}, {}:{}", slot,
                replPair.getSlaveUuid(), replPair.getHost(), replPair.getPort());
        return Repl.reply(slot, replPair, ReplType.s_exists_all_done, new EmptyContent());
    }

    private Reply s_exists_all_done(byte slot, byte[] contentBytes) {
        log.warn("Master reply exists/meta fetch all done, slot={}, slave uuid={}, {}:{}", slot,
                replPair.getSlaveUuid(), replPair.getHost(), replPair.getPort());

        var oneSlot = localPersist.oneSlot(slot);
        try {
            log.warn("Slot can read, slot={}", slot);
            oneSlot.setCanRead(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Repl.emptyReply();
    }
}
