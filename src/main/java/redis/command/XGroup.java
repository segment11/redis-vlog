
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import org.apache.commons.io.FileUtils;
import redis.BaseCommand;
import redis.ConfForSlot;
import redis.Dict;
import redis.DictMap;
import redis.persist.FdReadWrite;
import redis.persist.KeyLoader;
import redis.persist.OneSlot;
import redis.repl.Binlog;
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

import static redis.repl.ReplType.hi;
import static redis.repl.ReplType.pong;

public class XGroup extends BaseCommand {
    public XGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
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
            log.error("Repl handle error: unknown repl type: {}", replType);
            return null;
        }
        var contentBytes = data[3];

        var oneSlot = localPersist.oneSlot(slot);
        if (this.replPair == null) {
            if (replType.isSlaveSend) {
                this.replPair = oneSlot.getReplPairAsMaster(slaveUuid);
            } else {
                this.replPair = oneSlot.getReplPairAsSlave(slaveUuid);

                if (this.replPair == null) {
                    log.warn("Repl handle error: repl pair as slave not found, maybe closed already, slave uuid={}, repl type={}",
                            slaveUuid, replType);
                    return Repl.emptyReply();
                }
            }
        }

        if (!replType.newly && !replType.isSlaveSend) {
            log.warn("Repl slave fetch date from master, slave uuid={}, repl type={}, content length={}",
                    slaveUuid, replType, contentBytes.length);
        }

        return switch (replType) {
            case error -> {
                log.error("Repl handle receive error: {}", new String(contentBytes));
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
            case ok -> {
                log.info("Repl handle ok: slave uuid={}, {}", slaveUuid, replPair.getHostAndPort());
                yield Repl.emptyReply();
            }
            case bye -> {
                // server received bye from client
                var netListenAddresses = new String(contentBytes);
                log.warn("Repl handle bye: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

                if (replPair == null) {
                    yield Repl.emptyReply();
                }

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.reply(slot, replPair, ReplType.byeBye, new Ping(ConfForSlot.global.netListenAddresses));
            }
            case byeBye -> {
                // client received bye from server
                var netListenAddresses = new String(contentBytes);
                log.warn("Repl handle byeBye: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

                if (replPair == null) {
                    yield Repl.emptyReply();
                }

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.emptyReply();
            }
            case exists_chunk_segments -> exists_chunk_segments(slot, contentBytes);
            case exists_key_buckets -> exists_key_buckets(slot, contentBytes);
            case meta_key_bucket_split_number -> meta_key_bucket_split_number(slot, contentBytes);
            case stat_key_count_in_buckets -> stat_key_count_in_buckets(slot, contentBytes);
            case exists_big_string -> exists_big_string(slot, contentBytes);
            case incremental_big_string -> incremental_big_string(slot, contentBytes);
            case exists_dict -> exists_dict(slot, contentBytes);
            case exists_all_done -> exists_all_done(slot, contentBytes);
            case catch_up -> catch_up(slot, contentBytes);
            case s_exists_chunk_segments -> s_exists_chunk_segments(slot, contentBytes);
            case s_exists_key_buckets -> s_exists_key_buckets(slot, contentBytes);
            case s_meta_key_bucket_split_number -> s_meta_key_bucket_split_number(slot, contentBytes);
            case s_stat_key_count_in_buckets -> s_stat_key_count_in_buckets(slot, contentBytes);
            case s_exists_big_string -> s_exists_big_string(slot, contentBytes);
            case s_incremental_big_string -> s_incremental_big_string(slot, contentBytes);
            case s_exists_dict -> s_exists_dict(slot, contentBytes);
            case s_exists_all_done -> s_exists_all_done(slot, contentBytes);
            case s_catch_up -> s_catch_up(slot, contentBytes);
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

        // start binlog
        try {
            oneSlot.getDynConfig().setBinlogOn(true);
            log.warn("Repl master start binlog, master uuid={}", replPair.getMasterUuid());
        } catch (IOException e) {
            var errorMessage = "Repl handle error: start binlog error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }
        log.warn("Repl master handle hello: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

        var binlog = oneSlot.getBinlog();
        var currentFileIndexAndOffset = binlog.currentFileIndexAndOffset();
        var earliestFileIndexAndOffset = binlog.earliestFileIndexAndOffset();
        var content = new Hi(slaveUuid, oneSlot.getMasterUuid(), currentFileIndexAndOffset, earliestFileIndexAndOffset);
        return Repl.reply(slot, replPair, hi, content);
    }

    private Reply hi(byte slot, byte[] contentBytes) {
        // client received hi from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var slaveUuid = buffer.getLong();
        var masterUuid = buffer.getLong();
        var currentFileIndex = buffer.getInt();
        var currentOffset = buffer.getLong();
        var earliestFileIndex = buffer.getInt();
        var earliestOffset = buffer.getLong();

        // should not happen
        if (slaveUuid != replPair.getSlaveUuid()) {
            log.error("Repl slave handle error: slave uuid not match, client slave uuid={}, server hi slave uuid={}",
                    replPair.getSlaveUuid(), slaveUuid);
            return null;
        }

        replPair.setMasterUuid(masterUuid);
        log.warn("Repl slave handle hi: slave uuid={}, master uuid={}", slaveUuid, masterUuid);

        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

        var lastUpdatedMasterUuid = metaChunkSegmentIndex.getMasterUuid();
        var isExistsDataAllFetched = metaChunkSegmentIndex.isExistsDataAllFetched();
        if (lastUpdatedMasterUuid == masterUuid && isExistsDataAllFetched) {
            var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
            var catchUpFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
            long catchUpOffset = lastUpdatedFileIndexAndOffset.offset();

            if (catchUpFileIndex >= earliestFileIndex && catchUpOffset >= earliestOffset) {
                // need not fetch exists data from master
                // start fetch incremental data from master binglog
                log.warn("Repl slave start catch up from master binlog, slave uuid={}, master uuid={}, last updated file index={}, offset={}",
                        slaveUuid, masterUuid, catchUpFileIndex, catchUpOffset);

                var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
                var binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
                var nextCatchUpFileIndex = catchUpOffset == (binlogOneFileMaxLength - oneSegmentLength) ? catchUpFileIndex : catchUpFileIndex + 1;
                var nextCatchUpOffset = catchUpOffset == (binlogOneFileMaxLength - oneSegmentLength) ? 0 : catchUpOffset + oneSegmentLength;

                var content = new ToMasterCatchUpForBinlogOneSegment(masterUuid, nextCatchUpFileIndex, nextCatchUpOffset);
                return Repl.reply(slot, replPair, ReplType.catch_up, content);
            }
        }

        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false, currentFileIndex, currentOffset);
        log.warn("Repl set master binlog file index and offset for incremental catch up, slot: {}, master binlog file index: {}, master binlog offset: {}",
                slot, currentFileIndex, currentOffset);

        // begin to fetch exist data from master
        // first fetch dict
        var dictMap = DictMap.getInstance();
        var cacheDictBySeqCopy = dictMap.getCacheDictBySeqCopy();
        if (cacheDictBySeqCopy.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.exists_dict, EmptyContent.INSTANCE);
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

    Reply exists_chunk_segments(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();

        var oneSlot = localPersist.oneSlot(slot);
        var masterMetaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBath(beginSegmentIndex, segmentCount);
        if (ToMasterExistsChunkSegments.isSlaveSameForThisBatch(masterMetaBytes, contentBytes)) {
            var responseBytes = new byte[4 + 4];
            var responseBuffer = ByteBuffer.wrap(responseBytes);
            responseBuffer.putInt(beginSegmentIndex);
            responseBuffer.putInt(segmentCount);
            return Repl.reply(slot, replPair, ReplType.s_exists_chunk_segments, new RawBytesContent(responseBytes));
        }

        var chunkSegmentsBytes = oneSlot.preadForRepl(beginSegmentIndex);
        if (chunkSegmentsBytes == null) {
            chunkSegmentsBytes = new byte[0];
        }

        var responseBytes = new byte[4 + 4 + 4 + masterMetaBytes.length + 4 + chunkSegmentsBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putInt(beginSegmentIndex);
        responseBuffer.putInt(segmentCount);
        responseBuffer.putInt(masterMetaBytes.length);
        responseBuffer.put(masterMetaBytes);
        responseBuffer.putInt(chunkSegmentsBytes.length);
        if (chunkSegmentsBytes.length > 0) {
            responseBuffer.put(chunkSegmentsBytes);
        }

        return Repl.reply(slot, replPair, ReplType.s_exists_chunk_segments, new RawBytesContent(responseBytes));
    }

    Reply s_exists_chunk_segments(byte slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();

        var oneSlot = localPersist.oneSlot(slot);
        // content bytes length == 8 -> slave is same for this batch, skip
        if (contentBytes.length != 8) {
            var metaBytesLength = buffer.getInt();
            var metaBytes = new byte[metaBytesLength];
            buffer.get(metaBytes);
            oneSlot.getMetaChunkSegmentFlagSeq().overwriteOneBatch(metaBytes, beginSegmentIndex, segmentCount);

            var chunkSegmentsLength = buffer.getInt();
            if (chunkSegmentsLength == 0) {
                var bytes0 = new byte[segmentCount * ConfForSlot.global.confChunk.segmentLength];
                oneSlot.writeChunkSegmentsFromMasterExists(bytes0, beginSegmentIndex, segmentCount);
            } else {
                var chunkSegmentsBytes = new byte[chunkSegmentsLength];
                buffer.get(chunkSegmentsBytes);

                oneSlot.writeChunkSegmentsFromMasterExists(chunkSegmentsBytes, beginSegmentIndex, segmentCount);
            }
        }

        var maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
        boolean isLastBatch = maxSegmentNumber == beginSegmentIndex + segmentCount;
        if (isLastBatch) {
            return Repl.reply(slot, replPair, ReplType.exists_all_done, EmptyContent.INSTANCE);
        } else {
            var nextBatchBeginSegmentIndex = beginSegmentIndex + segmentCount;
            var nextBatchMetaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBath(nextBatchBeginSegmentIndex, segmentCount);
            var content = new ToMasterExistsChunkSegments(0, segmentCount, nextBatchMetaBytes);
            return Repl.reply(slot, replPair, ReplType.exists_chunk_segments, content);
        }
    }

    Reply exists_key_buckets(byte slot, byte[] contentBytes) {
        // server received from client
        var splitIndex = contentBytes[0];
        var beginBucketIndex = ByteBuffer.wrap(contentBytes, 2, 4).getInt();

        var oneSlot = localPersist.oneSlot(slot);
        byte splitNumber = oneSlot.getKeyLoader().maxSplitNumberForRepl();
        byte[] bytes = null;
        try {
            bytes = oneSlot.getKeyLoader().readKeyBucketBytesBatchToSlaveExists(splitIndex, beginBucketIndex);
        } catch (Exception e) {
            var errorMessage = "Repl key loader read key buckets bytes to slave error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }

        if (bytes == null) {
            bytes = new byte[0];
        }

        var responseBytes = new byte[1 + 1 + 4 + bytes.length];
        var buffer = ByteBuffer.wrap(responseBytes);
        buffer.put(splitIndex);
        buffer.put(splitNumber);
        buffer.putInt(beginBucketIndex);
        if (bytes.length != 0) {
            buffer.put(bytes);
        }

        return Repl.reply(slot, replPair, ReplType.s_exists_key_buckets, new RawBytesContent(responseBytes));
    }

    Reply s_exists_key_buckets(byte slot, byte[] contentBytes) {
        // client received from server
        // empty content means no exist key buckets, next step, fetch exists chunk segments
        var oneSlot = localPersist.oneSlot(slot);
        if (EmptyContent.isEmpty(contentBytes)) {
            // next step, fetch exists chunk segments
            var segmentCount = FdReadWrite.REPL_ONCE_INNER_COUNT;
            var metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBath(0, segmentCount);
            var content = new ToMasterExistsChunkSegments(0, segmentCount, metaBytes);
            return Repl.reply(slot, replPair, ReplType.exists_chunk_segments, content);
        }

        try {
            oneSlot.getKeyLoader().writeKeyBucketsBytesBatchFromMasterExists(contentBytes);
        } catch (Exception e) {
            var errorMessage = "Repl key loader write bytes from master error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }

        var splitIndex = contentBytes[0];
        var splitNumber = contentBytes[1];
        var prevBucketIndex = ByteBuffer.wrap(contentBytes, 2, 4).getInt();
        var bucketsPerSlot = ConfForSlot.global.confBucket.bucketsPerSlot;

        boolean isLastBatchInThisSplit = prevBucketIndex == bucketsPerSlot - KeyLoader.BATCH_ONCE_KEY_BUCKET_COUNT_READ_FOR_REPL;
        var isAllReceived = splitIndex == splitNumber - 1 && isLastBatchInThisSplit;
        if (isAllReceived) {
            // next step, fetch exists chunk segments
            var segmentCount = FdReadWrite.REPL_ONCE_INNER_COUNT;
            var metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBath(0, segmentCount);
            var content = new ToMasterExistsChunkSegments(0, segmentCount, metaBytes);
            return Repl.reply(slot, replPair, ReplType.exists_chunk_segments, content);
        } else {
            var nextSplitIndex = isLastBatchInThisSplit ? splitIndex + 1 : splitIndex;
            var nextBeginBucketIndex = isLastBatchInThisSplit ? 0 : prevBucketIndex + KeyLoader.BATCH_ONCE_KEY_BUCKET_COUNT_READ_FOR_REPL;
            var requestBytes = new byte[1 + 1 + 4];
            requestBytes[0] = (byte) nextSplitIndex;
            requestBytes[1] = 0;
            ByteBuffer.wrap(requestBytes, 2, 4).putInt(nextBeginBucketIndex);
            return Repl.reply(slot, replPair, ReplType.exists_key_buckets, new RawBytesContent(requestBytes));
        }
    }

    Reply stat_key_count_in_buckets(byte slot, byte[] contentBytes) {
        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var bytes = oneSlot.getKeyLoader().getStatKeyCountInBucketsBytesToSlaveExists();
        return Repl.reply(slot, replPair, ReplType.s_stat_key_count_in_buckets, new RawBytesContent(bytes));
    }

    Reply s_stat_key_count_in_buckets(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getKeyLoader().overwriteStatKeyCountInBucketsBytesFromMasterExists(contentBytes);
        // next step, fetch exists key buckets
        return Repl.reply(slot, replPair, ReplType.exists_key_buckets, EmptyContent.INSTANCE);
    }

    Reply meta_key_bucket_split_number(byte slot, byte[] contentBytes) {
        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var bytes = oneSlot.getKeyLoader().getMetaKeyBucketSplitNumberBytesToSlaveExists();
        return Repl.reply(slot, replPair, ReplType.s_meta_key_bucket_split_number, new RawBytesContent(bytes));
    }

    Reply s_meta_key_bucket_split_number(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getKeyLoader().overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(contentBytes);
        // next step, fetch exists key buckets
        return Repl.reply(slot, replPair, ReplType.stat_key_count_in_buckets, EmptyContent.INSTANCE);
    }

    Reply incremental_big_string(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();

        var oneSlot = localPersist.oneSlot(slot);

        var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid);
        if (bigStringBytes == null) {
            bigStringBytes = new byte[0];
        }

        var responseBytes = new byte[8 + bigStringBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putLong(uuid);
        if (bigStringBytes != null) {
            responseBuffer.put(bigStringBytes);
        }
        return Repl.reply(slot, replPair, ReplType.s_incremental_big_string, new RawBytesContent(responseBytes));
    }

    Reply s_incremental_big_string(byte slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();

        // master big string file already deleted, skip
        if (contentBytes.length != 8) {
            var bigStringBytes = new byte[contentBytes.length - 8];
            buffer.get(bigStringBytes);

            var oneSlot = localPersist.oneSlot(slot);
            oneSlot.getBigStringFiles().writeBigStringBytes(uuid, "ignore", bigStringBytes);
            log.info("Repl handle s incremental big string: write big string bytes, uuid={}, slot={}", uuid, slot);
        }

        replPair.doneFetchBigStringUuid(uuid);
        return Repl.emptyReply();
    }

    Reply exists_big_string(byte slot, byte[] contentBytes) {
        // server received from client
        // send back exists big string to client, with flag can do next step
        // client already persisted big string uuid, send to client exclude sent big string
        var sentUuidList = new ArrayList<Long>();
        if (contentBytes.length >= 8) {
            var sentUuidCount = contentBytes.length / 8;

            var buffer = ByteBuffer.wrap(contentBytes);
            for (int i = 0; i < sentUuidCount; i++) {
                sentUuidList.add(buffer.getLong());
            }
        }

        var oneSlot = localPersist.oneSlot(slot);
        var uuidListInMaster = oneSlot.getBigStringFiles().getBigStringFileUuidList();
        if (uuidListInMaster.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.s_exists_big_string, EmptyContent.INSTANCE);
        }

        var toSlaveExistsBigString = new ToSlaveExistsBigString(oneSlot.getBigStringDir(), uuidListInMaster, sentUuidList);
        return Repl.reply(slot, replPair, ReplType.s_exists_big_string, toSlaveExistsBigString);
    }

    // need delete local big string file if not exists in master, todo
    Reply s_exists_big_string(byte slot, byte[] contentBytes) {
        // client received from server
        // empty content means no big string, next step
        if (EmptyContent.isEmpty(contentBytes)) {
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, EmptyContent.INSTANCE);
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var bigStringCount = buffer.getShort();
        var isSendAllOnce = buffer.get() == 1;

        if (bigStringCount == 0) {
            // next step, fetch meta key bucket split number
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, EmptyContent.INSTANCE);
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
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, EmptyContent.INSTANCE);
        } else {
            return fetchExistsBigString(slot, oneSlot);
        }
    }

    Reply fetchExistsBigString(byte slot, OneSlot oneSlot) {
        var uuidListLocal = oneSlot.getBigStringFiles().getBigStringFileUuidList();
        if (uuidListLocal.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.exists_big_string, EmptyContent.INSTANCE);
        }

        var rawBytes = new byte[8 * uuidListLocal.size()];
        var rawBuffer = ByteBuffer.wrap(rawBytes);
        for (var uuid : uuidListLocal) {
            rawBuffer.putLong(uuid);
        }

        return Repl.reply(slot, replPair, ReplType.exists_big_string, new RawBytesContent(rawBytes));
    }

    Reply exists_dict(byte slot, byte[] contentBytes) {
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
            return Repl.reply(slot, replPair, ReplType.s_exists_dict, EmptyContent.INSTANCE);
        } else {
            var content = new ToSlaveExistsDict(cacheDictCopy, cacheDictBySeqCopy, sentDictSeqList);
            return Repl.reply(slot, replPair, ReplType.s_exists_dict, content);
        }
    }

    Reply s_exists_dict(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        // empty content means no dict, next step
        if (EmptyContent.isEmpty(contentBytes)) {
            return fetchExistsBigString(slot, oneSlot);
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var dictCount = buffer.getInt();
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
                dictMap.putDict(dictWithKey.keyPrefix(), dict);

                log.warn("Repl handle s exists dict: dict with key={}", dictWithKey);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // next step, fetch big string
        return fetchExistsBigString(slot, oneSlot);
    }

    Reply exists_all_done(byte slot, byte[] contentBytes) {
        // server received from client
        log.warn("Slave exists/meta fetch all done, slot={}, slave uuid={}, {}", slot,
                replPair.getSlaveUuid(), replPair.getHostAndPort());
        return Repl.reply(slot, replPair, ReplType.s_exists_all_done, EmptyContent.INSTANCE);
    }

    Reply s_exists_all_done(byte slot, byte[] contentBytes) {
        // client received from server
        log.warn("Master reply exists/meta fetch all done, slot={}, slave uuid={}, {}", slot,
                replPair.getSlaveUuid(), replPair.getHostAndPort());

        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();
        var fileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();

        // update exists data all fetched done
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                fileIndexAndOffset.fileIndex(), fileIndexAndOffset.offset());

        // begin incremental data catch up
        var binlogFileIndex = fileIndexAndOffset.fileIndex();
        var marginFileOffset = Binlog.marginFileOffset(fileIndexAndOffset.offset());
        var content = new ToMasterCatchUpForBinlogOneSegment(binlogMasterUuid, binlogFileIndex, marginFileOffset);
        return Repl.reply(slot, replPair, ReplType.catch_up, content);
    }

    Reply catch_up(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var binlogMasterUuid = buffer.getLong();
        var binlogFileIndex = buffer.getInt();
        var binlogOffset = buffer.getLong();

        var oneSlot = localPersist.oneSlot(slot);
        if (oneSlot.getMasterUuid() != binlogMasterUuid) {
            var errorMessage = "Repl handle error: master uuid not match";
            log.error(errorMessage);
            return Repl.error(slot, replPair, errorMessage);
        }

        var binlog = oneSlot.getBinlog();
        byte[] oneSegmentBytes;
        try {
            oneSegmentBytes = binlog.readPrevRafOneSegment(binlogFileIndex, binlogOffset);
        } catch (IOException e) {
            var errorMessage = "Repl handle error: read binlog file error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }

        if (oneSegmentBytes == null) {
            log.warn("Repl handle catch up: get binlog null, slot={}, slave uuid={}, {}, binlog file index={}, offset={}", slot,
                    replPair.getSlaveUuid(), replPair.getHostAndPort(), binlogFileIndex, binlogOffset);
            return Repl.error(slot, replPair, "Get binlog null, slot=" + slot +
                    ", binlog file index=" + binlogFileIndex + ", offset=" + binlogOffset);
        }

        var currentFileIndexAndOffset = binlog.currentFileIndexAndOffset();

        var responseBytes = new byte[4 + 8 + 4 + 8 + 4 + oneSegmentBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putInt(binlogFileIndex);
        responseBuffer.putLong(binlogOffset);
        responseBuffer.putInt(currentFileIndexAndOffset.fileIndex());
        responseBuffer.putLong(currentFileIndexAndOffset.offset());
        responseBuffer.putInt(oneSegmentBytes.length);
        responseBuffer.put(oneSegmentBytes);

        return Repl.reply(slot, replPair, ReplType.s_catch_up, new RawBytesContent(responseBytes));
    }

    Reply s_catch_up(byte slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var catchUpFileIndex = buffer.getInt();
        var catchUpOffset = buffer.getLong();

        var currentFileIndex = buffer.getInt();
        var currentOffset = buffer.getLong();

        var oneSegmentLength = buffer.getInt();
        var oneSegmentBytes = new byte[oneSegmentLength];
        buffer.get(oneSegmentBytes);

        int currentMarginFileOffset = Binlog.marginFileOffset(currentOffset);

        // update last catch up file index and offset
        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

        var skipBytesN = 0;
        var ff = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var isOffsetInLatestSegment = ff.fileIndex() == currentFileIndex && ff.offset() > currentMarginFileOffset;
        if (isOffsetInLatestSegment) {
            skipBytesN = (int) (ff.offset() - currentMarginFileOffset);
        }

        try {
            var n = Binlog.decodeAndApply(slot, oneSegmentBytes, skipBytesN, replPair);
            if (catchUpOffset == 0) {
                log.info("Repl binlog catch up success, slot={}, slave uuid={}, {}, catch up file index={}, catch up offset={}, apply n={}",
                        slot, replPair.getSlaveUuid(), replPair.getHostAndPort(), catchUpFileIndex, catchUpOffset, n);
            }
        } catch (Exception e) {
            var errorMessage = "Repl handle error: decode and apply binlog error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }

        var isCatchUpToCurrentFile = catchUpFileIndex == currentFileIndex;
        if (isCatchUpToCurrentFile) {
            var diffOffset = currentOffset - catchUpOffset;
            if (diffOffset < ConfForSlot.global.confRepl.catchUpOffsetMinDiff) {
                try {
                    log.warn("Slot can read, slot={}", slot);
                    oneSlot.setCanRead(true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();

        var isCatchUpOffsetInLatestSegment = isCatchUpToCurrentFile && catchUpOffset >= currentMarginFileOffset;
        if (isCatchUpOffsetInLatestSegment) {
            metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                    currentFileIndex, currentOffset);

            // still catch up current segment, delay
            var content = new ToMasterCatchUpForBinlogOneSegment(binlogMasterUuid, currentFileIndex, catchUpOffset);
            oneSlot.delayRun(ConfForSlot.global.confRepl.catchUpLatestSegmentDelayMillis, () -> {
                replPair.write(ReplType.catch_up, content);
            });
            return Repl.emptyReply();
        }

        var binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
        var nextCatchUpFileIndex = catchUpOffset == (binlogOneFileMaxLength - oneSegmentLength) ? catchUpFileIndex : catchUpFileIndex + 1;
        var nextCatchUpOffset = catchUpOffset == (binlogOneFileMaxLength - oneSegmentLength) ? 0 : catchUpOffset + oneSegmentLength;

        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                nextCatchUpFileIndex, nextCatchUpOffset);
        if (nextCatchUpOffset == 0) {
            log.info("Repl handle catch up: catch up to next file, slot={}, slave uuid={}, {}, binlog file index={}, offset={}",
                    slot, replPair.getSlaveUuid(), replPair.getHostAndPort(), nextCatchUpFileIndex, nextCatchUpOffset);
        }
        var content = new ToMasterCatchUpForBinlogOneSegment(binlogMasterUuid, nextCatchUpFileIndex, nextCatchUpOffset);
        return Repl.reply(slot, replPair, ReplType.catch_up, content);
    }
}
