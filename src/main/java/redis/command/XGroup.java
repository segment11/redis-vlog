
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import org.apache.commons.io.FileUtils;
import redis.*;
import redis.persist.FdReadWrite;
import redis.persist.KeyLoader;
import redis.persist.OneSlot;
import redis.persist.Wal;
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

import static redis.repl.ReplType.*;

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

    public static final String CONF_FOR_SLOT_KEY = "x_conf_for_slot";

    public void setReplPair(ReplPair replPair) {
        this.replPair = replPair;
    }

    private ReplPair replPair;

    public Repl.ReplReply handleRepl() {
        var slaveUuid = ByteBuffer.wrap(data[0]).getLong();

        var slot = data[1][0];
        var replType = ReplType.fromCode(data[2][0]);
        if (replType == null) {
            log.error("Repl handle error: unknown repl type code: {}", data[2][0]);
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
                    if (replType != error) {
                        log.warn("Repl handle error: repl pair as slave not found, maybe closed already, slave uuid={}, repl type={}",
                                slaveUuid, replType);
                        return Repl.emptyReply();
                    }
                }
            }
        }

        if (replPair != null) {
            replPair.increaseStatsCountForReplType(replType);
        }

        return switch (replType) {
            case error -> {
                log.error("Repl handle receive error: {}", new String(contentBytes));
                yield Repl.emptyReply();
            }
            case ping -> {
                // server received ping from client
                var netListenAddresses = new String(contentBytes);
                var array = netListenAddresses.split(":");
                var host = array[0];
                var port = Integer.parseInt(array[1]);

                if (replPair == null) {
                    replPair = oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, host, port);
                    replPair.increaseStatsCountForReplType(ping);
                }

                replPair.setLastPingGetTimestamp(System.currentTimeMillis());
                yield Repl.reply(slot, replPair, pong, new Pong(ConfForGlobal.netListenAddresses));
            }
            case pong -> {
                // client received pong from server
                replPair.setLastPongGetTimestamp(System.currentTimeMillis());

                var millis = replPair.getLastGetCatchUpResponseMillis();
                if (millis != 0 && System.currentTimeMillis() - millis > 1000 * 10) {
                    // trigger catch up
                    s_catch_up(slot, new byte[1]);
                }

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
                log.warn("Repl master handle bye: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

                if (replPair == null) {
                    yield Repl.emptyReply();
                }

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.reply(slot, replPair, ReplType.byeBye, new Pong(ConfForGlobal.netListenAddresses));
            }
            case byeBye -> {
                // client received bye from server
                var netListenAddresses = new String(contentBytes);
                log.warn("Repl slave handle bye bye: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

                oneSlot.addDelayNeedCloseReplPair(replPair);
                yield Repl.emptyReply();
            }
            case exists_wal -> exists_wal(slot, contentBytes);
            case exists_chunk_segments -> exists_chunk_segments(slot, contentBytes);
            case exists_key_buckets -> exists_key_buckets(slot, contentBytes);
            case meta_key_bucket_split_number -> meta_key_bucket_split_number(slot, contentBytes);
            case stat_key_count_in_buckets -> stat_key_count_in_buckets(slot, contentBytes);
            case exists_big_string -> exists_big_string(slot, contentBytes);
            case incremental_big_string -> incremental_big_string(slot, contentBytes);
            case exists_dict -> exists_dict(slot, contentBytes);
            case exists_all_done -> exists_all_done(slot, contentBytes);
            case catch_up -> catch_up(slot, contentBytes);
            case s_exists_wal -> s_exists_wal(slot, contentBytes);
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

    Repl.ReplReply hello(byte slot, byte[] contentBytes) {
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
            replPair.increaseStatsCountForReplType(hello);
        }

        // start binlog
        try {
            oneSlot.getDynConfig().setBinlogOn(true);
            log.warn("Repl master start binlog, master uuid={}", replPair.getMasterUuid());
        } catch (IOException e) {
            var errorMessage = "Repl master handle error: start binlog error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }
        log.warn("Repl master handle hello: slave uuid={}, net listen addresses={}", slaveUuid, netListenAddresses);

        var binlog = oneSlot.getBinlog();
        var currentFileIndexAndOffset = binlog.currentFileIndexAndOffset();
        var earliestFileIndexAndOffset = binlog.earliestFileIndexAndOffset();
        var content = new Hi(slaveUuid, oneSlot.getMasterUuid(), currentFileIndexAndOffset, earliestFileIndexAndOffset,
                oneSlot.getChunk().currentSegmentIndex());
        return Repl.reply(slot, replPair, hi, content);
    }

    private RawBytesContent toMasterCatchUp(long binlogMasterUuid, int lastUpdatedFileIndex, long marginLastUpdatedOffset, long lastUpdatedOffset) {
        var requestBytes = new byte[8 + 4 + 8 + 8];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.putLong(binlogMasterUuid);
        requestBuffer.putInt(lastUpdatedFileIndex);
        requestBuffer.putLong(marginLastUpdatedOffset);
        requestBuffer.putLong(lastUpdatedOffset);
        return new RawBytesContent(requestBytes);
    }

    Repl.ReplReply hi(byte slot, byte[] contentBytes) {
        // client received hi from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var slaveUuid = buffer.getLong();
        var masterUuid = buffer.getLong();
        // master binlog current (latest) file index and offset
        var currentFileIndex = buffer.getInt();
        var currentOffset = buffer.getLong();
        // master binlog earliest (not deleted yet) file index and offset
        var earliestFileIndex = buffer.getInt();
        var earliestOffset = buffer.getLong();
        var currentSegmentIndex = buffer.getInt();

        // should not happen
        if (slaveUuid != replPair.getSlaveUuid()) {
            log.error("Repl slave handle error: slave uuid not match, client slave uuid={}, server hi slave uuid={}",
                    replPair.getSlaveUuid(), slaveUuid);
            return null;
        }

        replPair.setMasterUuid(masterUuid);
        log.warn("Repl slave handle hi: slave uuid={}, master uuid={}", slaveUuid, masterUuid);

        var oneSlot = localPersist.oneSlot(slot);
        // after exist all done, when catch up, XOneWalGroupSeq will update chunk segment index
        oneSlot.setMetaChunkSegmentIndex(currentSegmentIndex);
        log.warn("Repl slave set meta chunk segment index, slot: {}, segment index: {}", slot, currentSegmentIndex);

        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

        var lastUpdatedMasterUuid = metaChunkSegmentIndex.getMasterUuid();
        var isExistsDataAllFetched = metaChunkSegmentIndex.isExistsDataAllFetched();
        if (lastUpdatedMasterUuid == masterUuid && isExistsDataAllFetched) {
            // last updated means next batch, but not fetch yet, refer end of method s_cache_up
            var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
            var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
            var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

            if (lastUpdatedFileIndex >= earliestFileIndex && lastUpdatedOffset >= earliestOffset) {
                // need not fetch exists data from master
                // start fetch incremental data from master binlog
                log.warn("Repl slave skip fetch exists data and start catch up from master binlog, " +
                                "slave uuid={}, master uuid={}, last updated file index={}, offset={}",
                        slaveUuid, masterUuid, lastUpdatedFileIndex, lastUpdatedOffset);

                // not catch up any binlog segment yet, start from the beginning
                if (lastUpdatedFileIndex == 0 && lastUpdatedOffset == 0) {
                    var content = toMasterCatchUp(masterUuid, 0, 0L, 0L);
                    return Repl.reply(slot, replPair, ReplType.catch_up, content);
                }

                // last fetched binlog segment is not a complete segment, need to re-fetch this segment
                var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
                if (marginLastUpdatedOffset != lastUpdatedOffset) {
                    var content = toMasterCatchUp(masterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
                    return Repl.reply(slot, replPair, ReplType.catch_up, content);
                }

                var content = toMasterCatchUp(masterUuid, lastUpdatedFileIndex, lastUpdatedOffset, lastUpdatedOffset);
                return Repl.reply(slot, replPair, ReplType.catch_up, content);
            }
        }

        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false,
                currentFileIndex, currentOffset);
        log.warn("Repl slave set master binlog current/latest file index and offset for incremental catch up, slot: {}, master binlog file index: {}, offset: {}",
                slot, currentFileIndex, currentOffset);
        log.warn("Repl slave begin fetch all exists data from master, slot: {}", slot);

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

    Repl.ReplReply exists_wal(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var groupIndex = buffer.getInt();

        var walGroupNumber = Wal.calcWalGroupNumber();
        if (groupIndex < 0 || groupIndex >= walGroupNumber) {
            log.error("Repl master update wal exists bytes error: group index out of range, slot: {}, group index: {}",
                    slot, groupIndex);
            return Repl.error(slot, replPair, "Repl master update wal exists bytes error: group index out of range");
        }

        if (groupIndex % 100 == 0) {
            log.warn("Repl master fetch exists wal, slot: {}, group index: {}", slot, groupIndex);
        }

        var oneSlot = localPersist.oneSlot(slot);
        var targetWal = oneSlot.getWalByGroupIndex(groupIndex);
        try {
            var toSlaveBytes = targetWal.toSlaveExistsOneWalGroupBytes();
            return Repl.reply(slot, replPair, ReplType.s_exists_wal, new RawBytesContent(toSlaveBytes));
        } catch (IOException e) {
            log.error("Repl master get wal exists bytes error, slot: " + slot + ", group index: " + groupIndex, e);
            return Repl.error(slot, replPair, "Repl master get wal exists bytes error: " + e.getMessage());
        }
    }

    Repl.ReplReply s_exists_wal(byte slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var groupIndex = buffer.getInt();

        var oneSlot = localPersist.oneSlot(slot);
        var targetWal = oneSlot.getWalByGroupIndex(groupIndex);
        try {
            targetWal.fromMasterExistsOneWalGroupBytes(contentBytes);
        } catch (IOException e) {
            log.error("Repl slave update wal exists bytes error, slot: " + slot + ", group index: " + groupIndex, e);
            return Repl.error(slot, replPair, "Repl slave update wal exists bytes error: " + e.getMessage());
        }

        var walGroupNumber = Wal.calcWalGroupNumber();
        if (groupIndex == walGroupNumber - 1) {
            return Repl.reply(slot, replPair, ReplType.exists_all_done, EmptyContent.INSTANCE);
        } else {
            var nextGroupIndex = groupIndex + 1;
            if (nextGroupIndex % 100 == 0) {
                // delay
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.exists_wal, requestExistsWal(nextGroupIndex));
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, ReplType.exists_wal, requestExistsWal(nextGroupIndex));
            }
        }
    }

    private RawBytesContent requestExistsWal(int groupIndex) {
        var requestBytes = new byte[4];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.putInt(groupIndex);
        return new RawBytesContent(requestBytes);
    }

    Repl.ReplReply exists_chunk_segments(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();

        if (beginSegmentIndex % (segmentCount * 10) == 0) {
            log.warn("Repl master fetch exists chunk segments, slot: {}, begin segment index: {}, segment count: {}",
                    slot, beginSegmentIndex, segmentCount);
        }

        var oneSlot = localPersist.oneSlot(slot);
        var masterMetaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(beginSegmentIndex, segmentCount);
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

    Repl.ReplReply s_exists_chunk_segments(byte slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var beginSegmentIndex = buffer.getInt();
        var segmentCount = buffer.getInt();
        // segmentCount == FdReadWrite.REPL_ONCE_INNER_COUNT

        if (beginSegmentIndex % (segmentCount * 10) == 0) {
            log.warn("Repl slave ready to fetch exists chunk segments, slot: {}, begin segment index: {}, segment count: {}",
                    slot, beginSegmentIndex, segmentCount);
        }

        var oneSlot = localPersist.oneSlot(slot);
        // content bytes length == 8 -> slave is same for this batch, skip
        if (contentBytes.length != 8) {
            var metaBytesLength = buffer.getInt();
            var metaBytes = new byte[metaBytesLength];
            buffer.get(metaBytes);
            oneSlot.getMetaChunkSegmentFlagSeq().overwriteOneBatch(metaBytes, beginSegmentIndex, segmentCount);

            var chunkSegmentsLength = buffer.getInt();
            if (chunkSegmentsLength == 0) {
                oneSlot.writeChunkSegmentsFromMasterExists(ConfForSlot.global.confChunk.REPL_EMPTY_BYTES_FOR_ONCE_WRITE,
                        beginSegmentIndex, segmentCount);
            } else {
                var chunkSegmentsBytes = new byte[chunkSegmentsLength];
                buffer.get(chunkSegmentsBytes);

                var realSegmentCount = chunkSegmentsLength / ConfForSlot.global.confChunk.segmentLength;
                oneSlot.writeChunkSegmentsFromMasterExists(chunkSegmentsBytes,
                        beginSegmentIndex, realSegmentCount);
            }
        }

        var maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber();
        boolean isLastBatch = maxSegmentNumber == beginSegmentIndex + segmentCount;
        if (isLastBatch) {
            // next step, fetch exists wal
            return Repl.reply(slot, replPair, exists_wal, requestExistsWal(0));
        } else {
            var nextBatchBeginSegmentIndex = beginSegmentIndex + segmentCount;
            var nextBatchMetaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(nextBatchBeginSegmentIndex, segmentCount);
            var content = new ToMasterExistsChunkSegments(nextBatchBeginSegmentIndex, segmentCount, nextBatchMetaBytes);

            if (nextBatchBeginSegmentIndex % (segmentCount * 10) == 0) {
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.exists_chunk_segments, content);
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, ReplType.exists_chunk_segments, content);
            }
        }
    }

    Repl.ReplReply exists_key_buckets(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var splitIndex = buffer.get();
        var beginBucketIndex = buffer.getInt();
        var oneWalGroupSeq = buffer.getLong();

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (beginBucketIndex % (oneChargeBucketNumber * 100) == 0) {
            log.warn("Repl master fetch exists key buckets, slot: {}, split index: {}, begin bucket index: {}", slot, splitIndex, beginBucketIndex);
        }

        var oneSlot = localPersist.oneSlot(slot);
        var maxSplitNumber = oneSlot.getKeyLoader().maxSplitNumberForRepl();

        byte[] bytes = null;
        var masterOneWalGroupSeq = oneSlot.getKeyLoader().getMetaOneWalGroupSeq(splitIndex, beginBucketIndex);
        var isSkip = masterOneWalGroupSeq == oneWalGroupSeq;
        if (!isSkip) {
            bytes = oneSlot.getKeyLoader().readBatchInOneWalGroup(splitIndex, beginBucketIndex);
        }

        if (bytes == null) {
            bytes = new byte[0];
        }

        var responseBytes = new byte[1 + 1 + 4 + 1 + 8 + bytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.put(splitIndex);
        responseBuffer.put(maxSplitNumber);
        responseBuffer.putInt(beginBucketIndex);
        responseBuffer.put(isSkip ? (byte) 1 : (byte) 0);
        responseBuffer.putLong(masterOneWalGroupSeq);
        if (bytes.length != 0) {
            responseBuffer.put(bytes);
        }

        return Repl.reply(slot, replPair, ReplType.s_exists_key_buckets, new RawBytesContent(responseBytes));
    }

    Repl.ReplReply s_exists_key_buckets(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);

        var buffer = ByteBuffer.wrap(contentBytes);
        var splitIndex = buffer.get();
        var maxSplitNumber = buffer.get();
        var beginBucketIndex = buffer.getInt();
        var isSkip = buffer.get() == 1;
        var masterOneWalGroupSeq = buffer.getLong();
        var leftLength = buffer.remaining();

        var oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber;
        if (beginBucketIndex % (oneChargeBucketNumber * 100) == 0) {
            log.warn("Repl slave ready to fetch exists key buckets, slot: {}, split index: {}, begin bucket index: {}", slot, splitIndex, beginBucketIndex);
        }

        if (!isSkip) {
            var sharedBytesList = new byte[splitIndex + 1][];
            if (leftLength == 0) {
                // clear local key buckets
                sharedBytesList[splitIndex] = new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE * oneChargeBucketNumber];
            } else {
                // overwrite key buckets
                var sharedBytes = new byte[leftLength];
                buffer.get(sharedBytes);
                sharedBytesList[splitIndex] = sharedBytes;
            }
            oneSlot.getKeyLoader().writeSharedBytesList(sharedBytesList, beginBucketIndex);
            oneSlot.getKeyLoader().setMetaOneWalGroupSeq(splitIndex, beginBucketIndex, masterOneWalGroupSeq);
        }

        boolean isLastBatchInThisSplit = beginBucketIndex == ConfForSlot.global.confBucket.bucketsPerSlot - oneChargeBucketNumber;
        var isAllReceived = splitIndex == maxSplitNumber - 1 && isLastBatchInThisSplit;
        if (isAllReceived) {
            log.warn("Repl slave fetch all key buckets done, slot: {}", slot);

            // next step, fetch exists chunk segments
            var segmentCount = FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD;
            var metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, segmentCount);
            var content = new ToMasterExistsChunkSegments(0, segmentCount, metaBytes);
            return Repl.reply(slot, replPair, ReplType.exists_chunk_segments, content);
        } else {
            var nextSplitIndex = isLastBatchInThisSplit ? splitIndex + 1 : splitIndex;
            var nextBeginBucketIndex = isLastBatchInThisSplit ? 0 : beginBucketIndex + oneChargeBucketNumber;

            var requestBytes = new byte[1 + 4 + 8];
            var requestBuffer = ByteBuffer.wrap(requestBytes);
            requestBuffer.put((byte) nextSplitIndex);
            requestBuffer.putInt(nextBeginBucketIndex);
            var slaveOneWalGroupSeq = oneSlot.getKeyLoader().getMetaOneWalGroupSeq((byte) nextSplitIndex, nextBeginBucketIndex);
            requestBuffer.putLong(slaveOneWalGroupSeq);
            var content = new RawBytesContent(requestBytes);

            if (nextBeginBucketIndex % (oneChargeBucketNumber * 100) == 0) {
                oneSlot.delayRun(1000, () -> {
                    replPair.write(ReplType.exists_key_buckets, content);
                });
                return Repl.emptyReply();
            } else {
                return Repl.reply(slot, replPair, ReplType.exists_key_buckets, content);
            }
        }
    }

    Repl.ReplReply stat_key_count_in_buckets(byte slot, byte[] contentBytes) {
        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var bytes = oneSlot.getKeyLoader().getStatKeyCountInBucketsBytesToSlaveExists();
        log.warn("Repl master fetch stat key count in key buckets, slot: {}", slot);
        return Repl.reply(slot, replPair, ReplType.s_stat_key_count_in_buckets, new RawBytesContent(bytes));
    }

    Repl.ReplReply s_stat_key_count_in_buckets(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getKeyLoader().overwriteStatKeyCountInBucketsBytesFromMasterExists(contentBytes);
        log.warn("Repl slave fetch stat key count in key buckets done, slot: {}", slot);

        // next step, fetch exists key buckets
        var requestBytes = new byte[1 + 4 + 8];
        var requestBuffer = ByteBuffer.wrap(requestBytes);
        requestBuffer.put((byte) 0);
        requestBuffer.putInt(0);
        var oneWalGroupSeq = oneSlot.getKeyLoader().getMetaOneWalGroupSeq((byte) 0, 0);
        requestBuffer.putLong(oneWalGroupSeq);
        return Repl.reply(slot, replPair, ReplType.exists_key_buckets, new RawBytesContent(requestBytes));
    }

    Repl.ReplReply meta_key_bucket_split_number(byte slot, byte[] contentBytes) {
        // server received from client
        // ignore content bytes, send all
        var oneSlot = localPersist.oneSlot(slot);
        var bytes = oneSlot.getKeyLoader().getMetaKeyBucketSplitNumberBytesToSlaveExists();
        log.warn("Repl master fetch meta key bucket split number, slot: {}", slot);
        return Repl.reply(slot, replPair, ReplType.s_meta_key_bucket_split_number, new RawBytesContent(bytes));
    }

    Repl.ReplReply s_meta_key_bucket_split_number(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.getKeyLoader().overwriteMetaKeyBucketSplitNumberBytesFromMasterExists(contentBytes);
        log.warn("Repl slave fetch meta key bucket split number done, slot: {}", slot);

        // next step, fetch exists key buckets
        return Repl.reply(slot, replPair, ReplType.stat_key_count_in_buckets, EmptyContent.INSTANCE);
    }

    Repl.ReplReply incremental_big_string(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();
        log.warn("Repl master fetch incremental big string, uuid={}, slot={}", uuid, slot);

        var oneSlot = localPersist.oneSlot(slot);

        var bigStringBytes = oneSlot.getBigStringFiles().getBigStringBytes(uuid);
        if (bigStringBytes == null) {
            bigStringBytes = new byte[0];
        }

        var responseBytes = new byte[8 + bigStringBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putLong(uuid);
        if (bigStringBytes.length > 0) {
            responseBuffer.put(bigStringBytes);
        }
        return Repl.reply(slot, replPair, ReplType.s_incremental_big_string, new RawBytesContent(responseBytes));
    }

    Repl.ReplReply s_incremental_big_string(byte slot, byte[] contentBytes) {
        // client received from server
        var buffer = ByteBuffer.wrap(contentBytes);
        var uuid = buffer.getLong();

        // master big string file already deleted, skip
        if (contentBytes.length != 8) {
            var bigStringBytes = new byte[contentBytes.length - 8];
            buffer.get(bigStringBytes);

            var oneSlot = localPersist.oneSlot(slot);
            oneSlot.getBigStringFiles().writeBigStringBytes(uuid, "ignore", bigStringBytes);
            log.warn("Repl slave fetch incremental big string done, uuid={}, slot={}", uuid, slot);
        }

        replPair.doneFetchBigStringUuid(uuid);
        return Repl.emptyReply();
    }

    Repl.ReplReply exists_big_string(byte slot, byte[] contentBytes) {
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
        log.warn("Repl master fetch exists big string, slave sent uuid list: {}, slot: {}", sentUuidList, slot);

        var oneSlot = localPersist.oneSlot(slot);
        var uuidListInMaster = oneSlot.getBigStringFiles().getBigStringFileUuidList();
        if (uuidListInMaster.isEmpty()) {
            return Repl.reply(slot, replPair, ReplType.s_exists_big_string, EmptyContent.INSTANCE);
        }

        var toSlaveExistsBigString = new ToSlaveExistsBigString(oneSlot.getBigStringDir(), uuidListInMaster, sentUuidList);
        return Repl.reply(slot, replPair, ReplType.s_exists_big_string, toSlaveExistsBigString);
    }

    // need delete local big string file if not exists in master, todo
    Repl.ReplReply s_exists_big_string(byte slot, byte[] contentBytes) {
        // client received from server
        // empty content means no big string, next step
        if (EmptyContent.isEmpty(contentBytes)) {
            log.warn("Repl slave fetch all big string done, slot: {}", slot);
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, EmptyContent.INSTANCE);
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var bigStringCount = buffer.getShort();
        var isSendAllOnce = buffer.get() == 1;

        if (bigStringCount == 0) {
            log.warn("Repl slave fetch all big string done, slot: {}", slot);
            // next step, fetch meta key bucket split number
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, EmptyContent.INSTANCE);
        }
        log.warn("Repl slave fetch exists big string, master sent big string count: {}, slot: {}", bigStringCount, slot);

        var oneSlot = localPersist.oneSlot(slot);
        var bigStringDir = oneSlot.getBigStringDir();
        try {
            for (int i = 0; i < bigStringCount; i++) {
                var uuid = buffer.getLong();
                var bigStringBytesLength = buffer.getInt();
                var bigStringBytes = new byte[bigStringBytesLength];
                buffer.get(bigStringBytes);

                var uuidAsFileName = String.valueOf(uuid);
                var file = new File(bigStringDir, uuidAsFileName);
                FileUtils.writeByteArrayToFile(file, bigStringBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (isSendAllOnce) {
            log.warn("Repl slave fetch all big string done, slot: {}", slot);
            // next step, fetch meta key bucket split number
            return Repl.reply(slot, replPair, ReplType.meta_key_bucket_split_number, EmptyContent.INSTANCE);
        } else {
            return fetchExistsBigString(slot, oneSlot);
        }
    }

    Repl.ReplReply fetchExistsBigString(byte slot, OneSlot oneSlot) {
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

    Repl.ReplReply exists_dict(byte slot, byte[] contentBytes) {
        // client already persisted dict seq, send to client exclude sent dict
        ArrayList<Integer> sentDictSeqList = new ArrayList<>();
        if (contentBytes.length >= 4) {
            var sentDictSeqCount = contentBytes.length / 4;

            var buffer = ByteBuffer.wrap(contentBytes);
            for (int i = 0; i < sentDictSeqCount; i++) {
                sentDictSeqList.add(buffer.getInt());
            }
        }
        log.warn("Repl master fetch exists dict, slave sent dict seq list: {}, slot: {}", sentDictSeqList, slot);

        var dictMap = DictMap.getInstance();
        var cacheDictCopy = dictMap.getCacheDictCopy();
        var cacheDictBySeqCopy = dictMap.getCacheDictBySeqCopy();
        // master always send global zstd dict to slave
        cacheDictCopy.put(Dict.GLOBAL_ZSTD_DICT_KEY, Dict.GLOBAL_ZSTD_DICT);
        cacheDictBySeqCopy.put(Dict.GLOBAL_ZSTD_DICT_SEQ, Dict.GLOBAL_ZSTD_DICT);

        var content = new ToSlaveExistsDict(cacheDictCopy, cacheDictBySeqCopy, sentDictSeqList);
        return Repl.reply(slot, replPair, ReplType.s_exists_dict, content);
    }

    Repl.ReplReply s_exists_dict(byte slot, byte[] contentBytes) {
        // client received from server
        var oneSlot = localPersist.oneSlot(slot);
        var buffer = ByteBuffer.wrap(contentBytes);
        var dictCount = buffer.getInt();
        log.warn("Repl slave fetch exists dict, master sent dict count: {}, slot: {}", dictCount, slot);

        var dictMap = DictMap.getInstance();
        // decode
        try {
            for (int i = 0; i < dictCount; i++) {
                var encodeLength = buffer.getInt();
                var encodeBytes = new byte[encodeLength];
                buffer.get(encodeBytes);

                var is = new DataInputStream(new ByteArrayInputStream(encodeBytes));
                var dictWithKeyPrefix = Dict.decode(is);

                var dict = dictWithKeyPrefix.dict();
                var keyPrefix = dictWithKeyPrefix.keyPrefix();
                if (keyPrefix.equals(Dict.GLOBAL_ZSTD_DICT_KEY)) {
                    dictMap.updateGlobalDictBytes(dict.getDictBytes());
                } else {
                    dictMap.putDict(keyPrefix, dict);
                }
                log.warn("Repl slave save master exists dict: dict with key={}", dictWithKeyPrefix);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // next step, fetch big string
        log.warn("Repl slave fetch all dict done, slot: {}", slot);
        return fetchExistsBigString(slot, oneSlot);
    }

    Repl.ReplReply exists_all_done(byte slot, byte[] contentBytes) {
        // server received from client
        log.warn("Repl slave exists/meta fetch all done, slot={}, slave uuid={}, {}", slot,
                replPair.getSlaveUuid(), replPair.getHostAndPort());
        return Repl.reply(slot, replPair, ReplType.s_exists_all_done, EmptyContent.INSTANCE);
    }

    Repl.ReplReply s_exists_all_done(byte slot, byte[] contentBytes) {
        // client received from server
        log.warn("Repl master reply exists/meta fetch all done, slot={}, slave uuid={}, {}", slot,
                replPair.getSlaveUuid(), replPair.getHostAndPort());

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.setChunkSegmentIndexFromMeta();

        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();

        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();
        var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
        var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

        // update exists data all fetched done
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                lastUpdatedFileIndex, lastUpdatedOffset);

        // begin incremental data catch up
        var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
        var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
        return Repl.reply(slot, replPair, ReplType.catch_up, content);
    }

    Repl.ReplReply catch_up(byte slot, byte[] contentBytes) {
        // server received from client
        var buffer = ByteBuffer.wrap(contentBytes);
        var binlogMasterUuid = buffer.getLong();
        var needFetchFileIndex = buffer.getInt();
        var needFetchOffset = buffer.getLong();
        var lastUpdatedOffset = buffer.getLong();

        if (needFetchOffset == 0) {
            log.warn("Repl master handle catch up from new binlog file, slot={}, slave uuid={}, {}, need fetch file index={}, offset={}",
                    slot, replPair.getSlaveUuid(), replPair.getHostAndPort(), needFetchFileIndex, needFetchOffset);
        }

        var binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        if (needFetchOffset % binlogOneSegmentLength != 0) {
            throw new IllegalArgumentException("Repl master handle error: catch up offset: " + needFetchOffset +
                    " is not a multiple of binlog one segment length: " + binlogOneSegmentLength);
        }

        var oneSlot = localPersist.oneSlot(slot);
        if (oneSlot.getMasterUuid() != binlogMasterUuid) {
            var errorMessage = "Repl master handle error: master uuid not match";
            log.error(errorMessage);
            return Repl.error(slot, replPair, errorMessage);
        }

        var binlog = oneSlot.getBinlog();
        if (needFetchOffset != lastUpdatedOffset) {
            // check if slave already catch up to last binlog segment offset
            var fo = binlog.currentFileIndexAndOffset();
            if (fo.fileIndex() == needFetchFileIndex && fo.offset() == lastUpdatedOffset) {
                return Repl.reply(slot, replPair, ReplType.s_catch_up, EmptyContent.INSTANCE);
            }
        }

        byte[] readSegmentBytes;
        try {
            readSegmentBytes = binlog.readPrevRafOneSegment(needFetchFileIndex, needFetchOffset);
        } catch (IOException e) {
            var errorMessage = "Repl master handle error: read binlog file error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }

        if (readSegmentBytes == null) {
            return Repl.reply(slot, replPair, ReplType.s_catch_up, EmptyContent.INSTANCE);
        }

        var currentFileIndexAndOffset = binlog.currentFileIndexAndOffset();

        var responseBytes = new byte[4 + 8 + 4 + 8 + 4 + readSegmentBytes.length];
        var responseBuffer = ByteBuffer.wrap(responseBytes);
        responseBuffer.putInt(needFetchFileIndex);
        responseBuffer.putLong(needFetchOffset);
        responseBuffer.putInt(currentFileIndexAndOffset.fileIndex());
        responseBuffer.putLong(currentFileIndexAndOffset.offset());
        responseBuffer.putInt(readSegmentBytes.length);
        responseBuffer.put(readSegmentBytes);

        return Repl.reply(slot, replPair, ReplType.s_catch_up, new RawBytesContent(responseBytes));
    }

    Repl.ReplReply s_catch_up(byte slot, byte[] contentBytes) {
        // client received from server
        replPair.setLastGetCatchUpResponseMillis(System.currentTimeMillis());

        var oneSlot = localPersist.oneSlot(slot);
        var metaChunkSegmentIndex = oneSlot.getMetaChunkSegmentIndex();
        var binlogMasterUuid = metaChunkSegmentIndex.getMasterUuid();

        // last updated means next batch, but not fetch yet, refer end of this method
        var lastUpdatedFileIndexAndOffset = metaChunkSegmentIndex.getMasterBinlogFileIndexAndOffset();
        var lastUpdatedFileIndex = lastUpdatedFileIndexAndOffset.fileIndex();
        var lastUpdatedOffset = lastUpdatedFileIndexAndOffset.offset();

        // master has no more binlog to catch up, delay to catch up again
        if (EmptyContent.isEmpty(contentBytes)) {
            // use margin file offset
            var marginLastUpdatedOffset = Binlog.marginFileOffset(lastUpdatedOffset);
            var content = toMasterCatchUp(binlogMasterUuid, lastUpdatedFileIndex, marginLastUpdatedOffset, lastUpdatedOffset);
            oneSlot.delayRun(1000, () -> {
                replPair.write(ReplType.catch_up, content);
            });
            return Repl.emptyReply();
        }

        var buffer = ByteBuffer.wrap(contentBytes);
        var fetchedFileIndex = buffer.getInt();
        var fetchedOffset = buffer.getLong();

        var currentFileIndex = buffer.getInt();
        var currentOffset = buffer.getLong();

        var readSegmentLength = buffer.getInt();
        var readSegmentBytes = new byte[readSegmentLength];
        buffer.get(readSegmentBytes);

        // only when self is as slave but also as master, need to write binlog
        try {
            oneSlot.getBinlog().writeFromMasterOneSegmentBytes(readSegmentBytes, fetchedFileIndex, fetchedOffset);
        } catch (IOException e) {
            log.error("Repl slave write binlog from master error, slot: " + slot, e);
        }

        // update last catch up file index and offset
        var skipBytesN = 0;
        var isLastTimeCatchUpThisSegmentButNotCompleted = lastUpdatedFileIndex == fetchedFileIndex && lastUpdatedOffset > fetchedOffset;
        if (isLastTimeCatchUpThisSegmentButNotCompleted) {
            skipBytesN = (int) (lastUpdatedOffset - fetchedOffset);
        }

        try {
            var n = Binlog.decodeAndApply(slot, readSegmentBytes, skipBytesN, replPair);
            if (fetchedOffset == 0) {
                log.info("Repl binlog catch up success, slot={}, slave uuid={}, {}, catch up file index={}, catch up offset={}, apply n={}",
                        slot, replPair.getSlaveUuid(), replPair.getHostAndPort(), fetchedFileIndex, fetchedOffset, n);
            }
        } catch (Exception e) {
            var errorMessage = "Repl slave handle error: decode and apply binlog error";
            log.error(errorMessage, e);
            return Repl.error(slot, replPair, errorMessage + ": " + e.getMessage());
        }

        // set can read if catch up to current file, and offset not too far
        var isCatchUpToCurrentFile = fetchedFileIndex == currentFileIndex;
        if (isCatchUpToCurrentFile) {
            var diffOffset = currentOffset - fetchedOffset - skipBytesN;
            if (diffOffset < ConfForSlot.global.confRepl.catchUpOffsetMinDiff) {
                try {
                    if (!oneSlot.isCanRead()) {
                        oneSlot.setCanRead(true);
                        log.warn("Repl slave can read now as already catch up nearly to master latest, slot: {}", slot);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // catch up latest segment, delay to catch up again
        var marginCurrentOffset = Binlog.marginFileOffset(currentOffset);
        var isCatchUpOffsetInLatestSegment = isCatchUpToCurrentFile && fetchedOffset == marginCurrentOffset;
        if (isCatchUpOffsetInLatestSegment) {
            metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                    fetchedFileIndex, fetchedOffset + readSegmentLength);

            // still catch up current (latest) segment, delay
            var content = toMasterCatchUp(binlogMasterUuid, fetchedFileIndex, fetchedOffset, fetchedOffset + readSegmentLength);
            oneSlot.delayRun(1000, () -> {
                replPair.write(ReplType.catch_up, content);
            });
            return Repl.emptyReply();
        }

        var binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        if (readSegmentLength != binlogOneSegmentLength) {
            throw new IllegalStateException("Repl slave handle error: read segment length: " + readSegmentLength +
                    " is not equal to binlog one segment length: " + binlogOneSegmentLength);
        }

        var binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
        var isCatchUpLastSegmentInOneFile = fetchedOffset == (binlogOneFileMaxLength - binlogOneSegmentLength);
        var nextCatchUpFileIndex = isCatchUpLastSegmentInOneFile ? fetchedFileIndex + 1 : fetchedFileIndex;
        // one segment length may != binlog one segment length, need to re-fetch this segment
        var nextCatchUpOffset = isCatchUpLastSegmentInOneFile ? 0 : fetchedOffset + binlogOneSegmentLength;

        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(binlogMasterUuid, true,
                nextCatchUpFileIndex, nextCatchUpOffset);

        var content = toMasterCatchUp(binlogMasterUuid, nextCatchUpFileIndex, nextCatchUpOffset, nextCatchUpOffset);
        // when catch up to next file, delay to catch up again
        if (nextCatchUpOffset == 0) {
            log.info("Repl slave ready to catch up to next file, slot={}, slave uuid={}, {}, binlog file index={}, offset={}",
                    slot, replPair.getSlaveUuid(), replPair.getHostAndPort(), nextCatchUpFileIndex, nextCatchUpOffset);

            oneSlot.delayRun(1000, () -> {
                replPair.write(ReplType.catch_up, content);
            });
            return Repl.emptyReply();
        } else {
            return Repl.reply(slot, replPair, ReplType.catch_up, content);
        }
    }
}
