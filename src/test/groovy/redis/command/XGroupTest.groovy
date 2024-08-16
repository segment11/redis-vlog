package redis.command

import io.netty.buffer.Unpooled
import redis.ConfForSlot
import redis.Dict
import redis.DictMap
import redis.persist.*
import redis.repl.*
import redis.repl.Repl.ReplReply
import redis.repl.content.Hello
import redis.repl.content.Hi
import redis.repl.content.Ping
import redis.repl.content.Pong
import redis.repl.incremental.XWalV
import redis.reply.NilReply
import spock.lang.Specification

import java.nio.ByteBuffer

class XGroupTest extends Specification {
    final byte slot = 0
    final short slotNumber = 1

    private byte[][] mockData(ReplPair replPair, ReplType replType, ReplContent content) {
        def reply = Repl.reply(slot, replPair, replType, content)
        mockData(reply)
    }

    private byte[][] mockData(ReplReply reply) {
        def nettyBuf = Unpooled.wrappedBuffer(reply.buffer().array())
        Repl.decode(nettyBuf)
    }

    def 'test as master'() {
        given:
        def data4 = new byte[4][]
        // slave uuid long
        data4[0] = new byte[8]
        // slot
        data4[1] = new byte[1]
        data4[1][0] = slot
        // repl type
        data4[2] = new byte[1]
        // no exist repl type
        data4[2][0] = (byte) -10
        def xGroup = new XGroup(null, data4, null)

        expect:
        XGroup.parseSlots(null, data4, slotNumber).size() == 0
        xGroup.handle() == NilReply.INSTANCE
        xGroup.handleRepl() == null

        when:
        ConfForSlot.global.netListenAddresses = 'localhost:6379'

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        // mock from slave repl request data
        final long slaveUuid = 1L
        def replPairAsSlave = ReplPairTest.mockAsSlave(0L, slaveUuid)
        def ping = new Ping('localhost:6380')
        def data = mockData(replPairAsSlave, ReplType.ping, ping)

        def x = new XGroup(null, data, null)
        def r = x.handleRepl()
        then:
        r.isReplType(ReplType.pong)

        when:
        // handle ping again, already created repl pair as master when first received ping
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.pong)

        // hello
        when:
        def hello = new Hello(slaveUuid, 'localhost:6380')
        data = mockData(replPairAsSlave, ReplType.hello, hello)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.hi)
        oneSlot.getReplPairAsMaster(slaveUuid) != null
        oneSlot.dynConfig.binlogOn

        // bye
        when:
        data = mockData(replPairAsSlave, ReplType.bye, ping)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.byeBye)

        when:
        // remove repl pair
        oneSlot.doTask(0)
        x.replPair = null
        r = x.handleRepl()
        then:
        // empty
        r.isEmpty()

        when:
        // master receive hello from slave, then create repl pair again
        data = mockData(replPairAsSlave, ReplType.hello, hello)
        x = new XGroup(null, data, null)
        x.handleRepl()
        ByteBuffer.wrap(data4[0]).putLong(slaveUuid)
        // response exists chunk segments
        data4[2][0] = ReplType.exists_chunk_segments.code

        def metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        def contentBytes = new byte[4 + 4 + metaBytes.length]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        requestBuffer.put(metaBytes)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // skip as meta bytes is same
        r.buffer().limit() == Repl.HEADER_LENGTH + 8

        when:
        // meta bytes not same
        requestBuffer.put(8, (byte) 1)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // only meta bytes, chunk segment bytes not write yet
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 8 + FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * MetaChunkSegmentFlagSeq.ONE_LENGTH

        when:
        // chunk segment bytes exists
        oneSlot.chunk.writeSegmentToTargetSegmentIndex(new byte[4096], 0)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // meta bytes with just one chunk segment bytes
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 8 + FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * MetaChunkSegmentFlagSeq.ONE_LENGTH + 4096

        // response exists wal
        when:
        data4[2][0] = ReplType.exists_wal.code
        contentBytes = new byte[4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // wal group index
        requestBuffer.putInt(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_wal)

        when:
        requestBuffer.position(0)
        // no log
        requestBuffer.putInt(99)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_wal)

        when:
        // exception
        def walGroupNumber = Wal.calcWalGroupNumber()
        requestBuffer.position(0)
        requestBuffer.putInt(walGroupNumber)
        r = x.handleRepl()
        then:
        // error
        r.isReplType(ReplType.error)

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(-1)
        r = x.handleRepl()
        then:
        // error
        r.isReplType(ReplType.error)

        // response exists key buckets
        when:
        data4[2][0] = ReplType.exists_key_buckets.code
        contentBytes = new byte[1 + 4 + 8]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // split index
        requestBuffer.put((byte) 0)
        // begin bucket index
        requestBuffer.putInt(0)
        // one wal group seq
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_key_buckets)
        // refer XGroup method exists_key_buckets
        r.buffer().limit() == Repl.HEADER_LENGTH + 1 + 1 + 4 + 1 + 8

        when:
        // one wal group seq not match
        requestBuffer.putLong(1 + 4, -1L)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_key_buckets)
        // key buckets not exists
        r.buffer().limit() == Repl.HEADER_LENGTH + 1 + 1 + 4 + 1 + 8

        when:
        def sharedBytesList = new byte[1][]
        sharedBytesList[0] = new byte[4096 * ConfForSlot.global.confWal.oneChargeBucketNumber]
        oneSlot.keyLoader.writeSharedBytesList(sharedBytesList, 0)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_key_buckets)
        // key buckets exists
        r.buffer().limit() == Repl.HEADER_LENGTH + 1 + 1 + 4 + 1 + 8 + sharedBytesList[0].length

        // stat_key_count_in_buckets
        when:
        data4[2][0] = ReplType.stat_key_count_in_buckets.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_stat_key_count_in_buckets)
        r.buffer().limit() == Repl.HEADER_LENGTH + ConfForSlot.global.confBucket.bucketsPerSlot * 2

        // meta_key_bucket_split_number
        when:
        data4[2][0] = ReplType.meta_key_bucket_split_number.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_meta_key_bucket_split_number)
        r.buffer().limit() == Repl.HEADER_LENGTH + ConfForSlot.global.confBucket.bucketsPerSlot

        // incremental_big_string
        when:
        data4[2][0] = ReplType.incremental_big_string.code
        // big string uuid long
        contentBytes = new byte[8]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_incremental_big_string)
        r.buffer().limit() == Repl.HEADER_LENGTH + 8

        when:
        def bigStringUuid = 1L
        oneSlot.bigStringFiles.writeBigStringBytes(bigStringUuid, 'test-big-string-key', new byte[1024])
        ByteBuffer.wrap(contentBytes).putLong(bigStringUuid)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_incremental_big_string)
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 1024

        // exists_big_string
        when:
        data4[2][0] = ReplType.exists_big_string.code
        contentBytes = new byte[1]
        data4[3] = contentBytes
        // master has one big string
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_big_string)

        when:
        oneSlot.bigStringFiles.deleteBigStringFileIfExist(bigStringUuid)
        // master has no big string
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_big_string)

        when:
        contentBytes = new byte[8 * 2]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(1L)
        requestBuffer.putLong(2L)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_big_string)

        // exists_dict
        when:
        data4[2][0] = ReplType.exists_dict.code
        contentBytes = new byte[1]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_dict)

        when:
        contentBytes = new byte[4 * 2]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(1)
        requestBuffer.putInt(2)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_dict)

        // exists_all_done
        when:
        data4[2][0] = ReplType.exists_all_done.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_all_done)

        // catch_up
        when:
        data4[2][0] = ReplType.catch_up.code
        contentBytes = new byte[8 + 4 + 8 + 8]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // master uuid long
        // not match
        requestBuffer.putLong(oneSlot.masterUuid + 1)
        // binlog file index
        requestBuffer.putInt(0)
        // binlog file offset
        requestBuffer.putLong(0)
        // last updated file offset
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.error)

        when:
        requestBuffer.position(0)
        requestBuffer.putLong(oneSlot.masterUuid)
        r = x.handleRepl()
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        def vList = Mock.prepareValueList(10)
        for (v in vList) {
            oneSlot.appendBinlog(new XWalV(v))
        }
        // read segment bytes < one segment length
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8 + 4 + 8)
        requestBuffer.putLong(106)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8 + 4 + 8)
        requestBuffer.putLong(oneSlot.binlog.currentFileIndexAndOffset().offset())
        r = x.handleRepl()
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8)
        // file index not match master binlog current file index
        requestBuffer.putInt(1)
        requestBuffer.putLong(0)
        requestBuffer.putLong(oneSlot.binlog.currentFileIndexAndOffset().offset())
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        boolean exception = false
        requestBuffer.position(0)
        requestBuffer.putLong(oneSlot.masterUuid)
        requestBuffer.putInt(0)
        // not margin
        requestBuffer.putLong(1)
        try {
            x.handleRepl()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test as slave'() {
        given:
        ConfForSlot.global.netListenAddresses = 'localhost:6380'
        ConfForSlot.global.confChunk.REPL_EMPTY_BYTES_FOR_ONCE_WRITE = new byte[FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * 4096]

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        // mock from master repl response data
        final long masterUuid = 10L
        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid
        def pong = new Pong('localhost:6379')
        def data = mockData(replPairAsMaster, ReplType.pong, pong)
        def x = new XGroup(null, data, null)
        x.replPair = null
        def r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        oneSlot.createReplPairAsSlave('localhost', 6379)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        // error
        data = mockData(Repl.error(slot, replPairAsMaster, 'error'))
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isEmpty()

        // hi
        when:
        var metaChunkSegmentIndex = oneSlot.metaChunkSegmentIndex
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false, 0, 0L)
        def hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        data = mockData(replPairAsMaster, ReplType.hi, hi)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.exists_dict)

        when:
        // first fetch dict, send local exist dict seq
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.persistDir)
        dictMap.putDict('key:', new Dict(new byte[10]))
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.exists_dict)

        when:
        // already fetch all exists data, just catch up binlog
        // from beginning
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // last updated offset not margined
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 106L)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // last updated offset margined
        def binlogOneSegmentLengthTmp = ConfForSlot.global.confRepl.binlogOneSegmentLength
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, binlogOneSegmentLengthTmp)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // not match slave uuid
        hi = new Hi(replPairAsMaster.slaveUuid + 1, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        data = mockData(replPairAsMaster, ReplType.hi, hi)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r == null

        // ok
        when:
        data = mockData(Repl.ok(slot, replPairAsMaster, 'ok'))
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isEmpty()

        // byeBye
        when:
        data = mockData(replPairAsMaster, ReplType.byeBye, pong)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        def data4 = new byte[4][]
        // slave uuid long
        data4[0] = new byte[8]
        ByteBuffer.wrap(data4[0]).putLong(replPairAsMaster.slaveUuid)
        // slot
        data4[1] = new byte[1]
        data4[1][0] = slot
        // repl type
        data4[2] = new byte[1]
        // fetch exists chunk segments
        data4[2][0] = ReplType.s_exists_chunk_segments.code

        def contentBytes = new byte[8]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_chunk_segments)

        when:
        // last batch
        requestBuffer.position(0)
        requestBuffer.putInt(ConfForSlot.global.confChunk.maxSegmentNumber() - FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.exists_wal)

        when:
        requestBuffer.position(0)
        // next batch will delay run
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * 9)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        def metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        contentBytes = new byte[8 + 4 + metaBytes.length + 4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(1024)
        requestBuffer.putInt(metaBytes.length)
        requestBuffer.put(metaBytes)
        requestBuffer.putInt(0)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_chunk_segments)

        when:
        contentBytes = new byte[8 + 4 + metaBytes.length + 4 + 4096]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(1024)
        requestBuffer.putInt(metaBytes.length)
        requestBuffer.put(metaBytes)
        requestBuffer.putInt(4096)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_chunk_segments)

        // exists wal
        when:
        data4[2][0] = ReplType.s_exists_wal.code
        contentBytes = new byte[16 + 2 * Wal.ONE_GROUP_BUFFER_SIZE]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // wal group index
        requestBuffer.putInt(0)
        requestBuffer.putInt(Wal.ONE_GROUP_BUFFER_SIZE)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_wal)

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(99)
        r = x.handleRepl()
        then:
        // delay fetch next batch
        r.isEmpty()

        when:
        // last batch
        def walGroupNumber = Wal.calcWalGroupNumber()
        requestBuffer.position(0)
        requestBuffer.putInt(walGroupNumber - 1)
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_all_done)

        // fetch exists key buckets
        when:
        data4[2][0] = ReplType.s_exists_key_buckets.code
        contentBytes = new byte[1 + 1 + 4 + 1 + 8]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // split index
        requestBuffer.put((byte) 0)
        // max split number
        requestBuffer.put((byte) 1)
        // begin bucket index
        requestBuffer.putInt(0)
        // is skip flag
        requestBuffer.put((byte) 1)
        // one wal group seq
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_key_buckets)

        when:
        // is skip flag false
        requestBuffer.put(1 + 1 + 4, (byte) 0)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_key_buckets)

        when:
        contentBytes = new byte[1 + 1 + 4 + 1 + 8 + 4096 * ConfForSlot.global.confWal.oneChargeBucketNumber]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // split index
        requestBuffer.put((byte) 0)
        // max split number
        requestBuffer.put((byte) 1)
        // begin bucket index
        requestBuffer.putInt(0)
        // is skip flag
        requestBuffer.put((byte) 0)
        // one wal group seq
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_key_buckets)

        when:
        // last batch in split index 0
        // max split number
        requestBuffer.put(1, (byte) 3)
        requestBuffer.putInt(1 + 1, ConfForSlot.global.confBucket.bucketsPerSlot - ConfForSlot.global.confWal.oneChargeBucketNumber)
        r = x.handleRepl()
        then:
        // delay fetch next batch
        r.isEmpty()

        when:
        // last batch in split index 2
        requestBuffer.put(0, (byte) 2)
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_chunk_segments)

        // s_stat_key_count_in_buckets
        when:
        data4[2][0] = ReplType.s_stat_key_count_in_buckets.code
        contentBytes = new byte[ConfForSlot.global.confBucket.bucketsPerSlot * 2]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_key_buckets)

        // s_meta_key_bucket_split_number
        when:
        data4[2][0] = ReplType.s_meta_key_bucket_split_number.code
        contentBytes = new byte[ConfForSlot.global.confBucket.bucketsPerSlot]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.stat_key_count_in_buckets)

        // s_incremental_big_string
        when:
        data4[2][0] = ReplType.s_incremental_big_string.code
        contentBytes = new byte[8]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        contentBytes = new byte[8 + 1024]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(1L)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()
        oneSlot.bigStringFiles.getBigStringBytes(1L).length == 1024

        // s_exists_big_string
        when:
        data4[2][0] = ReplType.s_exists_big_string.code
        contentBytes = new byte[1]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.meta_key_bucket_split_number)

        when:
        contentBytes = new byte[2 + 1]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // big string count short
        requestBuffer.putShort((short) 0)
        // is sent all once flag
        requestBuffer.put((byte) 1)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.meta_key_bucket_split_number)

        when:
        // mock two big string fetched
        contentBytes = new byte[2 + 1 + (8 + 4 + 1024) * 2]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putShort((short) 2)
        requestBuffer.put((byte) 1)
        requestBuffer.putLong(1L)
        requestBuffer.putInt(1024)
        requestBuffer.position(requestBuffer.position() + 1024)
        requestBuffer.putLong(2L)
        requestBuffer.putInt(1024)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.meta_key_bucket_split_number)

        when:
        // is sent all false
        requestBuffer.put(2, (byte) 0)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_big_string)

        when:
        def bigStringFileUuidList = oneSlot.bigStringFiles.bigStringFileUuidList
        if (bigStringFileUuidList) {
            for (uuid in bigStringFileUuidList) {
                oneSlot.bigStringFiles.deleteBigStringFileIfExist(uuid)
            }
        }
        r = x.fetchExistsBigString(slot, oneSlot)
        then:
        // empty content return
        r.isReplType(ReplType.exists_big_string)

        // s_exists_dict
        when:
        data4[2][0] = ReplType.s_exists_dict.code
        // dict count int
        contentBytes = new byte[4]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_big_string)

        when:
        def dict1 = new Dict(new byte[10])
        def dict2 = new Dict(new byte[20])
        def encoded1 = dict1.encode('k1')
        def encoded2 = dict2.encode(Dict.GLOBAL_ZSTD_DICT_KEY)
        contentBytes = new byte[4 + 4 + encoded1.length + 4 + encoded2.length]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(2)
        requestBuffer.putInt(encoded1.length)
        requestBuffer.put(encoded1)
        requestBuffer.putInt(encoded2.length)
        requestBuffer.put(encoded2)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_big_string)
        dictMap.getDict('k1') != null
        Dict.GLOBAL_ZSTD_DICT.hasDictBytes()

        // s_exists_all_done
        when:
        data4[2][0] = ReplType.s_exists_all_done.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.catch_up)
        oneSlot.metaChunkSegmentIndex.isExistsDataAllFetched()

        // s_catch_up
        when:
        // clear slave catch up binlog file index and offset
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        data4[2][0] = ReplType.s_catch_up.code
        // mock 10 wal values in binlog
        int n = 0
        def vList = Mock.prepareValueList(10)
        for (v in vList) {
            n += new XWalV(v).encodedLength()
        }
        contentBytes = new byte[4 + 8 + 4 + 8 + 4 + n]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // response binlog file index
        requestBuffer.putInt(0)
        // response binlog file offset
        requestBuffer.putLong(0)
        // current(latest) binlog file index
        requestBuffer.putInt(0)
        // current(latest) binlog file offset
        requestBuffer.putLong(n)
        // one segment bytes response
        requestBuffer.putInt(n)
        for (v in vList) {
            def encoded = new XWalV(v).encodeWithType()
            requestBuffer.put(encoded)
        }
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        def binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        contentBytes = new byte[4 + 8 + 4 + 8 + 4 + binlogOneSegmentLength]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneSegmentLength * 2)
        requestBuffer.putInt(binlogOneSegmentLength)
        int mockSkipN = 0
        for (v in vList) {
            def encoded = new XWalV(v).encodeWithType()
            mockSkipN = encoded.length
            requestBuffer.put(encoded)
        }
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.catch_up)
        oneSlot.metaChunkSegmentIndex.masterBinlogFileIndexAndOffset.fileIndex() == 0
        oneSlot.metaChunkSegmentIndex.masterBinlogFileIndexAndOffset.offset() == binlogOneSegmentLength

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneSegmentLength)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneSegmentLength - 1)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, mockSkipN)
        r = x.handleRepl()
        then:
        // delay to fetch next batch
        r.isEmpty()

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        def binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength
        requestBuffer.position(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneFileMaxLength - binlogOneSegmentLength)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl()
        then:
        // begin with new binlog file next batch, delay
        r.isEmpty()

        when:
        boolean exception = false
        requestBuffer.position(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(1)
        requestBuffer.put((byte) 0)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        try {
            x.handleRepl()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        // empty from master, mean no new binlog
        contentBytes = new byte[1]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()

        cleanup:
        localPersist.cleanUp()
        dictMap.close()
        Consts.persistDir.deleteDir()
    }
}
