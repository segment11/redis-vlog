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
        r.buffer().limit() == 0

        when:
        // master receive hello from slave, then create repl pair again
        data = mockData(replPairAsSlave, ReplType.hello, hello)
        x = new XGroup(null, data, null)
        x.handleRepl()
        ByteBuffer.wrap(data4[0]).putLong(slaveUuid)
        data4[2][0] = ReplType.exists_chunk_segments.code

        def metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, FdReadWrite.REPL_ONCE_INNER_COUNT)
        def contentBytes = new byte[4 + 4 + metaBytes.length]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_INNER_COUNT)
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
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 8 + FdReadWrite.REPL_ONCE_INNER_COUNT * MetaChunkSegmentFlagSeq.ONE_LENGTH

        when:
        // chunk segment bytes exists
        oneSlot.chunk.writeSegmentToTargetSegmentIndex(new byte[4096], 0)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // meta bytes with just one chunk segment bytes
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 8 + FdReadWrite.REPL_ONCE_INNER_COUNT * MetaChunkSegmentFlagSeq.ONE_LENGTH + 4096

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test as slave'() {
        given:
        ConfForSlot.global.netListenAddresses = 'localhost:6380'

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
        data = mockData(replPairAsMaster, ReplType.pong, pong)
        x = new XGroup(null, data, null)
        x.replPair = null
        r = x.handleRepl()
        then:
        // empty
        r.buffer().limit() == 0

        when:
        oneSlot.createReplPairAsSlave('localhost', 6379)
        r = x.handleRepl()
        then:
        // empty
        r.buffer().limit() == 0

        when:
        // error
        def data = mockData(Repl.error(slot, replPairAsMaster, 'error'))
        def x = new XGroup(null, data, null)
        def r = x.handleRepl()
        then:
        // empty
        r.buffer().limit() == 0

        // hi
        when:
        var metaChunkSegmentIndex = oneSlot.metaChunkSegmentIndex
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false, 0, 0L)
        def hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L))
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
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // not match slave uuid
        hi = new Hi(replPairAsMaster.slaveUuid + 1, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L))
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
        // empty
        r.buffer().limit() == 0

        // byeBye
        when:
        data = mockData(replPairAsMaster, ReplType.byeBye, pong)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        // empty
        r.buffer().limit() == 0

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
        data4[2][0] = ReplType.s_exists_chunk_segments.code

        def contentBytes = new byte[8]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_INNER_COUNT)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_chunk_segments)

        when:
        // last batch
        requestBuffer.position(0)
        requestBuffer.putInt(ConfForSlot.global.confChunk.maxSegmentNumber() - FdReadWrite.REPL_ONCE_INNER_COUNT)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.exists_all_done)

        when:
        def metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, FdReadWrite.REPL_ONCE_INNER_COUNT)
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

        cleanup:
        localPersist.cleanUp()
        dictMap.close()
        Consts.persistDir.deleteDir()
    }
}
