package redis.command

import io.netty.buffer.Unpooled
import redis.ConfForSlot
import redis.Dict
import redis.DictMap
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.repl.*
import redis.repl.content.Hello
import redis.repl.content.Hi
import redis.repl.content.Ping
import redis.repl.content.Pong
import redis.reply.NilReply
import spock.lang.Specification

class XGroupTest extends Specification {
    final byte slot = 0
    final short slotNumber = 1

    private byte[][] mockData(ReplPair replPair, ReplType replType, ReplContent content) {
        def reply = Repl.reply(slot, replPair, replType, content)
        def nettyBuf = Unpooled.wrappedBuffer(reply.buffer().array())
        Repl.decode(nettyBuf)
    }

    def 'test as master'() {
        given:
        def data4 = new byte[4][]
        data4[0] = new byte[8]
        data4[1] = new byte[1]
        data4[1][0] = 0
        data4[2] = new byte[1]
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
        r instanceof Repl.ReplReply
        ((Repl.ReplReply) r).isReplType(ReplType.pong)

        // hello
        when:
        def hello = new Hello(slaveUuid, 'localhost:6380')
        data = mockData(replPairAsSlave, ReplType.hello, hello)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r instanceof Repl.ReplReply
        ((Repl.ReplReply) r).isReplType(ReplType.hi)
        oneSlot.getReplPairAsMaster(slaveUuid) != null
        oneSlot.dynConfig.binlogOn

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
        def data = mockData(replPairAsMaster, ReplType.pong, pong)

        def x = new XGroup(null, data, null)
        x.replPair = null
        def r = x.handleRepl()
        then:
        r instanceof Repl.ReplReply
        // empty
        ((Repl.ReplReply) r).buffer().limit() == 0

        when:
        def replPairAsSlave0 = oneSlot.createReplPairAsSlave('localhost', 6379)
        r = x.handleRepl()
        then:
        r instanceof Repl.ReplReply
        // empty
        ((Repl.ReplReply) r).buffer().limit() == 0

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
        r instanceof Repl.ReplReply
        ((Repl.ReplReply) r).isReplType(ReplType.exists_dict)

        when:
        // first fetch dict, send local exist dict seq
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.persistDir)
        dictMap.putDict('key:', new Dict(new byte[10]))
        r = x.handleRepl()
        then:
        r instanceof Repl.ReplReply
        ((Repl.ReplReply) r).isReplType(ReplType.exists_dict)

        when:
        // already fetch all exists data, just catch up binlog
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl()
        then:
        r instanceof Repl.ReplReply
        ((Repl.ReplReply) r).isReplType(ReplType.catch_up)

        cleanup:
        localPersist.cleanUp()
        dictMap.close()
        Consts.persistDir.deleteDir()
    }
}
