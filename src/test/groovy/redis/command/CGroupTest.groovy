package redis.command

import io.activej.eventloop.Eventloop
import io.activej.net.socket.tcp.TcpSocket
import redis.BaseCommand
import redis.mock.InMemoryGetSet
import redis.persist.LocalPersist
import redis.persist.Mock
import redis.reply.*
import spock.lang.Specification

import java.nio.channels.SocketChannel
import java.time.Duration

class CGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        byte[][] data = new byte[3][]
        int slotNumber = 128

        and:
        data[1] = 'a'.bytes
        data[2] = 'b'.bytes

        when:
        def slotWithKeyHash = CGroup.parseSlot('copy', data, slotNumber)
        def slotWithKeyHashListCopy = CGroup.parseSlots('copy', data, slotNumber)
        def slotWithKeyHashListConfig = CGroup.parseSlots('config', data, slotNumber)

        then:
        slotWithKeyHash == null
        slotWithKeyHashListConfig[0] == null

        slotWithKeyHashListCopy.size() == 2
        slotWithKeyHashListCopy[0] != null
        slotWithKeyHashListCopy[1] != null

        when:
        data = new byte[2][]

        slotWithKeyHashListCopy = CGroup.parseSlots('copy', data, slotNumber)

        then:
        slotWithKeyHashListCopy.size() == 0
    }

    def 'test handle'() {
        given:
        byte[][] data = new byte[1][]

        def cGroup = new CGroup('client', data, null)
        cGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        cGroup.handle()
        cGroup.cmd = 'config'
        cGroup.handle()
        cGroup.cmd = 'copy'
        cGroup.handle()

        then:
        1 == 1

        when:
        cGroup.cmd = 'zzz'
        def reply = cGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test client'() {
        given:
        byte[][] data = new byte[1][]

        def socket = TcpSocket.wrapChannel(null, SocketChannel.open(),
                new InetSocketAddress('localhost', 46379), null)

        def cGroup = new CGroup('client', data, socket)
        cGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = cGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        data = new byte[2][]
        data[1] = 'id'.bytes
        cGroup.data = data

        reply = cGroup.handle()

        then:
        reply instanceof IntegerReply

        when:
        data[1] = 'setinfo'.bytes

        reply = cGroup.handle()

        then:
        reply == OKReply.INSTANCE

        when:
        data[1] = 'zzz'.bytes

        reply = cGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

    def 'test copy'() {
        given:
        final byte slot = 0

        byte[][] data = new byte[3][]
        data[1] = 'a'.bytes
        data[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def cGroup = new CGroup('copy', data, null)
        cGroup.byPassGetSet = inMemoryGetSet
        cGroup.from(BaseCommand.mockAGroup(slot, (byte) 1, (short) 1))

        when:
        cGroup.slotWithKeyHashListParsed = CGroup.parseSlots('copy', data, cGroup.slotNumber)
        def reply = cGroup.copy()

        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)

        reply = cGroup.copy()

        then:
        reply == IntegerReply.REPLY_1

        when:
        data = new byte[4][]
        data[1] = 'a'.bytes
        data[2] = 'b'.bytes
        data[3] = 'replace_'.bytes
        cGroup.data = data

        reply = cGroup.copy()

        then:
        reply == IntegerReply.REPLY_0

        when:
        data[3] = 'replace'.bytes

        reply = cGroup.copy()

        then:
        reply == IntegerReply.REPLY_1

        when:
        var eventloop = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)

        Thread.start {
            eventloop.run()
        }

        LocalPersist.instance.addOneSlotForTest(slot, eventloop)

        cGroup.isCrossRequestWorker = true

        reply = cGroup.copy()

        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')

        reply = cGroup.copy()

        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        data[3] = 'replace_'.bytes

        reply = cGroup.copy()

        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
