package redis.command

import redis.BaseCommand
import redis.reply.ErrorReply
import redis.reply.NilReply
import redis.reply.OKReply
import spock.lang.Specification

class SGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data4 = new byte[4][]
        int slotNumber = 128

        and:
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        when:
        def sSetList = SGroup.parseSlots('set', data4, slotNumber)
        def sSintercardList = SGroup.parseSlots('sintercard', data4, slotNumber)
        def sSmoveList = SGroup.parseSlots('smove', data4, slotNumber)
        def sList = SGroup.parseSlots('sxxx', data4, slotNumber)

        then:
        sSetList.size() == 1
        sSintercardList.size() == 2
        sSmoveList.size() == 2
        sList.size() == 0

        when:
        def sListList = '''
setex
setnx
setrange
strlen
substr
sadd
scard
sismember
smembers
smismember
spop
srandmember
srem
'''.readLines().collect { it.trim() }.findAll { it }.collect {
            SGroup.parseSlots(it, data4, slotNumber)
        }

        then:
        sListList.size() == 13
        sListList.every { it.size() == 1 }

        when:
        def sListList2 = '''
sdiff
sdiffstore
sinter
sinterstore
sunion
sunionstore
'''.readLines().collect { it.trim() }.findAll { it }.collect {
            SGroup.parseSlots(it, data4, slotNumber)
        }

        then:
        sListList2.size() == 6
        sListList2.every { it.size() > 1 }

        when:
        def data1 = new byte[1][]

        sSetList = SGroup.parseSlots('set', data1, slotNumber)
        def sDiffList = SGroup.parseSlots('sdiff', data1, slotNumber)
        sSintercardList = SGroup.parseSlots('sintercard', data1, slotNumber)
        sSmoveList = SGroup.parseSlots('smove', data1, slotNumber)

        then:
        sSetList.size() == 0
        sDiffList.size() == 0
        sSintercardList.size() == 0
        sSmoveList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def sGroup = new SGroup('set', data1, null)
        sGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        def reply = sGroup.handle()

        then:
        reply == ErrorReply.FORMAT

        when:
        def replyList = '''
setex
setnx
setrange
strlen
substr
sadd
scard
sismember
smembers
smismember
spop
srandmember
srem
sdiff
sdiffstore
sinter
sinterstore
sunion
sunionstore
sintercard
smove
select
slaveof
'''.readLines().collect { it.trim() }.findAll { it }.collect {
            sGroup.cmd = it
            sGroup.handle()
        }

        then:
        replyList.every { it == ErrorReply.FORMAT }

        when:
        sGroup.cmd = 'save'
        reply = sGroup.handle()

        then:
        reply == OKReply.INSTANCE

        when:
        sGroup.cmd = 'zzz'
        reply = sGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }
}
