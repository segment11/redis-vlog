package redis.command

import redis.BaseCommand
import redis.reply.ErrorReply
import redis.reply.NilReply
import spock.lang.Specification

class ZGroupTest extends Specification {
    def singleKeyCmdList1 = '''
zadd
zcard
zcount
zincrby
zlexcount
zmscore
zpopmax
zpopmin
zrandmember
zrange
zrangebylex
zrangebyscore
zrank
zrem
zremrangebylex
zremrangebyrank
zremrangebyscore
zrevrange
zrevrangebylex
zrevrangebyscore
zrevrank
zscore
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList2 = '''
zdiff
zinter
zunion
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList3 = '''
zdiffstore
zinterstore
zunionstore
'''.readLines().collect { it.trim() }.findAll { it }

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]
        def data4 = new byte[4][]
        int slotNumber = 128

        and:
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        when:
        def sZdiff = ZGroup.parseSlots('zdiff', data1, slotNumber)
        def sZintercard = ZGroup.parseSlots('zintercard', data1, slotNumber)
        def sList = ZGroup.parseSlots('zxxx', data4, slotNumber)

        then:
        sZdiff.size() == 0
        sZintercard.size() == 0
        sList.size() == 0

        when:
        def sListList1 = singleKeyCmdList1.collect {
            ZGroup.parseSlots(it, data4, slotNumber)
        }
        def sListList11 = singleKeyCmdList1.collect {
            ZGroup.parseSlots(it, data1, slotNumber)
        }

        then:
        sListList1.size() == 22
        sListList1.every { it.size() == 1 }
        sListList11.size() == 22
        sListList11.every { it.size() == 0 }

        when:
        def sListList2 = multiKeyCmdList2.collect {
            ZGroup.parseSlots(it, data4, slotNumber)
        }
        def sListList22 = multiKeyCmdList2.collect {
            ZGroup.parseSlots(it, data1, slotNumber)
        }

        then:
        sListList2.size() == 3
        sListList2.every { it.size() > 1 }
        sListList22.size() == 3
        sListList22.every { it.size() == 0 }

        when:
        def data5 = new byte[5][]
        data5[1] = 'dst'.bytes
        data5[2] = '2'.bytes
        data5[3] = 'a'.bytes
        data5[4] = 'b'.bytes

        def sListList3 = multiKeyCmdList3.collect {
            ZGroup.parseSlots(it, data5, slotNumber)
        }
        def sListList33 = multiKeyCmdList3.collect {
            ZGroup.parseSlots(it, data1, slotNumber)
        }

        then:
        sListList3.size() == 3
        sListList3.every { it.size() == 3 }
        sListList33.size() == 3
        sListList33.every { it.size() == 0 }

        when:
        // zrangestore
        data5[1] = 'dst'.bytes
        data5[2] = 'a'.bytes
        data5[3] = '0'.bytes
        data5[4] = '-1'.bytes

        def sZrangestoreList = ZGroup.parseSlots('zrangestore', data5, slotNumber)

        then:
        sZrangestoreList.size() == 2

        when:
        sZrangestoreList = ZGroup.parseSlots('zrangestore', data1, slotNumber)

        then:
        sZrangestoreList.size() == 0

        when:
        // zintercard
        data4[1] = '2'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes

        def sZintercardList = ZGroup.parseSlots('zintercard', data4, slotNumber)

        then:
        sZintercardList.size() == 2

        when:
        sZintercardList = ZGroup.parseSlots('zintercard', data1, slotNumber)

        then:
        sZintercardList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def zGroup = new ZGroup('zadd', data1, null)
        zGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        def allCmdList = singleKeyCmdList1 + multiKeyCmdList2 + multiKeyCmdList3 + ['zrangestore', 'zintercard']

        when:
        zGroup.data = data1
        def sAllList = allCmdList.collect {
            zGroup.cmd = it
            zGroup.handle()
        }

        then:
        sAllList.every {
            it == ErrorReply.FORMAT
        }

        when:
        zGroup.cmd = 'zzz'
        def reply = zGroup.handle()

        then:
        reply == NilReply.INSTANCE
    }

}
