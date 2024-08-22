package redis.command

import redis.BaseCommand
import redis.Dict
import redis.DictMap
import redis.persist.Consts
import redis.persist.LocalPersist
import redis.persist.LocalPersistTest
import redis.persist.Mock
import redis.repl.incremental.XOneWalGroupPersist
import redis.reply.*
import spock.lang.Specification

class ManageCommandTest extends Specification {
    final byte slot = 0

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        ManageCommand.parseSlots('manage', data1, 1).size() == 0

        when:
        def data5 = new byte[5][]
        data5[1] = 'slot'.bytes
        data5[2] = '0'.bytes
        def sList = ManageCommand.parseSlots('manage', data5, 1)
        then:
        sList.size() == 1

        when:
        data5[2] = 'a'.bytes
        sList = ManageCommand.parseSlots('manage', data5, 1)
        then:
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'slot'.bytes
        sList = ManageCommand.parseSlots('manage', data4, 1)
        then:
        sList.size() == 0

        when:
        data4[1] = 'xxx'.bytes
        sList = ManageCommand.parseSlots('manage', data4, 1)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def mGroup = new MGroup('manage', data1, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)

        when:
        def reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        data2[1] = 'debug'.bytes
        manage.data = data2
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'dyn-config'.bytes
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'dict'.bytes
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'slot'.bytes
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'zzz'.bytes
        reply = manage.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test debug'() {
        given:
        def data4 = new byte[4][]

        def mGroup = new MGroup('manage', data4, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        when:
        data4[2] = 'calc-key-hash'.bytes
        data4[3] = 'key:0'.bytes
        def reply = manage.debug()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        def data5 = new byte[5][]
        data5[2] = 'calc-key-hash'.bytes
        manage.data = data5
        reply = manage.debug()
        then:
        reply == ErrorReply.FORMAT

        when:
        data5[2] = 'log-switch'.bytes
        data5[3] = 'logCmd'.bytes
        data5[4] = 'true'.bytes
        manage.data = data5
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[4] = '1'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'logMerge'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'logTrainDict'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'logRestore'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'bulkLoad'.bytes
        data5[4] = '0'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'xxx'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = 'log-switch'.bytes
        manage.data = data4
        reply = manage.debug()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[2] = 'xxx'.bytes
        reply = manage.debug()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.debug()
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test dyn-config'() {
        given:
        def data4 = new byte[4][]

        def mGroup = new MGroup('manage', data4, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        data4[1] = 'dyn-config'.bytes
        data4[2] = 'testKey'.bytes
        data4[3] = '1'.bytes
        def reply = manage.dynConfig()
        then:
        reply instanceof AsyncReply

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.dynConfig()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test dict'() {
        given:
        def data4 = new byte[4][]

        def mGroup = new MGroup('manage', data4, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)

        def dict = new Dict()
        dict.dictBytes = 'test'.bytes
        dict.seq = 1
        dict.createdTime = System.currentTimeMillis()
        dictMap.putDict('key:', dict)

        when:
        data4[1] = 'dict'.bytes
        data4[2] = 'set-key-prefix-groups'.bytes
        data4[3] = 'key:,xxx:'.bytes
        def reply = manage.dict()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[3] = ''.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = 'view-dict-summary'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[2] = 'output-dict-bytes'.bytes
        // use dict seq
        data4[3] = '1'.bytes
        reply = manage.dict()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = 'output-dict-bytes'.bytes
        // use dict seq, but not exists
        data4[3] = '2'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        def data3 = new byte[3][]
        data3[1] = 'dict'.bytes
        data3[2] = 'view-dict-summary'.bytes
        manage.data = data3
        reply = manage.dict()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        // train new dict
        def data15 = new byte[15][]
        data15[1] = 'dict'.bytes
        data15[2] = 'train-new-dict'.bytes
        data15[3] = 'key:'.bytes
        11.times {
            data15[it + 4] = ('aaaaabbbbbccccc' * 5).bytes
        }
        manage.data = data15
        reply = manage.dict()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        def data14 = new byte[14][]
        data14[1] = 'dict'.bytes
        data14[2] = 'train-new-dict'.bytes
        manage.data = data14
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        data3[2] = 'output-dict-bytes'.bytes
        manage.data = data3
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        when:
        data3[2] = 'set-key-prefix-groups'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        when:
        data3[2] = 'xxx'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        dictMap.clearAll()
        dictMap.close()
    }

    def 'manage in on slot'() {
        given:
        def data5 = new byte[5][]

        def mGroup = new MGroup('manage', data5, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        data5[1] = 'slot'.bytes
        data5[2] = '0'.bytes
        data5[3] = 'view-bucket-key-count'.bytes
        def reply = manage.manageInOneSlot()
        then:
        reply instanceof IntegerReply

        when:
        data5[3] = 'view-bucket-keys'.bytes
        data5[4] = ''.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        data5[4] = 'iterate'.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        // set key bucket key / values for iterate
        def shortValueList = Mock.prepareShortValueList(10, 0)
        oneSlot.keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList,
                new XOneWalGroupPersist(true, false, 0))
        data5[4] = ''.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        data5[4] = 'iterate'.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        data5[3] = 'update-kv-lru-max-size'.bytes
        data5[4] = '10000'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'bucket'.bytes
        data5[4] = '0'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        when:
        data5[4] = 'a'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        def data7 = new byte[7][]
        data7[1] = 'slot'.bytes
        data7[2] = '0'.bytes
        data7[3] = 'bucket'.bytes
        data7[4] = '0'.bytes
        data7[5] = 'view-bucket-key-count'.bytes
        data7[6] = ''.bytes
        manage.data = data7
        reply = manage.manageInOneSlot()
        then:
        reply instanceof IntegerReply

        when:
        data7[5] = 'view-bucket-keys'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        def data4 = new byte[4][]
        data4[1] = 'slot'.bytes
        data4[2] = '0'.bytes
        data4[3] = 'update-kv-lru-max-size'.bytes
        manage.data = data4
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[3] = 'xxx'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
