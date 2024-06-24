package redis

import com.github.luben.zstd.Zstd
import io.activej.config.Config
import io.activej.net.socket.tcp.ITcpSocket
import redis.decode.Request
import redis.reply.Reply
import spock.lang.Specification

class BaseCommandTest extends Specification {
    static class SubCommand extends BaseCommand {
        SubCommand(String cmd, byte[][] data, ITcpSocket socket) {
            super(cmd, data, socket)
        }

        @Override
        Reply handle() {
            return null
        }
    }

    def 'test static methods'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)

        def k1 = 'key1'
        def s1 = BaseCommand.slot(k1.bytes, 1)
        println s1

        def k11 = 'key11'
        def s11 = BaseCommand.slot(k11.bytes, 2)

        def k2 = 'key2{x'
        def s2 = BaseCommand.slot(k2.bytes, 1)

        def k22 = 'key2}x'
        def s22 = BaseCommand.slot(k22.bytes, 1)

        def k3 = 'key3{x}'
        def s3 = BaseCommand.slot(k3.bytes, 1)

        def k33 = 'key3{x}'
        def s33 = BaseCommand.slot(k33.bytes, 2)

        def k4 = 'key4{x}'
        def s4 = BaseCommand.slot(k4.bytes, 1)

        def k5 = 'key5{xyz}'
        def s5 = BaseCommand.slot(k5.bytes, 1)

        expect:
        s1.slot == 0
        s1.bucketIndex < 16384
        s1.keyHash != 0

        s3.slot == s4.slot
    }

    def 'test all'() {
        given:
        byte[][] data = new byte[2][0]
        data[0] = 'get'.bytes
        data[1] = 'key'.bytes
        def c = new SubCommand('get', data, null)

        expect:
        c.cmd == 'get'
        c.data == data
        c.socket == null

        when:
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, null, Config.create())
        c.init(requestHandler, new Request(data, false, false))

        // overwrite
        c.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))
        c.byPassGetSet = null

        then:
        c.workerId == 0
        c.netWorkers == 1
        c.slotNumber == 1

        c.compressStats != null
        c.compressLevel == Zstd.defaultCompressionLevel()
        c.trainSampleListMaxSize == 100
        c.snowFlake != null
        c.trainSampleJob != null
        c.sampleToTrainList.size() == 0

        !c.localTest
        c.localTestRandomValueListSize == 0
        c.localTestRandomValueList.size() == 0

        c.slotWithKeyHashListParsed.size() == 0
        !c.isCrossRequestWorker

        c.handle() == null

        when:
        def s1 = c.slot('key1'.bytes)
        def s11 = c.slotPreferParsed('key11'.bytes, 0)

        c.slotWithKeyHashListParsed = null
        s11 = c.slotPreferParsed('key11'.bytes, 0)

        c.slotWithKeyHashListParsed = [s1]
        def s111 = c.slotPreferParsed('key11'.bytes, 0)
        def s1111 = c.slotPreferParsed('key11'.bytes, 1)
        def s2 = c.slotPreferParsed('key11'.bytes)

        then:
        s1.slot == 0
        s11.slot == 0
        s111.slot == 0
        s1111.slot == 0
        s2.slot == 0
    }
}
