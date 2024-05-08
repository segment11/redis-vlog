package redis.command

import redis.BaseCommand
import redis.mock.InMemoryGetSet
import spock.lang.Specification

class AGroupTest extends Specification {
    def "ParseSlot"() {
        given:
        byte[][] dataAppend = new byte[3][]
        int slotNumber = 128

        and:
        dataAppend[1] = 'a'.bytes

        when:
        def slotWithKeyHash = AGroup.parseSlot('append', dataAppend, slotNumber)

        then:
        slotWithKeyHash.slot() == 63
    }

    def "Append"() {
        given:
        byte[][] dataAppend = new byte[3][]
        def inMemoryGetSet = new InMemoryGetSet()

        and:
        dataAppend[1] = 'a'.bytes
        dataAppend[2] = '123'.bytes

        def aGroup = new AGroup('append', dataAppend, null)
        aGroup.byPassGetSet = inMemoryGetSet
        aGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 1))

        when:
        aGroup.append()

        then:
        aGroup.get('a'.bytes) == '123'.bytes

        when:
        dataAppend[2] = '456'.bytes
        aGroup.append()

        then:
        aGroup.get('a'.bytes) == '123456'.bytes
    }
}
