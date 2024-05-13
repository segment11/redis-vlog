package redis

import io.activej.config.Config
import org.jetbrains.annotations.Nullable
import spock.lang.Specification

class ConfVolumeDirsForSlotTest extends Specification {
    def "init"() {
        given:
        def persistConfig = new Config() {
            private final Map<String, String> inner = [:]

            void add(String key, String value) {
                inner.put(key, value)
            }

            @Override
            String getValue(@Nullable String defaultValue) {
                inner[defaultValue]
            }

            @Override
            Map<String, Config> getChildren() {
                [:]
            }
        }

        new File('/tmp/data0').mkdir()
        new File('/tmp/data1').mkdir()
        new File('/tmp/data2').mkdir()
        new File('/tmp/data3').mkdir()

        when:
        persistConfig.add('volumeDirsBySlot', '/tmp/data0:0-31,/tmp/data1:32-63,/tmp/data2:64-95,/tmp/data3:96-127')
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, (short) 128)

        then:
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 0) == '/tmp/data0'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 31) == '/tmp/data0'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 32) == '/tmp/data1'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 63) == '/tmp/data1'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 64) == '/tmp/data2'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 95) == '/tmp/data2'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 96) == '/tmp/data3'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 127) == '/tmp/data3'
    }
}
