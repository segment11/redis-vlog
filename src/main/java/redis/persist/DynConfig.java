package redis.persist;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class DynConfig {
    private final Logger log = LoggerFactory.getLogger(DynConfig.class);
    private final byte slot;
    private final File dynConfigFile;

    private final HashMap<String, Object> data;

    private Object get(String key) {
        return data.get(key);
    }

    Long getMasterUuid() {
        Object val = get("masterUuid");
        return val == null ? null : Long.valueOf(val.toString());
    }

    void setMasterUuid(long masterUuid) throws IOException {
        update("masterUuid", masterUuid);
    }

    // add other config items get/set here

    boolean isReadonly() {
        var obj = get("readonly");
        return obj != null && (boolean) obj;
    }

    void setReadonly(boolean readonly) throws IOException {
        update("readonly", readonly);
    }

    boolean isCanRead() {
        var obj = get("canRead");
        return obj == null || (boolean) obj;
    }

    void setCanRead(boolean canRead) throws IOException {
        update("canRead", canRead);
    }

    int getTestKey() {
        var obj = get("testKey");
        return obj == null ? 10 : (int) obj;
    }

    void setTestKey(int testKey) throws IOException {
        update("testKey", testKey);
    }

    DynConfig(byte slot, File dynConfigFile) throws IOException {
        this.slot = slot;
        this.dynConfigFile = dynConfigFile;

        if (!dynConfigFile.exists()) {
            FileUtils.touch(dynConfigFile);
            FileUtils.writeByteArrayToFile(dynConfigFile, "{}".getBytes());

            this.data = new HashMap<>();
            log.info("Init dyn config, data: {}, slot: {}", data, slot);
        } else {
            // read json
            var objectMapper = new ObjectMapper();
            this.data = objectMapper.readValue(dynConfigFile, HashMap.class);
            log.info("Init dyn config, data: {}, slot: {}", data, slot);
        }
    }

    private synchronized void update(String key, Object value) throws IOException {
        data.put(key, value);
        // write json
        var objectMapper = new ObjectMapper();
        objectMapper.writeValue(dynConfigFile, data);

        log.info("Update dyn config, key: {}, value: {}, slot: {}", key, value, slot);
    }
}
