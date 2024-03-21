package redis.persist;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    String getMasterUuid() {
        var obj = get("masterUuid");
        return obj == null ? null : obj.toString();
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

    int clearExpiredPvmWhenKeyBucketReadTimes() {
        var obj = get("clearExpiredPvmWhenKeyBucketReadTimes");
        return obj == null ? 10 : (int) obj;
    }

    void setClearExpiredPvmWhenKeyBucketReadTimes(int clearExpiredPvmWhenKeyBucketReadTimes) throws IOException {
        update("clearExpiredPvmWhenKeyBucketReadTimes", clearExpiredPvmWhenKeyBucketReadTimes);
    }

    DynConfig(byte slot, File dynConfigFile) throws IOException {
        this.slot = slot;
        this.dynConfigFile = dynConfigFile;
        // read json
        var objectMapper = new ObjectMapper();
        data = objectMapper.readValue(dynConfigFile, HashMap.class);

        log.info("Init dyn config, data: {}, slot: {}", data, slot);
    }

    private synchronized void update(String key, Object value) throws IOException {
        data.put(key, value);
        // write json
        var objectMapper = new ObjectMapper();
        objectMapper.writeValue(dynConfigFile, data);

        log.info("Update dyn config, key: {}, value: {}, slot: {}", key, value, slot);
    }
}
