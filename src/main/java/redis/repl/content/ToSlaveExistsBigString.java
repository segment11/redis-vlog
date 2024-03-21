package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import org.apache.commons.io.FileUtils;
import redis.repl.ReplContent;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class ToSlaveExistsBigString implements ReplContent {
    private final File bigStringDir;
    private final ArrayList<Long> toSendUuidList;
    private final boolean isSendAllOnce;

    private static final int ONCE_SEND_DICT_COUNT = 10;

    public ToSlaveExistsBigString(File bigStringDir, ArrayList<Long> uuidListServer, ArrayList<Long> sentUuidList) {
        this.bigStringDir = bigStringDir;

        var toSendUuidList = new ArrayList<Long>();
        // exclude sent uuid
        for (var uuid : uuidListServer) {
            if (!sentUuidList.contains(uuid)) {
                toSendUuidList.add(uuid);
            }
        }

        this.isSendAllOnce = toSendUuidList.size() <= ONCE_SEND_DICT_COUNT;
        if (!isSendAllOnce) {
            toSendUuidList = new ArrayList<>(toSendUuidList.subList(0, ONCE_SEND_DICT_COUNT));
        }

        this.toSendUuidList = toSendUuidList;
    }

    // 2 bytes as short for send big string count, 1 byte as flag for is sent all
    private static final int HEADER_LENGTH = 2 + 1;

    @Override
    public void encodeTo(ByteBuf toBuf) {
        if (toSendUuidList.isEmpty()) {
            toBuf.writeShort((short) 0);
            toBuf.put((byte) 1);
            return;
        }

        toBuf.writeShort((short) toSendUuidList.size());
        toBuf.put((byte) (isSendAllOnce ? 1 : 0));

        short existCount = 0;
        for (var uuid : toSendUuidList) {
            var file = new File(bigStringDir, String.valueOf(uuid));
            if (!file.exists()) {
                continue;
            }

            existCount++;

            toBuf.writeLong(uuid);
            var encodeLength = (int) file.length();
            toBuf.writeInt(encodeLength);

            try {
                byte[] bytes = FileUtils.readFileToByteArray(file);
                toBuf.put(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (existCount != toSendUuidList.size()) {
            toBuf.tail(0);
            toBuf.writeShort(existCount);
        }
    }

    @Override
    public int encodeLength() {
        if (toSendUuidList.isEmpty()) {
            return HEADER_LENGTH;
        }

        var length = HEADER_LENGTH;
        for (var uuid : toSendUuidList) {
            var file = new File(bigStringDir, String.valueOf(uuid));
            if (!file.exists()) {
                continue;
            }

            var encodeLength = (int) file.length();
            length += 8 + 4 + encodeLength;
        }
        return length;
    }
}
