package redis.decode;

import redis.BaseCommand;

public class Request {
    private final byte[][] data;

    private final boolean isHttp;
    private final boolean isRepl;

    public byte[][] getData() {
        return data;
    }

    public boolean isHttp() {
        return isHttp;
    }

    public boolean isRepl() {
        return isRepl;
    }

    public Request(byte[][] data, boolean isHttp, boolean isRepl) {
        this.data = data;
        this.isHttp = isHttp;
        this.isRepl = isRepl;
    }

    private String cmd;

    private short slotNumber;

    public short getSlotNumber() {
        return slotNumber;
    }

    public void setSlotNumber(short slotNumber) {
        this.slotNumber = slotNumber;
    }

    // todo, need check if cross slot so can check if need use multi slot eventloop to execute task
    private BaseCommand.SlotWithKeyHash slotWithKeyHash;

    public BaseCommand.SlotWithKeyHash getSlotWithKeyHash() {
        return slotWithKeyHash;
    }

    public void setSlotWithKeyHash(BaseCommand.SlotWithKeyHash slotWithKeyHash) {
        this.slotWithKeyHash = slotWithKeyHash;
    }

    public byte getSlot() {
        if (isRepl) {
            // refer to Repl.decode
            return data[1][0];
        }

        if (slotWithKeyHash == null) {
            return -1;
        }
        return slotWithKeyHash.slot();
    }

    public String cmd() {
        if (cmd != null) {
            return cmd;
        }
        cmd = new String(data[0]).toLowerCase();
        return cmd;
    }

    @Override
    public String toString() {
        return "Request{" +
                "cmd=" + cmd() +
                "data.length=" + data.length +
                ", isHttp=" + isHttp +
                ", isRepl=" + isRepl +
                '}';
    }
}
