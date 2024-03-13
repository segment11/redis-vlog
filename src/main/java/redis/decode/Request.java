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

    private BaseCommand.SlotWithKeyHash slotWithKeyHash;

    public BaseCommand.SlotWithKeyHash getSlotWithKeyHash() {
        return slotWithKeyHash;
    }

    public void setSlotWithKeyHash(BaseCommand.SlotWithKeyHash slotWithKeyHash) {
        this.slotWithKeyHash = slotWithKeyHash;
    }

    public int getSlot() {
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
}
