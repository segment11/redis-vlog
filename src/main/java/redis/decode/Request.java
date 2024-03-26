package redis.decode;

import redis.BaseCommand;

import java.util.ArrayList;

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

    private boolean isCrossRequestWorker;
    private ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList;

    public boolean isCrossRequestWorker() {
        return isCrossRequestWorker;
    }

    public void setCrossRequestWorker(boolean crossRequestWorker) {
        isCrossRequestWorker = crossRequestWorker;
    }

    public ArrayList<BaseCommand.SlotWithKeyHash> getSlotWithKeyHashList() {
        return slotWithKeyHashList;
    }

    public void setSlotWithKeyHashList(ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList) {
        this.slotWithKeyHashList = slotWithKeyHashList;
    }

    public byte getSingleSlot() {
        if (isRepl) {
            // refer to Repl.decode
            return data[1][0];
        }

        if (slotWithKeyHashList == null) {
            return -1;
        }
        var first = slotWithKeyHashList.getFirst();
        if (first == null) {
            return -1;
        }
        return first.slot();
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
