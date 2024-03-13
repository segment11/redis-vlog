package redis.task;

import io.activej.eventloop.Eventloop;
import redis.persist.OneSlot;

import java.util.ArrayList;

public class TaskRunnable implements Runnable {
    private final byte i;
    private final byte requestWorkers;

    public TaskRunnable(byte i, byte requestWorkers) {
        this.i = i;
        this.requestWorkers = requestWorkers;
    }

    private final ArrayList<OneSlot> oneSlots = new ArrayList<>();

    public void chargeOneSlots(OneSlot[] oneSlots) {
        for (var oneSlot : oneSlots) {
            if (oneSlot.slot() % requestWorkers == i) {
                this.oneSlots.add(oneSlot);
            }
        }
    }

    private Eventloop eventloop;

    public void setEventloop(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    private int loopCount = 0;

    @Override
    public void run() {
        for (var oneSlot : oneSlots) {
            oneSlot.doTask(loopCount);
        }
        loopCount++;

        eventloop.delay(1000L, this);
    }
}
