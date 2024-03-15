package redis.task;

import io.activej.eventloop.Eventloop;
import redis.RequestHandler;
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

                oneSlot.setEventloop(eventloop);
                oneSlot.setRequestHandler(requestHandler);
            }
        }
    }

    private Eventloop eventloop;

    public void setEventloop(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    private RequestHandler requestHandler;

    public void setRequestHandler(RequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    private int loopCount = 0;

    @Override
    public void run() {
        for (var oneSlot : oneSlots) {
            oneSlot.doTask(loopCount);
        }
        loopCount++;

        if (isStopped) {
            return;
        }

        eventloop.delay(1000L, this);
    }

    private boolean isStopped = false;

    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped. index: " + i);
    }
}
