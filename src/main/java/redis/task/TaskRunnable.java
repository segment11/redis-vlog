package redis.task;

import io.activej.eventloop.Eventloop;
import redis.RequestHandler;
import redis.persist.OneSlot;

import java.util.ArrayList;

public class TaskRunnable implements Runnable {
    private final byte netWorkerId;
    private final byte netWorkers;

    public TaskRunnable(byte netWorkerId, byte netWorkers) {
        this.netWorkerId = netWorkerId;
        this.netWorkers = netWorkers;
    }

    private final ArrayList<OneSlot> oneSlots = new ArrayList<>();

    public void chargeOneSlots(OneSlot[] oneSlots) {
        for (var oneSlot : oneSlots) {
            if (oneSlot.slot() % netWorkers == netWorkerId) {
                this.oneSlots.add(oneSlot);

                oneSlot.setNetWorkerEventloop(netWorkerEventloop);
                oneSlot.setRequestHandler(requestHandler);
            }
        }
    }

    private Eventloop netWorkerEventloop;

    public void setNetWorkerEventloop(Eventloop netWorkerEventloop) {
        this.netWorkerEventloop = netWorkerEventloop;
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

        netWorkerEventloop.delay(1000L, this);
    }

    private boolean isStopped = false;

    public void stop() {
        isStopped = true;
        System.out.println("Task delay stopped for net worker eventloop, net worker id: " + netWorkerId);
    }
}
