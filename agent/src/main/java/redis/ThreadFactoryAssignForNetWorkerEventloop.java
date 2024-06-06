package redis;

import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;

import java.util.concurrent.ThreadFactory;

public class ThreadFactoryAssignForNetWorkerEventloop {
    // singleton
    private static final ThreadFactoryAssignForNetWorkerEventloop instance = new ThreadFactoryAssignForNetWorkerEventloop();

    public static ThreadFactoryAssignForNetWorkerEventloop getInstance() {
        return instance;
    }

    private ThreadFactory[] threadFactories;

    int threadNumberPerCpuCore;

    int count;

    void init(int threadNumberPerCpuCore, int cpuNumberLimit) {
        this.threadNumberPerCpuCore = threadNumberPerCpuCore;

        var cpuNumber = Runtime.getRuntime().availableProcessors();
        if (cpuNumberLimit > cpuNumber) {
            throw new RuntimeException("ThreadFactoryAssignForNetWorkerEventloop: cpu number limit is greater than available cpu number. cpu number limit: "
                    + cpuNumberLimit + ", available cpu number: " + cpuNumber);
        }

        this.threadFactories = new ThreadFactory[cpuNumberLimit];
        for (int i = 0; i < cpuNumberLimit; i++) {
            this.threadFactories[i] = new AffinityThreadFactory("net-worker-cpu-core-" + i, AffinityStrategies.SAME_CORE);
        }
    }

    ThreadFactory getThreadFactory() {
        var i = count / threadNumberPerCpuCore;
        if (i >= threadFactories.length) {
            throw new RuntimeException("ThreadFactoryAssignForNetWorkerEventloop: no more thread factories. cpu core number: " +
                    "" + threadFactories.length + ", thread number per cpu core: " + threadNumberPerCpuCore + ", count: " + count);
        }

        count++;
        return threadFactories[i];
    }
}
