package redis;

import io.activej.eventloop.Eventloop;
import io.activej.service.adapter.ServiceAdapter;
import io.activej.service.adapter.ServiceAdapters;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;

public class MyInterceptor {
    private static ThreadFactoryAssignForNetWorkerEventloop assign;

    static {
        assign = ThreadFactoryAssignForNetWorkerEventloop.getInstance();

        var threadNumberPerCpuCore = 2;
        var envValue = System.getenv("NET_WORKER_THREAD_NUMBER_PER_CPU_CORE");
        if (envValue != null) {
            threadNumberPerCpuCore = Integer.parseInt(envValue);
        }

        var cpuNumberLimit = 4;
        envValue = System.getenv("NET_WORKER_CPU_NUMBER_LIMIT");
        if (envValue != null) {
            cpuNumberLimit = Integer.parseInt(envValue);
        }

        assign.init(threadNumberPerCpuCore, cpuNumberLimit);
    }

    private static ThreadFactory innerThreadFactory = new InnerThreadFactory();

    private static class InnerThreadFactory implements ThreadFactory {
        private boolean isPrimary = true;

        @Override
        public Thread newThread(@NotNull Runnable r) {
            System.out.println("My agent intercepted the start eventloop thread and use affinity thread factory for net worker.");
            var tf = assign.getThreadFactory();
            var thread = tf.newThread(r);

            // primary eventloop thread use a single cpu core
            if (isPrimary) {
                assign.count = assign.threadNumberPerCpuCore;
                isPrimary = false;
            }
            return thread;
        }
    }

    public static ServiceAdapter<Eventloop> ForEventloopDelegation() {
        return ServiceAdapters.forEventloop(r -> {
            Thread thread = innerThreadFactory.newThread(r);
            thread.setName("eventloop: " + thread.getName());
            return thread;
        });
    }
}
