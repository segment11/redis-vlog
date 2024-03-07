package redis;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(@NotNull Runnable r) {
        var t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }

    public NamedThreadFactory(String namePrefix) {
        this.group = Thread.currentThread().getThreadGroup();
        this.namePrefix = namePrefix + "-" + poolNumber.getAndIncrement() + "-thread-";
    }

    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private static final AtomicInteger threadNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final String namePrefix;
}
