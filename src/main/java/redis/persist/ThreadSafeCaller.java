package redis.persist;

import io.activej.async.callback.AsyncComputation;
import io.activej.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.ConfForSlot;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

public abstract class ThreadSafeCaller {
    protected Eventloop eventloop;
    protected long threadId;


    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    abstract ThreadFactory getNextThreadFactory();

    abstract String threadName();

    public void initEventloop() {
        initEventloop(null);
    }

    public void initEventloop(ThreadFactory threadFactoryGiven) {
        this.eventloop = Eventloop.builder()
                .withThreadName(threadName())
                .withIdleInterval(Duration.ofMillis(ConfForSlot.global.eventLoopIdleMillis))
                .build();
        this.eventloop.keepAlive(true);

        var threadFactory = threadFactoryGiven == null ? getNextThreadFactory() : threadFactoryGiven;
        var thread = threadFactory.newThread(this.eventloop);
        thread.start();
        log.info("Init eventloop, thread name: {}", threadName());

        this.threadId = thread.threadId();
    }

    protected void stopEventLoop() {
        if (this.eventloop != null) {
            this.eventloop.breakEventloop();
            System.out.println("Eventloop stopped, thread name: " + threadName());

            this.eventloop = null;
        }
    }

    protected <T> CompletableFuture<T> call(Callable<T> callable) {
        var currentThreadId = Thread.currentThread().getId();
        if (currentThreadId == threadId) {
            CompletableFuture<T> future = new CompletableFuture<>();
            try {
                future.complete(callable.call());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future;
        } else {
            return eventloop.submit(AsyncComputation.of(callable::call));
        }
    }

    protected <T> T callSync(Callable<T> callable) {
        try {
            return call(callable).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
