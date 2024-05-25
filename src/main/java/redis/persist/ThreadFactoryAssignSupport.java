package redis.persist;

import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;

import java.util.concurrent.ThreadFactory;

public class ThreadFactoryAssignSupport {
    // singleton
    private static final ThreadFactoryAssignSupport instance = new ThreadFactoryAssignSupport();

    public static ThreadFactoryAssignSupport getInstance() {
        return instance;
    }

    // one or two ssd volume, one cpu v-core is enough, suppose there are at most 8 ssd volumes
    public Inner ForFdReadWrite = new Inner(8, 16, "fd-read-write-group-", true);

    public Inner ForKeyLoader = new Inner(16, 4, "key-loader-", true);

    public Inner ForSlotWalBatchPersist = new Inner(4, 8, "slot-wal-persist-", true);

    public Inner ForChunkMerge = new Inner(4, 8, "chunk-merge-", true);

    class Inner {
        private final ThreadFactory[] threadFactories;

        private final int threadNumberPerGroup;

        public Inner(int number, int threadNumberPerGroup, String threadGroupPrefix, boolean isThreadGroupUseSameCore) {
            this.threadFactories = new ThreadFactory[number];
            this.threadNumberPerGroup = threadNumberPerGroup;
            for (int i = 0; i < number; i++) {
                this.threadFactories[i] = new AffinityThreadFactory(threadGroupPrefix + i,
                        isThreadGroupUseSameCore ? AffinityStrategies.SAME_CORE : AffinityStrategies.DIFFERENT_CORE);
            }
        }

        private int count = 0;

        public ThreadFactory getNextThreadFactory() {
            count++;

            for (int i = 0; i < threadFactories.length; i++) {
                if (count <= threadNumberPerGroup * (i + 1)) {
                    return threadFactories[i];
                }
            }

            count = 1;
            return threadFactories[0];
        }
    }
}
