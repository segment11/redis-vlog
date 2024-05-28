package redis;

import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;

import java.util.concurrent.ThreadFactory;

public class ThreadFactoryAssignSupport {
    // singleton
    private static final ThreadFactoryAssignSupport instance = new ThreadFactoryAssignSupport();

    public static ThreadFactoryAssignSupport getInstance() {
        return instance;
    }

    public Inner ForMultiSlotRequest = new Inner(2, 16, "multi-slot-request-", true);

    // one or two ssd volume, one cpu v-core is enough, suppose there are at most 8 ssd volumes
    public Inner ForFdReadWrite = new Inner(8, 16, "fd-read-write-group-", true);

    public Inner ForSlotWalBatchPersist = new Inner(4, 8, "slot-wal-persist-", true);

    public Inner ForChunkMerge = new Inner(4, 8, "chunk-merge-", true);

    public class Inner {
        final int number;
        final int threadNumberPerGroup;
        private final ThreadFactory[] threadFactories;

        public Inner(int number, int threadNumberPerGroup, String threadGroupPrefix, boolean isThreadGroupUseSameCore) {
            this.number = number;
            this.threadNumberPerGroup = threadNumberPerGroup;
            this.threadFactories = new ThreadFactory[number];
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
