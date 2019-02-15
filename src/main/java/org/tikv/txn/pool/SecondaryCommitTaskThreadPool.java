package org.tikv.txn.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 2PC: Secondary commit thread pool
 */
public final class SecondaryCommitTaskThreadPool implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(SecondaryCommitTaskThreadPool.class);

    private ExecutorService taskThreadsPool;

    public SecondaryCommitTaskThreadPool() {
        this.taskThreadsPool = Executors.newWorkStealingPool();
    }

    public String submitSecondaryTask(Runnable task) {
        try {
            this.taskThreadsPool.submit(task);
            return null;
        } catch (Exception e) {
            LOG.error("Submit secondary task failed");
            return String.format("Submit secondary task failed");
        }
    }
    @Override
    public void close() throws Exception {
        if(taskThreadsPool != null) {
            if (!taskThreadsPool.awaitTermination(20, TimeUnit.SECONDS)) {
                taskThreadsPool.shutdownNow(); // Cancel currently executing tasks
            } else {

            }
        }
    }
}
