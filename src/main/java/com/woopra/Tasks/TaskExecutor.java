package com.woopra.Tasks;

import com.woopra.MapReduce;
import com.woopra.Tasks.Task;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author : anudeep on 2/16/18
 * @project : MapReduce
 */
public class TaskExecutor {

    ExecutorService thread_pool;
    final static Logger log = Logger.getLogger(TaskExecutor.class);

    /**
     * execute- Executes Thread pool and shutdowns once done.
     *
     * @param tasks
     * @return - True/False
     */
    public boolean execute(List<Task> tasks) {
        List<Future<Integer>> futures = createPool(tasks);
        if (futures == null) {
            return false;
        }
        if (thread_pool != null) {
            shutdown();
        }
        return true;
    }

    /**
     * CreatePool - Creates thread pool with specific noof threads.
     *
     * @param tasks
     * @return - Future Tasks
     */
    public List<Future<Integer>> createPool(List<Task> tasks) {
        List<Future<Integer>> futures = null;
        try {
            thread_pool = Executors.newFixedThreadPool(tasks.size());
            futures = thread_pool.invokeAll(tasks);
            shutdown();
        } catch (InterruptedException ex) {
            log.error(ex.getMessage());
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return futures;
    }

    /**
     * Shutdown- Shutdowns Thread pool.
     */
    public void shutdown() {
        thread_pool.shutdown();
    }
}
