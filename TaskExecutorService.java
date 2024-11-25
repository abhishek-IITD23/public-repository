package threadAssignment;

import java.util.*;
import java.util.concurrent.*;

/**
 * Implementation of TaskExecutor that enforces concurrency limits, preserves task order,
 * and prevents concurrent execution of tasks within the same TaskGroup.
 */
public class TaskExecutorService implements Main.TaskExecutor {
    private final ExecutorService executorService;
    private final Map<UUID, Semaphore> groupConcurrency = new ConcurrentHashMap<>();
    private final Queue<FutureTask<?>> taskQueue = new LinkedList<>();
    private final Object lock = new Object();

    public TaskExecutorService(int maxConcurrency) {
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
    }

    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        Objects.requireNonNull(task, "Task must not be null");

        // Create a FutureTask for the task
        FutureTask<T> futureTask = new FutureTask<>(() -> executeTask(task));

        // Synchronize to add to the queue while preserving order
        synchronized (lock) {
            taskQueue.add(futureTask);
        }

        // Execute the task asynchronously
        executorService.submit(() -> {
            synchronized (lock) {
                FutureTask<?> nextTask = taskQueue.poll();
                if (nextTask != null) {
                    nextTask.run();
                }
            }
        });

        return futureTask;
    }

    private <T> T executeTask(Main.Task<T> task) throws Exception {
        // Acquire lock for the task's group to enforce single task execution per group
        Semaphore groupLock = groupConcurrency.get(task.taskGroup().groupUUID());
        if (groupLock == null) {
            synchronized (groupConcurrency) {
                groupLock = groupConcurrency.get(task.taskGroup().groupUUID());
                if (groupLock == null) {
                    groupLock = new Semaphore(1);
                    groupConcurrency.put(task.taskGroup().groupUUID(), groupLock);
                }
            }
        }
        groupLock.acquire();

        try {
            // Perform the task action
            return task.taskAction().call();
        } finally {
            groupLock.release();
            synchronized (groupConcurrency) {
                Semaphore semaphore = groupConcurrency.get(task.taskGroup().groupUUID());
                if (semaphore != null && !semaphore.hasQueuedThreads()) {
                    groupConcurrency.remove(task.taskGroup().groupUUID());
                }
            }
        }
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
