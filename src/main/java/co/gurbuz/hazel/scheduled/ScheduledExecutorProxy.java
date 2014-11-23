package co.gurbuz.hazel.scheduled;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.executor.impl.ExecutorServiceProxy;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.UuidUtil.buildRandomUuidString;

/**
 * TODO add a proper JavaDoc
 */
public class ScheduledExecutorProxy extends ExecutorServiceProxy implements IScheduledExecutorService {

    private final Random random = new Random(-System.currentTimeMillis());

    private final int partitionCount;

    public ScheduledExecutorProxy(String name, NodeEngine nodeEngine, ScheduledExecutorService service) {
        super(name, nodeEngine, service);
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        RunnableAdapter<Object> callable = createRunnableAdapter(command);
        return schedule(callable, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        final int partitionId = getTaskPartitionId(callable);
        return submitToPartitionOwner(callable, partitionId, unit.toMillis(delay), -1);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return null;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return null;
    }

    private <T> RunnableAdapter<T> createRunnableAdapter(Runnable command) {
        if (command == null) {
            throw new NullPointerException();
        }
        return new RunnableAdapter<T>(command);
    }

    private <T> ScheduledFuture<T> submitToPartitionOwner(Callable<T> task, int partitionId, long delay, long period) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }
        if (isShutdown()) {
            throw new RejectedExecutionException(getRejectionMessage());
        }
        NodeEngine nodeEngine = getNodeEngine();
        Data taskData = nodeEngine.toData(task);
        String uuid = buildRandomUuidString();
        String name = getName();
        ScheduledCallableTaskOperation op = new ScheduledCallableTaskOperation(name, uuid, taskData, delay, period);
        ICompletableFuture future = invoke(partitionId, op);
        return new ScheduledDelegatingFuture<T>(future, nodeEngine.getSerializationService(), delay);
//        return new CancellableDelegatingFuture<T>(future, nodeEngine, uuid, partitionId);
    }

    private <T> int getTaskPartitionId(Callable<T> task) {
        int partitionId;
        if (task instanceof PartitionAware) {
            final Object partitionKey = ((PartitionAware) task).getPartitionKey();
            partitionId = getNodeEngine().getPartitionService().getPartitionId(partitionKey);
        } else {
            partitionId = random.nextInt(partitionCount);
        }
        return partitionId;
    }

    private String getRejectionMessage() {
        String name = getName();
        return "ExecutorService[" + name + "] is shutdown! In order to create a new ExecutorService with name '" + name
                + "', you need to destroy current ExecutorService first!";
    }

    private InternalCompletableFuture invoke(int partitionId, ScheduledCallableTaskOperation op) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.invokeOnPartition(ScheduledExecutorService.SERVICE_NAME, op, partitionId);
    }

    private ScheduledExecutorService service() {
        return (ScheduledExecutorService) getService();
    }

}
