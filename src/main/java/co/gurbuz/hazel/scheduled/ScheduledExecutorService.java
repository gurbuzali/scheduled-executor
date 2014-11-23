package co.gurbuz.hazel.scheduled;

import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.ExecutorServiceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO add a proper JavaDoc
 */
public class ScheduledExecutorService extends DistributedExecutorService {

    public static final String SERVICE_NAME = "grbz:scheduledExecutorService";

    NodeEngine nodeEngine;
    ExecutionService executionService;
    private ILogger logger;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.executionService = nodeEngine.getExecutionService();
        this.logger = nodeEngine.getLogger(ScheduledExecutorService.class);
    }

    @Override
    public ExecutorServiceProxy createDistributedObject(String name) {
        return new ScheduledExecutorProxy(name, nodeEngine, this);
    }

    public void schedule(String name, String uuid, Callable callable, ResponseHandler responseHandler, long delay, long period) {
//        startPending(name);
        CallableProcessor processor = new CallableProcessor(name, uuid, callable, responseHandler);
//        if (uuid != null) {
//            submittedTasks.put(uuid, processor);
//        }

        try {
            if (period != -1) {
                executionService.scheduleAtFixedRate(name, processor, delay, period, TimeUnit.MILLISECONDS);
            } else {
                executionService.schedule(name, processor, delay, TimeUnit.MILLISECONDS);
            }
        } catch (RejectedExecutionException e) {
//            rejectExecution(name);
            logger.warning("While executing " + callable + " on Executor[" + name + "]", e);
//            if (uuid != null) {
//                submittedTasks.remove(uuid);
//            }
            processor.sendResponse(e);
        }
    }

    private final class CallableProcessor extends FutureTask implements Runnable {

        private AtomicBoolean responseFlag = new AtomicBoolean();

        private final String name;
        private final String uuid;
        private final ResponseHandler responseHandler;
        private final String callableToString;
        private final long creationTime = Clock.currentTimeMillis();

        private CallableProcessor(String name, String uuid, Callable callable, ResponseHandler responseHandler) {
            //noinspection unchecked
            super(callable);
            this.name = name;
            this.uuid = uuid;
            this.callableToString = String.valueOf(callable);
            this.responseHandler = responseHandler;
        }

        @Override
        public void run() {
            long start = Clock.currentTimeMillis();
//            startExecution(name, start - creationTime);
            Object result = null;
            try {
                super.run();
                if (!isCancelled()) {
                    result = get();
                }
            } catch (Exception e) {
                logException(e);
                result = e;
            } finally {
//                if (uuid != null) {
//                    submittedTasks.remove(uuid);
//                }
                sendResponse(result);
//                if (!isCancelled()) {
//                    finishExecution(name, Clock.currentTimeMillis() - start);
//                }
            }
        }

        private void logException(Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("While executing callable: " + callableToString, e);
            }
        }

        private void sendResponse(Object result) {
            if (responseFlag.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
                responseHandler.sendResponse(result);
            }
        }
    }

}
