package co.gurbuz.hazel.scheduled;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.RunnableAdapter;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TraceableOperation;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * TODO add a proper JavaDoc
 */
public class ScheduledCallableTaskOperation extends Operation implements TraceableOperation {

    protected String name;
    protected String uuid;
    protected transient Callable callable;
    private Data callableData;
    private long delay;
    private long period;

    public ScheduledCallableTaskOperation() {
    }

    public ScheduledCallableTaskOperation(String name, String uuid, Data callableData, long delay, long period) {
        this.name = name;
        this.uuid = uuid;
        this.callableData = callableData;
        this.delay = delay;
        this.period = period;
    }

    @Override
    public final void beforeRun() throws Exception {
        callable = getCallable();
        ManagedContext managedContext = getManagedContext();

        if (callable instanceof RunnableAdapter) {
            RunnableAdapter adapter = (RunnableAdapter) callable;
            Runnable runnable = (Runnable) managedContext.initialize(adapter.getRunnable());
            adapter.setRunnable(runnable);
        } else {
            callable = (Callable) managedContext.initialize(callable);
        }
    }

    /**
     * since this operation handles responses in an async way, we need to handle serialization exceptions too
     *
     * @return
     */
    private Callable getCallable() {
        try {
            return getNodeEngine().toObject(callableData);
        } catch (HazelcastSerializationException e) {
            getResponseHandler().sendResponse(e);
            throw ExceptionUtil.rethrow(e);
        }
    }

    private ManagedContext getManagedContext() {
        HazelcastInstanceImpl hazelcastInstance = (HazelcastInstanceImpl) getNodeEngine().getHazelcastInstance();
        SerializationServiceImpl serializationService =
                (SerializationServiceImpl) hazelcastInstance.getSerializationService();
        return serializationService.getManagedContext();
    }

    @Override
    public final void run() throws Exception {
        ScheduledExecutorService service = getService();
        service.schedule(name, uuid, callable, getResponseHandler(), delay, period);
    }

    @Override
    public final void afterRun() throws Exception {
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        return null;
    }

    @Override
    public Object getTraceIdentifier() {
        return uuid;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(uuid);
        out.writeData(callableData);
        out.writeLong(delay);
        out.writeLong(period);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        uuid = in.readUTF();
        callableData = in.readData();
        delay = in.readLong();
        period = in.readLong();
    }


}
