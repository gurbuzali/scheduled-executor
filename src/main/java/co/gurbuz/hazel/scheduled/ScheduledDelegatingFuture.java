package co.gurbuz.hazel.scheduled;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * TODO add a proper JavaDoc
 */
public class ScheduledDelegatingFuture<V> extends DelegatingFuture<V> implements ScheduledFuture<V> {

    final long delay;

    public ScheduledDelegatingFuture(ICompletableFuture future, SerializationService serializationService, long delay) {
        super(future, serializationService);
        this.delay = delay;
    }

    public ScheduledDelegatingFuture(ICompletableFuture future, SerializationService serializationService, V defaultValue, long delay) {
        super(future, serializationService, defaultValue);
        this.delay = delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return delay;
    }

    @Override
    public int compareTo(Delayed o) {
        long otherDelay = o.getDelay(TimeUnit.MILLISECONDS);
        return Long.compare(delay, otherDelay);
    }
}
