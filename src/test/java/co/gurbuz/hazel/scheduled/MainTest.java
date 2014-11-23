package co.gurbuz.hazel.scheduled;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * TODO add a proper JavaDoc
 */
public class MainTest {

    static {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);
    }

    public static void main(String[] args) throws Exception {
        final ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true);
        serviceConfig.setClassName(ScheduledExecutorService.class.getName());
        serviceConfig.setName(ScheduledExecutorService.SERVICE_NAME);
        final Config config = new Config();
        final ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(serviceConfig);
        SerializationConfig memberSerializationConfig = config.getSerializationConfig();
//        PriorityPortableHook hook = new PriorityPortableHook();
//        memberSerializationConfig.addPortableFactory(PriorityPortableHook.F_ID, hook.createFactory());
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        IScheduledExecutorService scheduled = instance.getDistributedObject(ScheduledExecutorService.SERVICE_NAME, "foo");
        ScheduledFuture future = scheduled.scheduleAtFixedRate(new MyCallable(), 5, 5, TimeUnit.SECONDS);

        Thread.sleep(23000);

//        ClientConfig clientConfig = new ClientConfig();
//        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
////        proxyFactoryConfig.setClassName(ProxyFactory.class.getName());
////        proxyFactoryConfig.setService(ScheduledExecutorService.SERVICE_NAME);
//        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);
//        SerializationConfig clientSerializationConfig = clientConfig.getSerializationConfig();
////        clientSerializationConfig.addPortableFactory(PriorityPortableHook.F_ID, hook.createFactory());
//        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
//        IQueue clientQ = client.getDistributedObject(ScheduledExecutorService.SERVICE_NAME, "foo");
//        clientQ.offer("veli");
//        clientQ.offer("ali");
//        Object ali = memberQ.poll();
//        Object veli = memberQ.poll();
//        System.err.println("ali: " + ali);
//        System.err.println("veli: " + veli);
    }

    static class MyCallable implements Runnable, Serializable {

        public MyCallable() {
        }

        @Override
        public void run() {
            System.err.println("fatal oldu!!!");
        }
    }
}
