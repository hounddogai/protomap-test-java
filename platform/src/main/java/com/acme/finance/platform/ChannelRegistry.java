package com.acme.finance.platform;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Resolves service names through the mesh; all channels are closed with the application context. */
@Component
public final class ChannelRegistry implements DisposableBean {
    private final Map<String, Channel> channels = new ConcurrentHashMap<>();
    private final Channel mesh;

    @Autowired
    public ChannelRegistry(@Value("${acme.mesh.target:localhost:15001}") String target) {
        this(ManagedChannelBuilder.forTarget(target).usePlaintext().build());
    }

    private ChannelRegistry(Channel mesh) {
        this.mesh = mesh;
    }

    public static ChannelRegistry fixed(Channel channel) {
        return new ChannelRegistry(channel);
    }

    public Channel lookup(String service) {
        return channels.computeIfAbsent(service, ignored -> ClientInterceptors.intercept(mesh, CallIdentity.forwarding()));
    }

    @Override
    public void destroy() throws InterruptedException {
        if (mesh instanceof ManagedChannel managed) {
            managed.shutdown();
            if (!managed.awaitTermination(5, TimeUnit.SECONDS)) {
                managed.shutdownNow();
            }
        }
    }
}
