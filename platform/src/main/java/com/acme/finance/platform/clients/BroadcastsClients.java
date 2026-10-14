package com.acme.finance.platform.clients;

import com.acme.broadcasts.v1.BroadcastQueryServiceGrpc;
import com.acme.broadcasts.v1.BroadcastServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the broadcasts contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class BroadcastsClients {
    @Bean
    public BroadcastQueryServiceGrpc.BroadcastQueryServiceBlockingStub broadcastsBroadcastQueryServiceBlocking(ChannelRegistry channels) {
        return BroadcastQueryServiceGrpc.newBlockingStub(channels.lookup("acme.broadcasts.v1.BroadcastQueryService"));
    }

    @Bean
    public BroadcastServiceGrpc.BroadcastServiceBlockingStub broadcastsBroadcastServiceBlocking(ChannelRegistry channels) {
        return BroadcastServiceGrpc.newBlockingStub(channels.lookup("acme.broadcasts.v1.BroadcastService"));
    }
}
