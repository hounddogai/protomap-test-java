package com.acme.finance.platform.clients;

import com.acme.dispatch.v1.DispatchOrderQueryServiceGrpc;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the dispatch contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DispatchClients {
    @Bean
    public DispatchOrderQueryServiceGrpc.DispatchOrderQueryServiceStub dispatchDispatchOrderQueryServiceAsync(ChannelRegistry channels) {
        return DispatchOrderQueryServiceGrpc.newStub(channels.lookup("acme.dispatch.v1.DispatchOrderQueryService"));
    }

    @Bean
    public DispatchOrderServiceGrpc.DispatchOrderServiceBlockingStub dispatchDispatchOrderServiceBlocking(ChannelRegistry channels) {
        return DispatchOrderServiceGrpc.newBlockingStub(channels.lookup("acme.dispatch.v1.DispatchOrderService"));
    }
}
