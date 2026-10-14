package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.performers.v1.PerformerServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the performers contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PerformersClients {
    @Bean
    public PerformerServiceGrpc.PerformerServiceBlockingStub performersPerformerServiceBlocking(ChannelRegistry channels) {
        return PerformerServiceGrpc.newBlockingStub(channels.lookup("acme.performers.v1.PerformerService"));
    }
}
