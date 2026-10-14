package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.yards.v1.YardCalculationServiceGrpc;
import com.acme.yards.v1.YardQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the yards contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class YardsClients {
    @Bean
    public YardCalculationServiceGrpc.YardCalculationServiceBlockingStub yardsYardCalculationServiceBlocking(ChannelRegistry channels) {
        return YardCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.yards.v1.YardCalculationService"));
    }

    @Bean
    public YardQueryServiceGrpc.YardQueryServiceBlockingStub yardsYardQueryServiceBlocking(ChannelRegistry channels) {
        return YardQueryServiceGrpc.newBlockingStub(channels.lookup("acme.yards.v1.YardQueryService"));
    }
}
