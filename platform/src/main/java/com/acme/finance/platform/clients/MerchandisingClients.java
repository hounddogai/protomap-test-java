package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.merchandising.v1.CollectionQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the merchandising contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class MerchandisingClients {
    @Bean
    public CollectionQueryServiceGrpc.CollectionQueryServiceStub merchandisingCollectionQueryServiceAsync(ChannelRegistry channels) {
        return CollectionQueryServiceGrpc.newStub(channels.lookup("acme.merchandising.v1.CollectionQueryService"));
    }
}
