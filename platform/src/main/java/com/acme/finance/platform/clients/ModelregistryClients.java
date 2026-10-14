package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.modelregistry.v1.RegisteredModelCalculationServiceGrpc;
import com.acme.modelregistry.v1.RegisteredModelQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the modelregistry contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ModelregistryClients {
    @Bean
    public RegisteredModelCalculationServiceGrpc.RegisteredModelCalculationServiceBlockingStub modelregistryRegisteredModelCalculationServiceBlocking(ChannelRegistry channels) {
        return RegisteredModelCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.modelregistry.v1.RegisteredModelCalculationService"));
    }

    @Bean
    public RegisteredModelQueryServiceGrpc.RegisteredModelQueryServiceBlockingStub modelregistryRegisteredModelQueryServiceBlocking(ChannelRegistry channels) {
        return RegisteredModelQueryServiceGrpc.newBlockingStub(channels.lookup("acme.modelregistry.v1.RegisteredModelQueryService"));
    }
}
