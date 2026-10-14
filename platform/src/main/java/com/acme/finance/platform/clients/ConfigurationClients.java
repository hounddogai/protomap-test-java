package com.acme.finance.platform.clients;

import com.acme.configuration.v1.ConfigSetCalculationServiceGrpc;
import com.acme.configuration.v1.ConfigSetQueryServiceGrpc;
import com.acme.configuration.v1.ConfigSetServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the configuration contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ConfigurationClients {
    @Bean
    public ConfigSetCalculationServiceGrpc.ConfigSetCalculationServiceBlockingStub configurationConfigSetCalculationServiceBlocking(ChannelRegistry channels) {
        return ConfigSetCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.configuration.v1.ConfigSetCalculationService"));
    }

    @Bean
    public ConfigSetQueryServiceGrpc.ConfigSetQueryServiceBlockingStub configurationConfigSetQueryServiceBlocking(ChannelRegistry channels) {
        return ConfigSetQueryServiceGrpc.newBlockingStub(channels.lookup("acme.configuration.v1.ConfigSetQueryService"));
    }

    @Bean
    public ConfigSetServiceGrpc.ConfigSetServiceBlockingStub configurationConfigSetServiceBlocking(ChannelRegistry channels) {
        return ConfigSetServiceGrpc.newBlockingStub(channels.lookup("acme.configuration.v1.ConfigSetService"));
    }
}
