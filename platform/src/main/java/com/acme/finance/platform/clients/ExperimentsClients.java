package com.acme.finance.platform.clients;

import com.acme.experiments.v1.ExperimentCalculationServiceGrpc;
import com.acme.experiments.v1.ExperimentQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the experiments contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ExperimentsClients {
    @Bean
    public ExperimentCalculationServiceGrpc.ExperimentCalculationServiceBlockingStub experimentsExperimentCalculationServiceBlocking(ChannelRegistry channels) {
        return ExperimentCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.experiments.v1.ExperimentCalculationService"));
    }

    @Bean
    public ExperimentQueryServiceGrpc.ExperimentQueryServiceBlockingStub experimentsExperimentQueryServiceBlocking(ChannelRegistry channels) {
        return ExperimentQueryServiceGrpc.newBlockingStub(channels.lookup("acme.experiments.v1.ExperimentQueryService"));
    }
}
