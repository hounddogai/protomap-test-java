package com.acme.finance.platform.clients;

import com.acme.concessions.v1.ConcessionStandCalculationServiceGrpc;
import com.acme.concessions.v1.ConcessionStandQueryServiceGrpc;
import com.acme.concessions.v1.ConcessionStandServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the concessions contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ConcessionsClients {
    @Bean
    public ConcessionStandCalculationServiceGrpc.ConcessionStandCalculationServiceBlockingStub concessionsConcessionStandCalculationServiceBlocking(ChannelRegistry channels) {
        return ConcessionStandCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.concessions.v1.ConcessionStandCalculationService"));
    }

    @Bean
    public ConcessionStandQueryServiceGrpc.ConcessionStandQueryServiceBlockingStub concessionsConcessionStandQueryServiceBlocking(ChannelRegistry channels) {
        return ConcessionStandQueryServiceGrpc.newBlockingStub(channels.lookup("acme.concessions.v1.ConcessionStandQueryService"));
    }

    @Bean
    public ConcessionStandServiceGrpc.ConcessionStandServiceBlockingStub concessionsConcessionStandServiceBlocking(ChannelRegistry channels) {
        return ConcessionStandServiceGrpc.newBlockingStub(channels.lookup("acme.concessions.v1.ConcessionStandService"));
    }
}
