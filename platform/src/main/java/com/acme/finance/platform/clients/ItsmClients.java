package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.itsm.v1.IncidentCalculationServiceGrpc;
import com.acme.itsm.v1.IncidentServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the itsm contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ItsmClients {
    @Bean
    public IncidentCalculationServiceGrpc.IncidentCalculationServiceBlockingStub itsmIncidentCalculationServiceBlocking(ChannelRegistry channels) {
        return IncidentCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.itsm.v1.IncidentCalculationService"));
    }

    @Bean
    public IncidentServiceGrpc.IncidentServiceBlockingStub itsmIncidentServiceBlocking(ChannelRegistry channels) {
        return IncidentServiceGrpc.newBlockingStub(channels.lookup("acme.itsm.v1.IncidentService"));
    }
}
