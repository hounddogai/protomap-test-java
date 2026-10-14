package com.acme.finance.platform.clients;

import com.acme.corporateevents.v1.CorporateEventCalculationServiceGrpc;
import com.acme.corporateevents.v1.CorporateEventServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the corporateevents contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CorporateeventsClients {
    @Bean
    public CorporateEventCalculationServiceGrpc.CorporateEventCalculationServiceBlockingStub corporateeventsCorporateEventCalculationServiceBlocking(ChannelRegistry channels) {
        return CorporateEventCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.corporateevents.v1.CorporateEventCalculationService"));
    }

    @Bean
    public CorporateEventServiceGrpc.CorporateEventServiceStub corporateeventsCorporateEventServiceAsync(ChannelRegistry channels) {
        return CorporateEventServiceGrpc.newStub(channels.lookup("acme.corporateevents.v1.CorporateEventService"));
    }

    @Bean
    public CorporateEventServiceGrpc.CorporateEventServiceBlockingStub corporateeventsCorporateEventServiceBlocking(ChannelRegistry channels) {
        return CorporateEventServiceGrpc.newBlockingStub(channels.lookup("acme.corporateevents.v1.CorporateEventService"));
    }
}
