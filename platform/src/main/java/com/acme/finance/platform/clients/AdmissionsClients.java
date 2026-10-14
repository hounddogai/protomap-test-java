package com.acme.finance.platform.clients;

import com.acme.admissions.v1.AdmissionCalculationServiceGrpc;
import com.acme.admissions.v1.AdmissionServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the admissions contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AdmissionsClients {
    @Bean
    public AdmissionCalculationServiceGrpc.AdmissionCalculationServiceBlockingStub admissionsAdmissionCalculationServiceBlocking(ChannelRegistry channels) {
        return AdmissionCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.admissions.v1.AdmissionCalculationService"));
    }

    @Bean
    public AdmissionServiceGrpc.AdmissionServiceBlockingStub admissionsAdmissionServiceBlocking(ChannelRegistry channels) {
        return AdmissionServiceGrpc.newBlockingStub(channels.lookup("acme.admissions.v1.AdmissionService"));
    }

    @Bean
    public AdmissionServiceGrpc.AdmissionServiceFutureStub admissionsAdmissionServiceFuture(ChannelRegistry channels) {
        return AdmissionServiceGrpc.newFutureStub(channels.lookup("acme.admissions.v1.AdmissionService"));
    }
}
