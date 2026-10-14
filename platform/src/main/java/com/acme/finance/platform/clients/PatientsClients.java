package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.patients.v1.PatientQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the patients contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PatientsClients {
    @Bean
    public PatientQueryServiceGrpc.PatientQueryServiceBlockingStub patientsPatientQueryServiceBlocking(ChannelRegistry channels) {
        return PatientQueryServiceGrpc.newBlockingStub(channels.lookup("acme.patients.v1.PatientQueryService"));
    }
}
