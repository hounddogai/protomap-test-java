package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.patientapp.v1.PatientHomeQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the patientapp contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PatientappClients {
    @Bean
    public PatientHomeQueryServiceGrpc.PatientHomeQueryServiceBlockingStub patientappPatientHomeQueryServiceBlocking(ChannelRegistry channels) {
        return PatientHomeQueryServiceGrpc.newBlockingStub(channels.lookup("acme.patientapp.v1.PatientHomeQueryService"));
    }
}
