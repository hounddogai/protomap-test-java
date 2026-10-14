package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.PrescriptionServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the prescriptions contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PrescriptionsClients {
    @Bean
    public PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking(ChannelRegistry channels) {
        return PrescriptionCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.prescriptions.v1.PrescriptionCalculationService"));
    }

    @Bean
    public PrescriptionServiceGrpc.PrescriptionServiceBlockingStub prescriptionsPrescriptionServiceBlocking(ChannelRegistry channels) {
        return PrescriptionServiceGrpc.newBlockingStub(channels.lookup("acme.prescriptions.v1.PrescriptionService"));
    }
}
