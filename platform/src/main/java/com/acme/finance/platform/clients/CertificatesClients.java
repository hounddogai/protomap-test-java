package com.acme.finance.platform.clients;

import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CertificateQueryServiceGrpc;
import com.acme.certificates.v1.CertificateServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the certificates contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CertificatesClients {
    @Bean
    public CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking(ChannelRegistry channels) {
        return CertificateCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.certificates.v1.CertificateCalculationService"));
    }

    @Bean
    public CertificateQueryServiceGrpc.CertificateQueryServiceBlockingStub certificatesCertificateQueryServiceBlocking(ChannelRegistry channels) {
        return CertificateQueryServiceGrpc.newBlockingStub(channels.lookup("acme.certificates.v1.CertificateQueryService"));
    }

    @Bean
    public CertificateServiceGrpc.CertificateServiceBlockingStub certificatesCertificateServiceBlocking(ChannelRegistry channels) {
        return CertificateServiceGrpc.newBlockingStub(channels.lookup("acme.certificates.v1.CertificateService"));
    }
}
