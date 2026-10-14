package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.licensing.v1.SoftwareLicenseCalculationServiceGrpc;
import com.acme.licensing.v1.SoftwareLicenseQueryServiceGrpc;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the licensing contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LicensingClients {
    @Bean
    public SoftwareLicenseCalculationServiceGrpc.SoftwareLicenseCalculationServiceBlockingStub licensingSoftwareLicenseCalculationServiceBlocking(ChannelRegistry channels) {
        return SoftwareLicenseCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.licensing.v1.SoftwareLicenseCalculationService"));
    }

    @Bean
    public SoftwareLicenseQueryServiceGrpc.SoftwareLicenseQueryServiceBlockingStub licensingSoftwareLicenseQueryServiceBlocking(ChannelRegistry channels) {
        return SoftwareLicenseQueryServiceGrpc.newBlockingStub(channels.lookup("acme.licensing.v1.SoftwareLicenseQueryService"));
    }

    @Bean
    public SoftwareLicenseServiceGrpc.SoftwareLicenseServiceBlockingStub licensingSoftwareLicenseServiceBlocking(ChannelRegistry channels) {
        return SoftwareLicenseServiceGrpc.newBlockingStub(channels.lookup("acme.licensing.v1.SoftwareLicenseService"));
    }
}
