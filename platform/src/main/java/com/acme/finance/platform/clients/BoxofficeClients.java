package com.acme.finance.platform.clients;

import com.acme.boxoffice.v1.BoxOfficeSaleCalculationServiceGrpc;
import com.acme.boxoffice.v1.BoxOfficeSaleQueryServiceGrpc;
import com.acme.boxoffice.v1.BoxOfficeSaleServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the boxoffice contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class BoxofficeClients {
    @Bean
    public BoxOfficeSaleCalculationServiceGrpc.BoxOfficeSaleCalculationServiceBlockingStub boxofficeBoxOfficeSaleCalculationServiceBlocking(ChannelRegistry channels) {
        return BoxOfficeSaleCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.boxoffice.v1.BoxOfficeSaleCalculationService"));
    }

    @Bean
    public BoxOfficeSaleQueryServiceGrpc.BoxOfficeSaleQueryServiceBlockingStub boxofficeBoxOfficeSaleQueryServiceBlocking(ChannelRegistry channels) {
        return BoxOfficeSaleQueryServiceGrpc.newBlockingStub(channels.lookup("acme.boxoffice.v1.BoxOfficeSaleQueryService"));
    }

    @Bean
    public BoxOfficeSaleServiceGrpc.BoxOfficeSaleServiceBlockingStub boxofficeBoxOfficeSaleServiceBlocking(ChannelRegistry channels) {
        return BoxOfficeSaleServiceGrpc.newBlockingStub(channels.lookup("acme.boxoffice.v1.BoxOfficeSaleService"));
    }
}
