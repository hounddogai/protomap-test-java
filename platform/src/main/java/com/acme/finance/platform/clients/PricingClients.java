package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.pricing.v1.PriceListServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the pricing contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PricingClients {
    @Bean
    public PriceListServiceGrpc.PriceListServiceBlockingStub pricingPriceListServiceBlocking(ChannelRegistry channels) {
        return PriceListServiceGrpc.newBlockingStub(channels.lookup("acme.pricing.v1.PriceListService"));
    }
}
