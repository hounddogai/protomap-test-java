package com.acme.finance.platform.clients;

import com.acme.catalog.v1.ProductCalculationServiceGrpc;
import com.acme.catalog.v1.ProductQueryServiceGrpc;
import com.acme.catalog.v1.ProductServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the catalog contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CatalogClients {
    @Bean
    public ProductCalculationServiceGrpc.ProductCalculationServiceFutureStub catalogProductCalculationServiceFuture(ChannelRegistry channels) {
        return ProductCalculationServiceGrpc.newFutureStub(channels.lookup("acme.catalog.v1.ProductCalculationService"));
    }

    @Bean
    public ProductQueryServiceGrpc.ProductQueryServiceFutureStub catalogProductQueryServiceFuture(ChannelRegistry channels) {
        return ProductQueryServiceGrpc.newFutureStub(channels.lookup("acme.catalog.v1.ProductQueryService"));
    }

    @Bean
    public ProductServiceGrpc.ProductServiceBlockingStub catalogProductServiceBlocking(ChannelRegistry channels) {
        return ProductServiceGrpc.newBlockingStub(channels.lookup("acme.catalog.v1.ProductService"));
    }
}
