package com.acme.finance.platform.clients;

import com.acme.cart.v1.CartCalculationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the cart contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CartClients {
    @Bean
    public CartCalculationServiceGrpc.CartCalculationServiceStub cartCartCalculationServiceAsync(ChannelRegistry channels) {
        return CartCalculationServiceGrpc.newStub(channels.lookup("acme.cart.v1.CartCalculationService"));
    }

    @Bean
    public CartCalculationServiceGrpc.CartCalculationServiceBlockingStub cartCartCalculationServiceBlocking(ChannelRegistry channels) {
        return CartCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.cart.v1.CartCalculationService"));
    }
}
