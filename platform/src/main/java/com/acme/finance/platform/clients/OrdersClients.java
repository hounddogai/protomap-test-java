package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.orders.v1.OrderCalculationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the orders contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class OrdersClients {
    @Bean
    public OrderCalculationServiceGrpc.OrderCalculationServiceBlockingStub ordersOrderCalculationServiceBlocking(ChannelRegistry channels) {
        return OrderCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.orders.v1.OrderCalculationService"));
    }
}
