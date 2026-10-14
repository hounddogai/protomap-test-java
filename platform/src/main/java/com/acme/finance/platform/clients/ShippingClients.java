package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.shipping.v1.ShipmentQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the shipping contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ShippingClients {
    @Bean
    public ShipmentQueryServiceGrpc.ShipmentQueryServiceBlockingStub shippingShipmentQueryServiceBlocking(ChannelRegistry channels) {
        return ShipmentQueryServiceGrpc.newBlockingStub(channels.lookup("acme.shipping.v1.ShipmentQueryService"));
    }
}
