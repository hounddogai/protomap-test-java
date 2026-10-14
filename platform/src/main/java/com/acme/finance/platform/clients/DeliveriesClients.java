package com.acme.finance.platform.clients;

import com.acme.deliveries.v1.DeliveryQueryServiceGrpc;
import com.acme.deliveries.v1.DeliveryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the deliveries contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DeliveriesClients {
    @Bean
    public DeliveryQueryServiceGrpc.DeliveryQueryServiceBlockingStub deliveriesDeliveryQueryServiceBlocking(ChannelRegistry channels) {
        return DeliveryQueryServiceGrpc.newBlockingStub(channels.lookup("acme.deliveries.v1.DeliveryQueryService"));
    }

    @Bean
    public DeliveryServiceGrpc.DeliveryServiceBlockingStub deliveriesDeliveryServiceBlocking(ChannelRegistry channels) {
        return DeliveryServiceGrpc.newBlockingStub(channels.lookup("acme.deliveries.v1.DeliveryService"));
    }
}
