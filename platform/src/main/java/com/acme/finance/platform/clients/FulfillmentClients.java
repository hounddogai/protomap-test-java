package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.fulfillment.v1.FulfillmentQueryServiceGrpc;
import com.acme.fulfillment.v1.FulfillmentServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the fulfillment contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FulfillmentClients {
    @Bean
    public FulfillmentQueryServiceGrpc.FulfillmentQueryServiceBlockingStub fulfillmentFulfillmentQueryServiceBlocking(ChannelRegistry channels) {
        return FulfillmentQueryServiceGrpc.newBlockingStub(channels.lookup("acme.fulfillment.v1.FulfillmentQueryService"));
    }

    @Bean
    public FulfillmentServiceGrpc.FulfillmentServiceBlockingStub fulfillmentFulfillmentServiceBlocking(ChannelRegistry channels) {
        return FulfillmentServiceGrpc.newBlockingStub(channels.lookup("acme.fulfillment.v1.FulfillmentService"));
    }
}
