package com.acme.finance.platform.clients;

import com.acme.checkout.v1.CheckoutSessionServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the checkout contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CheckoutClients {
    @Bean
    public CheckoutSessionServiceGrpc.CheckoutSessionServiceBlockingStub checkoutCheckoutSessionServiceBlocking(ChannelRegistry channels) {
        return CheckoutSessionServiceGrpc.newBlockingStub(channels.lookup("acme.checkout.v1.CheckoutSessionService"));
    }
}
