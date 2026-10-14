package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the subscriptions contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SubscriptionsClients {
    @Bean
    public SubscriptionServiceGrpc.SubscriptionServiceBlockingStub subscriptionsSubscriptionServiceBlocking(ChannelRegistry channels) {
        return SubscriptionServiceGrpc.newBlockingStub(channels.lookup("acme.subscriptions.v1.SubscriptionService"));
    }
}
