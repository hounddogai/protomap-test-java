package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.webhooks.v1.WebhookEndpointQueryServiceGrpc;
import com.acme.webhooks.v1.WebhookEndpointServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the webhooks contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class WebhooksClients {
    @Bean
    public WebhookEndpointQueryServiceGrpc.WebhookEndpointQueryServiceBlockingStub webhooksWebhookEndpointQueryServiceBlocking(ChannelRegistry channels) {
        return WebhookEndpointQueryServiceGrpc.newBlockingStub(channels.lookup("acme.webhooks.v1.WebhookEndpointQueryService"));
    }

    @Bean
    public WebhookEndpointServiceGrpc.WebhookEndpointServiceBlockingStub webhooksWebhookEndpointServiceBlocking(ChannelRegistry channels) {
        return WebhookEndpointServiceGrpc.newBlockingStub(channels.lookup("acme.webhooks.v1.WebhookEndpointService"));
    }
}
