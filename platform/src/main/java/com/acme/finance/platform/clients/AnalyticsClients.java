package com.acme.finance.platform.clients;

import com.acme.analytics.v1.AnalyticsEventQueryServiceGrpc;
import com.acme.analytics.v1.AnalyticsEventServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the analytics contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AnalyticsClients {
    @Bean
    public AnalyticsEventQueryServiceGrpc.AnalyticsEventQueryServiceBlockingStub analyticsAnalyticsEventQueryServiceBlocking(ChannelRegistry channels) {
        return AnalyticsEventQueryServiceGrpc.newBlockingStub(channels.lookup("acme.analytics.v1.AnalyticsEventQueryService"));
    }

    @Bean
    public AnalyticsEventServiceGrpc.AnalyticsEventServiceBlockingStub analyticsAnalyticsEventServiceBlocking(ChannelRegistry channels) {
        return AnalyticsEventServiceGrpc.newBlockingStub(channels.lookup("acme.analytics.v1.AnalyticsEventService"));
    }

    @Bean
    public AnalyticsEventServiceGrpc.AnalyticsEventServiceFutureStub analyticsAnalyticsEventServiceFuture(ChannelRegistry channels) {
        return AnalyticsEventServiceGrpc.newFutureStub(channels.lookup("acme.analytics.v1.AnalyticsEventService"));
    }
}
