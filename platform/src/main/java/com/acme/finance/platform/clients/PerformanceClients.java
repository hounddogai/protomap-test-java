package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.performance.v1.ReviewQueryServiceGrpc;
import com.acme.performance.v1.ReviewServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the performance contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PerformanceClients {
    @Bean
    public ReviewQueryServiceGrpc.ReviewQueryServiceBlockingStub performanceReviewQueryServiceBlocking(ChannelRegistry channels) {
        return ReviewQueryServiceGrpc.newBlockingStub(channels.lookup("acme.performance.v1.ReviewQueryService"));
    }

    @Bean
    public ReviewServiceGrpc.ReviewServiceBlockingStub performanceReviewServiceBlocking(ChannelRegistry channels) {
        return ReviewServiceGrpc.newBlockingStub(channels.lookup("acme.performance.v1.ReviewService"));
    }
}
