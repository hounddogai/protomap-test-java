package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.ratelimiting.v1.RateLimitCalculationServiceGrpc;
import com.acme.ratelimiting.v1.RateLimitQueryServiceGrpc;
import com.acme.ratelimiting.v1.RateLimitServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the ratelimiting contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class RatelimitingClients {
    @Bean
    public RateLimitCalculationServiceGrpc.RateLimitCalculationServiceBlockingStub ratelimitingRateLimitCalculationServiceBlocking(ChannelRegistry channels) {
        return RateLimitCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.ratelimiting.v1.RateLimitCalculationService"));
    }

    @Bean
    public RateLimitCalculationServiceGrpc.RateLimitCalculationServiceFutureStub ratelimitingRateLimitCalculationServiceFuture(ChannelRegistry channels) {
        return RateLimitCalculationServiceGrpc.newFutureStub(channels.lookup("acme.ratelimiting.v1.RateLimitCalculationService"));
    }

    @Bean
    public RateLimitQueryServiceGrpc.RateLimitQueryServiceBlockingStub ratelimitingRateLimitQueryServiceBlocking(ChannelRegistry channels) {
        return RateLimitQueryServiceGrpc.newBlockingStub(channels.lookup("acme.ratelimiting.v1.RateLimitQueryService"));
    }

    @Bean
    public RateLimitServiceGrpc.RateLimitServiceBlockingStub ratelimitingRateLimitServiceBlocking(ChannelRegistry channels) {
        return RateLimitServiceGrpc.newBlockingStub(channels.lookup("acme.ratelimiting.v1.RateLimitService"));
    }

    @Bean
    public RateLimitServiceGrpc.RateLimitServiceFutureStub ratelimitingRateLimitServiceFuture(ChannelRegistry channels) {
        return RateLimitServiceGrpc.newFutureStub(channels.lookup("acme.ratelimiting.v1.RateLimitService"));
    }
}
