package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.recommendations.v1.RecommendationSetQueryServiceGrpc;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the recommendations contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class RecommendationsClients {
    @Bean
    public RecommendationSetQueryServiceGrpc.RecommendationSetQueryServiceBlockingStub recommendationsRecommendationSetQueryServiceBlocking(ChannelRegistry channels) {
        return RecommendationSetQueryServiceGrpc.newBlockingStub(channels.lookup("acme.recommendations.v1.RecommendationSetQueryService"));
    }

    @Bean
    public RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking(ChannelRegistry channels) {
        return RecommendationSetServiceGrpc.newBlockingStub(channels.lookup("acme.recommendations.v1.RecommendationSetService"));
    }
}
