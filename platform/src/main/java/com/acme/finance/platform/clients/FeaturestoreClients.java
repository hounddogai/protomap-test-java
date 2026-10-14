package com.acme.finance.platform.clients;

import com.acme.featurestore.v1.FeatureViewCalculationServiceGrpc;
import com.acme.featurestore.v1.FeatureViewQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the featurestore contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FeaturestoreClients {
    @Bean
    public FeatureViewCalculationServiceGrpc.FeatureViewCalculationServiceBlockingStub featurestoreFeatureViewCalculationServiceBlocking(ChannelRegistry channels) {
        return FeatureViewCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.featurestore.v1.FeatureViewCalculationService"));
    }

    @Bean
    public FeatureViewQueryServiceGrpc.FeatureViewQueryServiceBlockingStub featurestoreFeatureViewQueryServiceBlocking(ChannelRegistry channels) {
        return FeatureViewQueryServiceGrpc.newBlockingStub(channels.lookup("acme.featurestore.v1.FeatureViewQueryService"));
    }
}
