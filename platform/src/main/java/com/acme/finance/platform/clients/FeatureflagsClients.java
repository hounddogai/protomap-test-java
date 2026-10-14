package com.acme.finance.platform.clients;

import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the featureflags contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FeatureflagsClients {
    @Bean
    public FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceStub featureflagsFeatureFlagQueryServiceAsync(ChannelRegistry channels) {
        return FeatureFlagQueryServiceGrpc.newStub(channels.lookup("acme.featureflags.v1.FeatureFlagQueryService"));
    }

    @Bean
    public FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking(ChannelRegistry channels) {
        return FeatureFlagQueryServiceGrpc.newBlockingStub(channels.lookup("acme.featureflags.v1.FeatureFlagQueryService"));
    }

    @Bean
    public FeatureFlagServiceGrpc.FeatureFlagServiceStub featureflagsFeatureFlagServiceAsync(ChannelRegistry channels) {
        return FeatureFlagServiceGrpc.newStub(channels.lookup("acme.featureflags.v1.FeatureFlagService"));
    }

    @Bean
    public FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking(ChannelRegistry channels) {
        return FeatureFlagServiceGrpc.newBlockingStub(channels.lookup("acme.featureflags.v1.FeatureFlagService"));
    }

    @Bean
    public FeatureFlagServiceGrpc.FeatureFlagServiceFutureStub featureflagsFeatureFlagServiceFuture(ChannelRegistry channels) {
        return FeatureFlagServiceGrpc.newFutureStub(channels.lookup("acme.featureflags.v1.FeatureFlagService"));
    }

    @Bean
    public FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceStub featureflagsFeatureFlagValidationServiceAsync(ChannelRegistry channels) {
        return FeatureFlagValidationServiceGrpc.newStub(channels.lookup("acme.featureflags.v1.FeatureFlagValidationService"));
    }

    @Bean
    public FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking(ChannelRegistry channels) {
        return FeatureFlagValidationServiceGrpc.newBlockingStub(channels.lookup("acme.featureflags.v1.FeatureFlagValidationService"));
    }

    @Bean
    public FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceFutureStub featureflagsFeatureFlagValidationServiceFuture(ChannelRegistry channels) {
        return FeatureFlagValidationServiceGrpc.newFutureStub(channels.lookup("acme.featureflags.v1.FeatureFlagValidationService"));
    }
}
