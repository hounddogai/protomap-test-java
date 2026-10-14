package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.onboarding.v1.OnboardingPlanCalculationServiceGrpc;
import com.acme.onboarding.v1.OnboardingPlanQueryServiceGrpc;
import com.acme.onboarding.v1.OnboardingPlanServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the onboarding contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class OnboardingClients {
    @Bean
    public OnboardingPlanCalculationServiceGrpc.OnboardingPlanCalculationServiceBlockingStub onboardingOnboardingPlanCalculationServiceBlocking(ChannelRegistry channels) {
        return OnboardingPlanCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.onboarding.v1.OnboardingPlanCalculationService"));
    }

    @Bean
    public OnboardingPlanQueryServiceGrpc.OnboardingPlanQueryServiceBlockingStub onboardingOnboardingPlanQueryServiceBlocking(ChannelRegistry channels) {
        return OnboardingPlanQueryServiceGrpc.newBlockingStub(channels.lookup("acme.onboarding.v1.OnboardingPlanQueryService"));
    }

    @Bean
    public OnboardingPlanServiceGrpc.OnboardingPlanServiceBlockingStub onboardingOnboardingPlanServiceBlocking(ChannelRegistry channels) {
        return OnboardingPlanServiceGrpc.newBlockingStub(channels.lookup("acme.onboarding.v1.OnboardingPlanService"));
    }
}
