package com.acme.finance.platform.clients;

import com.acme.benefits.v1.BenefitPlanQueryServiceGrpc;
import com.acme.benefits.v1.BenefitPlanServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the benefits contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class BenefitsClients {
    @Bean
    public BenefitPlanQueryServiceGrpc.BenefitPlanQueryServiceBlockingStub benefitsBenefitPlanQueryServiceBlocking(ChannelRegistry channels) {
        return BenefitPlanQueryServiceGrpc.newBlockingStub(channels.lookup("acme.benefits.v1.BenefitPlanQueryService"));
    }

    @Bean
    public BenefitPlanServiceGrpc.BenefitPlanServiceBlockingStub benefitsBenefitPlanServiceBlocking(ChannelRegistry channels) {
        return BenefitPlanServiceGrpc.newBlockingStub(channels.lookup("acme.benefits.v1.BenefitPlanService"));
    }
}
