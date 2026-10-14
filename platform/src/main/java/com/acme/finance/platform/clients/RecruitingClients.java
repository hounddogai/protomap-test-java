package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.recruiting.v1.JobOpeningCalculationServiceGrpc;
import com.acme.recruiting.v1.JobOpeningQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the recruiting contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class RecruitingClients {
    @Bean
    public JobOpeningCalculationServiceGrpc.JobOpeningCalculationServiceStub recruitingJobOpeningCalculationServiceAsync(ChannelRegistry channels) {
        return JobOpeningCalculationServiceGrpc.newStub(channels.lookup("acme.recruiting.v1.JobOpeningCalculationService"));
    }

    @Bean
    public JobOpeningCalculationServiceGrpc.JobOpeningCalculationServiceBlockingStub recruitingJobOpeningCalculationServiceBlocking(ChannelRegistry channels) {
        return JobOpeningCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.recruiting.v1.JobOpeningCalculationService"));
    }

    @Bean
    public JobOpeningQueryServiceGrpc.JobOpeningQueryServiceBlockingStub recruitingJobOpeningQueryServiceBlocking(ChannelRegistry channels) {
        return JobOpeningQueryServiceGrpc.newBlockingStub(channels.lookup("acme.recruiting.v1.JobOpeningQueryService"));
    }
}
