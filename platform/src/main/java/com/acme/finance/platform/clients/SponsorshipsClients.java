package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import com.acme.sponsorships.v1.SponsorQueryServiceGrpc;
import com.acme.sponsorships.v1.SponsorServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the sponsorships contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SponsorshipsClients {
    @Bean
    public SponsorCalculationServiceGrpc.SponsorCalculationServiceStub sponsorshipsSponsorCalculationServiceAsync(ChannelRegistry channels) {
        return SponsorCalculationServiceGrpc.newStub(channels.lookup("acme.sponsorships.v1.SponsorCalculationService"));
    }

    @Bean
    public SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking(ChannelRegistry channels) {
        return SponsorCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.sponsorships.v1.SponsorCalculationService"));
    }

    @Bean
    public SponsorQueryServiceGrpc.SponsorQueryServiceBlockingStub sponsorshipsSponsorQueryServiceBlocking(ChannelRegistry channels) {
        return SponsorQueryServiceGrpc.newBlockingStub(channels.lookup("acme.sponsorships.v1.SponsorQueryService"));
    }

    @Bean
    public SponsorQueryServiceGrpc.SponsorQueryServiceFutureStub sponsorshipsSponsorQueryServiceFuture(ChannelRegistry channels) {
        return SponsorQueryServiceGrpc.newFutureStub(channels.lookup("acme.sponsorships.v1.SponsorQueryService"));
    }

    @Bean
    public SponsorServiceGrpc.SponsorServiceBlockingStub sponsorshipsSponsorServiceBlocking(ChannelRegistry channels) {
        return SponsorServiceGrpc.newBlockingStub(channels.lookup("acme.sponsorships.v1.SponsorService"));
    }
}
