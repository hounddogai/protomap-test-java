package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.acme.venues.v1.VenueServiceGrpc;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the venues contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class VenuesClients {
    @Bean
    public VenueQueryServiceGrpc.VenueQueryServiceStub venuesVenueQueryServiceAsync(ChannelRegistry channels) {
        return VenueQueryServiceGrpc.newStub(channels.lookup("acme.venues.v1.VenueQueryService"));
    }

    @Bean
    public VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking(ChannelRegistry channels) {
        return VenueQueryServiceGrpc.newBlockingStub(channels.lookup("acme.venues.v1.VenueQueryService"));
    }

    @Bean
    public VenueQueryServiceGrpc.VenueQueryServiceFutureStub venuesVenueQueryServiceFuture(ChannelRegistry channels) {
        return VenueQueryServiceGrpc.newFutureStub(channels.lookup("acme.venues.v1.VenueQueryService"));
    }

    @Bean
    public VenueServiceGrpc.VenueServiceStub venuesVenueServiceAsync(ChannelRegistry channels) {
        return VenueServiceGrpc.newStub(channels.lookup("acme.venues.v1.VenueService"));
    }

    @Bean
    public VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking(ChannelRegistry channels) {
        return VenueServiceGrpc.newBlockingStub(channels.lookup("acme.venues.v1.VenueService"));
    }

    @Bean
    public VenueServiceGrpc.VenueServiceFutureStub venuesVenueServiceFuture(ChannelRegistry channels) {
        return VenueServiceGrpc.newFutureStub(channels.lookup("acme.venues.v1.VenueService"));
    }

    @Bean
    public VenueValidationServiceGrpc.VenueValidationServiceStub venuesVenueValidationServiceAsync(ChannelRegistry channels) {
        return VenueValidationServiceGrpc.newStub(channels.lookup("acme.venues.v1.VenueValidationService"));
    }

    @Bean
    public VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking(ChannelRegistry channels) {
        return VenueValidationServiceGrpc.newBlockingStub(channels.lookup("acme.venues.v1.VenueValidationService"));
    }

    @Bean
    public VenueValidationServiceGrpc.VenueValidationServiceFutureStub venuesVenueValidationServiceFuture(ChannelRegistry channels) {
        return VenueValidationServiceGrpc.newFutureStub(channels.lookup("acme.venues.v1.VenueValidationService"));
    }
}
