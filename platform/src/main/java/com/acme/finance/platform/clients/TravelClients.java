package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.travel.v1.TripCalculationServiceGrpc;
import com.acme.travel.v1.TripQueryServiceGrpc;
import com.acme.travel.v1.TripServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the travel contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class TravelClients {
    @Bean
    public TripCalculationServiceGrpc.TripCalculationServiceBlockingStub travelTripCalculationServiceBlocking(ChannelRegistry channels) {
        return TripCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.travel.v1.TripCalculationService"));
    }

    @Bean
    public TripQueryServiceGrpc.TripQueryServiceBlockingStub travelTripQueryServiceBlocking(ChannelRegistry channels) {
        return TripQueryServiceGrpc.newBlockingStub(channels.lookup("acme.travel.v1.TripQueryService"));
    }

    @Bean
    public TripServiceGrpc.TripServiceBlockingStub travelTripServiceBlocking(ChannelRegistry channels) {
        return TripServiceGrpc.newBlockingStub(channels.lookup("acme.travel.v1.TripService"));
    }
}
