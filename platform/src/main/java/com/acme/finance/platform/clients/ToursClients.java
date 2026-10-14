package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.tours.v1.TourQueryServiceGrpc;
import com.acme.tours.v1.TourServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the tours contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ToursClients {
    @Bean
    public TourQueryServiceGrpc.TourQueryServiceBlockingStub toursTourQueryServiceBlocking(ChannelRegistry channels) {
        return TourQueryServiceGrpc.newBlockingStub(channels.lookup("acme.tours.v1.TourQueryService"));
    }

    @Bean
    public TourServiceGrpc.TourServiceBlockingStub toursTourServiceBlocking(ChannelRegistry channels) {
        return TourServiceGrpc.newBlockingStub(channels.lookup("acme.tours.v1.TourService"));
    }
}
