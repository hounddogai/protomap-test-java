package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.shows.v1.ShowQueryServiceGrpc;
import com.acme.shows.v1.ShowServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the shows contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ShowsClients {
    @Bean
    public ShowCalculationServiceGrpc.ShowCalculationServiceBlockingStub showsShowCalculationServiceBlocking(ChannelRegistry channels) {
        return ShowCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.shows.v1.ShowCalculationService"));
    }

    @Bean
    public ShowQueryServiceGrpc.ShowQueryServiceBlockingStub showsShowQueryServiceBlocking(ChannelRegistry channels) {
        return ShowQueryServiceGrpc.newBlockingStub(channels.lookup("acme.shows.v1.ShowQueryService"));
    }

    @Bean
    public ShowServiceGrpc.ShowServiceBlockingStub showsShowServiceBlocking(ChannelRegistry channels) {
        return ShowServiceGrpc.newBlockingStub(channels.lookup("acme.shows.v1.ShowService"));
    }
}
