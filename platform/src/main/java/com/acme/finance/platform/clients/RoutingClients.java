package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.routing.v1.RouteCalculationServiceGrpc;
import com.acme.routing.v1.RouteQueryServiceGrpc;
import com.acme.routing.v1.RouteServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the routing contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class RoutingClients {
    @Bean
    public RouteCalculationServiceGrpc.RouteCalculationServiceBlockingStub routingRouteCalculationServiceBlocking(ChannelRegistry channels) {
        return RouteCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.routing.v1.RouteCalculationService"));
    }

    @Bean
    public RouteQueryServiceGrpc.RouteQueryServiceBlockingStub routingRouteQueryServiceBlocking(ChannelRegistry channels) {
        return RouteQueryServiceGrpc.newBlockingStub(channels.lookup("acme.routing.v1.RouteQueryService"));
    }

    @Bean
    public RouteServiceGrpc.RouteServiceBlockingStub routingRouteServiceBlocking(ChannelRegistry channels) {
        return RouteServiceGrpc.newBlockingStub(channels.lookup("acme.routing.v1.RouteService"));
    }
}
