package com.acme.finance.platform.clients;

import com.acme.edgegateway.v1.RouteBindingQueryServiceGrpc;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the edgegateway contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class EdgegatewayClients {
    @Bean
    public RouteBindingQueryServiceGrpc.RouteBindingQueryServiceBlockingStub edgegatewayRouteBindingQueryServiceBlocking(ChannelRegistry channels) {
        return RouteBindingQueryServiceGrpc.newBlockingStub(channels.lookup("acme.edgegateway.v1.RouteBindingQueryService"));
    }

    @Bean
    public RouteBindingServiceGrpc.RouteBindingServiceBlockingStub edgegatewayRouteBindingServiceBlocking(ChannelRegistry channels) {
        return RouteBindingServiceGrpc.newBlockingStub(channels.lookup("acme.edgegateway.v1.RouteBindingService"));
    }
}
