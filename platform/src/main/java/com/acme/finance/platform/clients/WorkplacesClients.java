package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.workplaces.v1.BuildingCalculationServiceGrpc;
import com.acme.workplaces.v1.BuildingQueryServiceGrpc;
import com.acme.workplaces.v1.BuildingServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the workplaces contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class WorkplacesClients {
    @Bean
    public BuildingCalculationServiceGrpc.BuildingCalculationServiceBlockingStub workplacesBuildingCalculationServiceBlocking(ChannelRegistry channels) {
        return BuildingCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.workplaces.v1.BuildingCalculationService"));
    }

    @Bean
    public BuildingQueryServiceGrpc.BuildingQueryServiceBlockingStub workplacesBuildingQueryServiceBlocking(ChannelRegistry channels) {
        return BuildingQueryServiceGrpc.newBlockingStub(channels.lookup("acme.workplaces.v1.BuildingQueryService"));
    }

    @Bean
    public BuildingServiceGrpc.BuildingServiceBlockingStub workplacesBuildingServiceBlocking(ChannelRegistry channels) {
        return BuildingServiceGrpc.newBlockingStub(channels.lookup("acme.workplaces.v1.BuildingService"));
    }
}
