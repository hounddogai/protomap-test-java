package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.fleet.v1.VehicleQueryServiceGrpc;
import com.acme.fleet.v1.VehicleServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the fleet contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FleetClients {
    @Bean
    public VehicleQueryServiceGrpc.VehicleQueryServiceBlockingStub fleetVehicleQueryServiceBlocking(ChannelRegistry channels) {
        return VehicleQueryServiceGrpc.newBlockingStub(channels.lookup("acme.fleet.v1.VehicleQueryService"));
    }

    @Bean
    public VehicleServiceGrpc.VehicleServiceBlockingStub fleetVehicleServiceBlocking(ChannelRegistry channels) {
        return VehicleServiceGrpc.newBlockingStub(channels.lookup("acme.fleet.v1.VehicleService"));
    }
}
