package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.fuel.v1.FuelCardCalculationServiceGrpc;
import com.acme.fuel.v1.FuelCardQueryServiceGrpc;
import com.acme.fuel.v1.FuelCardServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the fuel contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FuelClients {
    @Bean
    public FuelCardCalculationServiceGrpc.FuelCardCalculationServiceBlockingStub fuelFuelCardCalculationServiceBlocking(ChannelRegistry channels) {
        return FuelCardCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.fuel.v1.FuelCardCalculationService"));
    }

    @Bean
    public FuelCardQueryServiceGrpc.FuelCardQueryServiceBlockingStub fuelFuelCardQueryServiceBlocking(ChannelRegistry channels) {
        return FuelCardQueryServiceGrpc.newBlockingStub(channels.lookup("acme.fuel.v1.FuelCardQueryService"));
    }

    @Bean
    public FuelCardServiceGrpc.FuelCardServiceBlockingStub fuelFuelCardServiceBlocking(ChannelRegistry channels) {
        return FuelCardServiceGrpc.newBlockingStub(channels.lookup("acme.fuel.v1.FuelCardService"));
    }
}
