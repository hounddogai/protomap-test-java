package com.acme.finance.platform.clients;

import com.acme.drivers.v1.DriverCalculationServiceGrpc;
import com.acme.drivers.v1.DriverServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the drivers contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DriversClients {
    @Bean
    public DriverCalculationServiceGrpc.DriverCalculationServiceBlockingStub driversDriverCalculationServiceBlocking(ChannelRegistry channels) {
        return DriverCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.drivers.v1.DriverCalculationService"));
    }

    @Bean
    public DriverServiceGrpc.DriverServiceBlockingStub driversDriverServiceBlocking(ChannelRegistry channels) {
        return DriverServiceGrpc.newBlockingStub(channels.lookup("acme.drivers.v1.DriverService"));
    }
}
