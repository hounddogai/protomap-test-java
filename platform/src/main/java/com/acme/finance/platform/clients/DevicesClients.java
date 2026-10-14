package com.acme.finance.platform.clients;

import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the devices contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DevicesClients {
    @Bean
    public ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking(ChannelRegistry channels) {
        return ManagedDeviceCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.devices.v1.ManagedDeviceCalculationService"));
    }
}
