package com.acme.finance.platform.clients;

import com.acme.driverapp.v1.DriverHomeQueryServiceGrpc;
import com.acme.driverapp.v1.DriverHomeServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the driverapp contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DriverappClients {
    @Bean
    public DriverHomeQueryServiceGrpc.DriverHomeQueryServiceBlockingStub driverappDriverHomeQueryServiceBlocking(ChannelRegistry channels) {
        return DriverHomeQueryServiceGrpc.newBlockingStub(channels.lookup("acme.driverapp.v1.DriverHomeQueryService"));
    }

    @Bean
    public DriverHomeServiceGrpc.DriverHomeServiceBlockingStub driverappDriverHomeServiceBlocking(ChannelRegistry channels) {
        return DriverHomeServiceGrpc.newBlockingStub(channels.lookup("acme.driverapp.v1.DriverHomeService"));
    }
}
