package com.acme.finance.platform.clients;

import com.acme.careplans.v1.CarePlanCalculationServiceGrpc;
import com.acme.careplans.v1.CarePlanQueryServiceGrpc;
import com.acme.careplans.v1.CarePlanServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the careplans contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CareplansClients {
    @Bean
    public CarePlanCalculationServiceGrpc.CarePlanCalculationServiceBlockingStub careplansCarePlanCalculationServiceBlocking(ChannelRegistry channels) {
        return CarePlanCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.careplans.v1.CarePlanCalculationService"));
    }

    @Bean
    public CarePlanQueryServiceGrpc.CarePlanQueryServiceBlockingStub careplansCarePlanQueryServiceBlocking(ChannelRegistry channels) {
        return CarePlanQueryServiceGrpc.newBlockingStub(channels.lookup("acme.careplans.v1.CarePlanQueryService"));
    }

    @Bean
    public CarePlanServiceGrpc.CarePlanServiceBlockingStub careplansCarePlanServiceBlocking(ChannelRegistry channels) {
        return CarePlanServiceGrpc.newBlockingStub(channels.lookup("acme.careplans.v1.CarePlanService"));
    }
}
