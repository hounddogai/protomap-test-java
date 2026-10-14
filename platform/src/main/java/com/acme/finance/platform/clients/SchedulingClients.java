package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.scheduling.v1.ShiftServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the scheduling contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SchedulingClients {
    @Bean
    public ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking(ChannelRegistry channels) {
        return ShiftQueryServiceGrpc.newBlockingStub(channels.lookup("acme.scheduling.v1.ShiftQueryService"));
    }

    @Bean
    public ShiftServiceGrpc.ShiftServiceBlockingStub schedulingShiftServiceBlocking(ChannelRegistry channels) {
        return ShiftServiceGrpc.newBlockingStub(channels.lookup("acme.scheduling.v1.ShiftService"));
    }
}
