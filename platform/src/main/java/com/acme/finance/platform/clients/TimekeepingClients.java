package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.timekeeping.v1.TimesheetQueryServiceGrpc;
import com.acme.timekeeping.v1.TimesheetServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the timekeeping contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class TimekeepingClients {
    @Bean
    public TimesheetQueryServiceGrpc.TimesheetQueryServiceBlockingStub timekeepingTimesheetQueryServiceBlocking(ChannelRegistry channels) {
        return TimesheetQueryServiceGrpc.newBlockingStub(channels.lookup("acme.timekeeping.v1.TimesheetQueryService"));
    }

    @Bean
    public TimesheetServiceGrpc.TimesheetServiceBlockingStub timekeepingTimesheetServiceBlocking(ChannelRegistry channels) {
        return TimesheetServiceGrpc.newBlockingStub(channels.lookup("acme.timekeeping.v1.TimesheetService"));
    }
}
