package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.helpdesk.v1.TicketCalculationServiceGrpc;
import com.acme.helpdesk.v1.TicketQueryServiceGrpc;
import com.acme.helpdesk.v1.TicketServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the helpdesk contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class HelpdeskClients {
    @Bean
    public TicketCalculationServiceGrpc.TicketCalculationServiceBlockingStub helpdeskTicketCalculationServiceBlocking(ChannelRegistry channels) {
        return TicketCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.helpdesk.v1.TicketCalculationService"));
    }

    @Bean
    public TicketQueryServiceGrpc.TicketQueryServiceBlockingStub helpdeskTicketQueryServiceBlocking(ChannelRegistry channels) {
        return TicketQueryServiceGrpc.newBlockingStub(channels.lookup("acme.helpdesk.v1.TicketQueryService"));
    }

    @Bean
    public TicketServiceGrpc.TicketServiceBlockingStub helpdeskTicketServiceBlocking(ChannelRegistry channels) {
        return TicketServiceGrpc.newBlockingStub(channels.lookup("acme.helpdesk.v1.TicketService"));
    }
}
