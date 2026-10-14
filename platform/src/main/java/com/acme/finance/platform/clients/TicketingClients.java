package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.ticketing.v1.TicketQueryServiceGrpc;
import com.acme.ticketing.v1.TicketServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the ticketing contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class TicketingClients {
    @Bean
    public TicketQueryServiceGrpc.TicketQueryServiceBlockingStub ticketingTicketQueryServiceBlocking(ChannelRegistry channels) {
        return TicketQueryServiceGrpc.newBlockingStub(channels.lookup("acme.ticketing.v1.TicketQueryService"));
    }

    @Bean
    public TicketQueryServiceGrpc.TicketQueryServiceFutureStub ticketingTicketQueryServiceFuture(ChannelRegistry channels) {
        return TicketQueryServiceGrpc.newFutureStub(channels.lookup("acme.ticketing.v1.TicketQueryService"));
    }

    @Bean
    public TicketServiceGrpc.TicketServiceBlockingStub ticketingTicketServiceBlocking(ChannelRegistry channels) {
        return TicketServiceGrpc.newBlockingStub(channels.lookup("acme.ticketing.v1.TicketService"));
    }
}
