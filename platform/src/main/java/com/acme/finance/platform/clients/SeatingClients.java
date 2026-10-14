package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.seating.v1.SeatMapCalculationServiceGrpc;
import com.acme.seating.v1.SeatMapQueryServiceGrpc;
import com.acme.seating.v1.SeatMapServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the seating contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SeatingClients {
    @Bean
    public SeatMapCalculationServiceGrpc.SeatMapCalculationServiceBlockingStub seatingSeatMapCalculationServiceBlocking(ChannelRegistry channels) {
        return SeatMapCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.seating.v1.SeatMapCalculationService"));
    }

    @Bean
    public SeatMapQueryServiceGrpc.SeatMapQueryServiceBlockingStub seatingSeatMapQueryServiceBlocking(ChannelRegistry channels) {
        return SeatMapQueryServiceGrpc.newBlockingStub(channels.lookup("acme.seating.v1.SeatMapQueryService"));
    }

    @Bean
    public SeatMapServiceGrpc.SeatMapServiceBlockingStub seatingSeatMapServiceBlocking(ChannelRegistry channels) {
        return SeatMapServiceGrpc.newBlockingStub(channels.lookup("acme.seating.v1.SeatMapService"));
    }
}
