package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.lineage.v1.LineageNodeCalculationServiceGrpc;
import com.acme.lineage.v1.LineageNodeQueryServiceGrpc;
import com.acme.lineage.v1.LineageNodeServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the lineage contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LineageClients {
    @Bean
    public LineageNodeCalculationServiceGrpc.LineageNodeCalculationServiceBlockingStub lineageLineageNodeCalculationServiceBlocking(ChannelRegistry channels) {
        return LineageNodeCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.lineage.v1.LineageNodeCalculationService"));
    }

    @Bean
    public LineageNodeQueryServiceGrpc.LineageNodeQueryServiceBlockingStub lineageLineageNodeQueryServiceBlocking(ChannelRegistry channels) {
        return LineageNodeQueryServiceGrpc.newBlockingStub(channels.lookup("acme.lineage.v1.LineageNodeQueryService"));
    }

    @Bean
    public LineageNodeServiceGrpc.LineageNodeServiceStub lineageLineageNodeServiceAsync(ChannelRegistry channels) {
        return LineageNodeServiceGrpc.newStub(channels.lookup("acme.lineage.v1.LineageNodeService"));
    }

    @Bean
    public LineageNodeServiceGrpc.LineageNodeServiceBlockingStub lineageLineageNodeServiceBlocking(ChannelRegistry channels) {
        return LineageNodeServiceGrpc.newBlockingStub(channels.lookup("acme.lineage.v1.LineageNodeService"));
    }
}
