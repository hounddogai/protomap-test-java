package com.acme.finance.platform.clients;

import com.acme.anomalies.v1.AnomalyQueryServiceGrpc;
import com.acme.anomalies.v1.AnomalyServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the anomalies contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AnomaliesClients {
    @Bean
    public AnomalyQueryServiceGrpc.AnomalyQueryServiceBlockingStub anomaliesAnomalyQueryServiceBlocking(ChannelRegistry channels) {
        return AnomalyQueryServiceGrpc.newBlockingStub(channels.lookup("acme.anomalies.v1.AnomalyQueryService"));
    }

    @Bean
    public AnomalyServiceGrpc.AnomalyServiceBlockingStub anomaliesAnomalyServiceBlocking(ChannelRegistry channels) {
        return AnomalyServiceGrpc.newBlockingStub(channels.lookup("acme.anomalies.v1.AnomalyService"));
    }
}
