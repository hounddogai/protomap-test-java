package com.acme.finance.platform.clients;

import com.acme.datasets.v1.DatasetServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the datasets contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class DatasetsClients {
    @Bean
    public DatasetServiceGrpc.DatasetServiceBlockingStub datasetsDatasetServiceBlocking(ChannelRegistry channels) {
        return DatasetServiceGrpc.newBlockingStub(channels.lookup("acme.datasets.v1.DatasetService"));
    }
}
