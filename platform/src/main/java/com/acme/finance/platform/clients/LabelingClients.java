package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.labeling.v1.LabelingTaskCalculationServiceGrpc;
import com.acme.labeling.v1.LabelingTaskQueryServiceGrpc;
import com.acme.labeling.v1.LabelingTaskServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the labeling contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LabelingClients {
    @Bean
    public LabelingTaskCalculationServiceGrpc.LabelingTaskCalculationServiceBlockingStub labelingLabelingTaskCalculationServiceBlocking(ChannelRegistry channels) {
        return LabelingTaskCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.labeling.v1.LabelingTaskCalculationService"));
    }

    @Bean
    public LabelingTaskQueryServiceGrpc.LabelingTaskQueryServiceBlockingStub labelingLabelingTaskQueryServiceBlocking(ChannelRegistry channels) {
        return LabelingTaskQueryServiceGrpc.newBlockingStub(channels.lookup("acme.labeling.v1.LabelingTaskQueryService"));
    }

    @Bean
    public LabelingTaskServiceGrpc.LabelingTaskServiceBlockingStub labelingLabelingTaskServiceBlocking(ChannelRegistry channels) {
        return LabelingTaskServiceGrpc.newBlockingStub(channels.lookup("acme.labeling.v1.LabelingTaskService"));
    }
}
