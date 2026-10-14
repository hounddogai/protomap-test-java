package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.pipelines.v1.PipelineCalculationServiceGrpc;
import com.acme.pipelines.v1.PipelineQueryServiceGrpc;
import com.acme.pipelines.v1.PipelineServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the pipelines contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PipelinesClients {
    @Bean
    public PipelineCalculationServiceGrpc.PipelineCalculationServiceBlockingStub pipelinesPipelineCalculationServiceBlocking(ChannelRegistry channels) {
        return PipelineCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.pipelines.v1.PipelineCalculationService"));
    }

    @Bean
    public PipelineQueryServiceGrpc.PipelineQueryServiceBlockingStub pipelinesPipelineQueryServiceBlocking(ChannelRegistry channels) {
        return PipelineQueryServiceGrpc.newBlockingStub(channels.lookup("acme.pipelines.v1.PipelineQueryService"));
    }

    @Bean
    public PipelineQueryServiceGrpc.PipelineQueryServiceFutureStub pipelinesPipelineQueryServiceFuture(ChannelRegistry channels) {
        return PipelineQueryServiceGrpc.newFutureStub(channels.lookup("acme.pipelines.v1.PipelineQueryService"));
    }

    @Bean
    public PipelineServiceGrpc.PipelineServiceBlockingStub pipelinesPipelineServiceBlocking(ChannelRegistry channels) {
        return PipelineServiceGrpc.newBlockingStub(channels.lookup("acme.pipelines.v1.PipelineService"));
    }
}
