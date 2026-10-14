package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the metrics contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class MetricsClients {
    @Bean
    public MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceStub metricsMetricSeriesQueryServiceAsync(ChannelRegistry channels) {
        return MetricSeriesQueryServiceGrpc.newStub(channels.lookup("acme.metrics.v1.MetricSeriesQueryService"));
    }

    @Bean
    public MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking(ChannelRegistry channels) {
        return MetricSeriesQueryServiceGrpc.newBlockingStub(channels.lookup("acme.metrics.v1.MetricSeriesQueryService"));
    }

    @Bean
    public MetricSeriesServiceGrpc.MetricSeriesServiceStub metricsMetricSeriesServiceAsync(ChannelRegistry channels) {
        return MetricSeriesServiceGrpc.newStub(channels.lookup("acme.metrics.v1.MetricSeriesService"));
    }

    @Bean
    public MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking(ChannelRegistry channels) {
        return MetricSeriesServiceGrpc.newBlockingStub(channels.lookup("acme.metrics.v1.MetricSeriesService"));
    }

    @Bean
    public MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking(ChannelRegistry channels) {
        return MetricSeriesValidationServiceGrpc.newBlockingStub(channels.lookup("acme.metrics.v1.MetricSeriesValidationService"));
    }
}
