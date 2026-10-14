package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the forecasting contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ForecastingClients {
    @Bean
    public DemandForecastQueryServiceGrpc.DemandForecastQueryServiceBlockingStub forecastingDemandForecastQueryServiceBlocking(ChannelRegistry channels) {
        return DemandForecastQueryServiceGrpc.newBlockingStub(channels.lookup("acme.forecasting.v1.DemandForecastQueryService"));
    }

    @Bean
    public DemandForecastQueryServiceGrpc.DemandForecastQueryServiceFutureStub forecastingDemandForecastQueryServiceFuture(ChannelRegistry channels) {
        return DemandForecastQueryServiceGrpc.newFutureStub(channels.lookup("acme.forecasting.v1.DemandForecastQueryService"));
    }
}
