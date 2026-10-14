package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.media.v1.MediaAssetCalculationServiceGrpc;
import com.acme.media.v1.MediaAssetQueryServiceGrpc;
import com.acme.media.v1.MediaAssetServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the media contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class MediaClients {
    @Bean
    public MediaAssetCalculationServiceGrpc.MediaAssetCalculationServiceStub mediaMediaAssetCalculationServiceAsync(ChannelRegistry channels) {
        return MediaAssetCalculationServiceGrpc.newStub(channels.lookup("acme.media.v1.MediaAssetCalculationService"));
    }

    @Bean
    public MediaAssetCalculationServiceGrpc.MediaAssetCalculationServiceBlockingStub mediaMediaAssetCalculationServiceBlocking(ChannelRegistry channels) {
        return MediaAssetCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.media.v1.MediaAssetCalculationService"));
    }

    @Bean
    public MediaAssetQueryServiceGrpc.MediaAssetQueryServiceBlockingStub mediaMediaAssetQueryServiceBlocking(ChannelRegistry channels) {
        return MediaAssetQueryServiceGrpc.newBlockingStub(channels.lookup("acme.media.v1.MediaAssetQueryService"));
    }

    @Bean
    public MediaAssetQueryServiceGrpc.MediaAssetQueryServiceFutureStub mediaMediaAssetQueryServiceFuture(ChannelRegistry channels) {
        return MediaAssetQueryServiceGrpc.newFutureStub(channels.lookup("acme.media.v1.MediaAssetQueryService"));
    }

    @Bean
    public MediaAssetServiceGrpc.MediaAssetServiceBlockingStub mediaMediaAssetServiceBlocking(ChannelRegistry channels) {
        return MediaAssetServiceGrpc.newBlockingStub(channels.lookup("acme.media.v1.MediaAssetService"));
    }
}
