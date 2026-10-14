package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.localization.v1.TranslationKeyCalculationServiceGrpc;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.localization.v1.TranslationKeyServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the localization contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LocalizationClients {
    @Bean
    public TranslationKeyCalculationServiceGrpc.TranslationKeyCalculationServiceBlockingStub localizationTranslationKeyCalculationServiceBlocking(ChannelRegistry channels) {
        return TranslationKeyCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.localization.v1.TranslationKeyCalculationService"));
    }

    @Bean
    public TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceBlockingStub localizationTranslationKeyQueryServiceBlocking(ChannelRegistry channels) {
        return TranslationKeyQueryServiceGrpc.newBlockingStub(channels.lookup("acme.localization.v1.TranslationKeyQueryService"));
    }

    @Bean
    public TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceFutureStub localizationTranslationKeyQueryServiceFuture(ChannelRegistry channels) {
        return TranslationKeyQueryServiceGrpc.newFutureStub(channels.lookup("acme.localization.v1.TranslationKeyQueryService"));
    }

    @Bean
    public TranslationKeyServiceGrpc.TranslationKeyServiceStub localizationTranslationKeyServiceAsync(ChannelRegistry channels) {
        return TranslationKeyServiceGrpc.newStub(channels.lookup("acme.localization.v1.TranslationKeyService"));
    }

    @Bean
    public TranslationKeyServiceGrpc.TranslationKeyServiceBlockingStub localizationTranslationKeyServiceBlocking(ChannelRegistry channels) {
        return TranslationKeyServiceGrpc.newBlockingStub(channels.lookup("acme.localization.v1.TranslationKeyService"));
    }
}
