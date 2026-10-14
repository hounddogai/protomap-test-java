package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the search contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SearchClients {
    @Bean
    public SearchIndexQueryServiceGrpc.SearchIndexQueryServiceStub searchSearchIndexQueryServiceAsync(ChannelRegistry channels) {
        return SearchIndexQueryServiceGrpc.newStub(channels.lookup("acme.search.v1.SearchIndexQueryService"));
    }

    @Bean
    public SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking(ChannelRegistry channels) {
        return SearchIndexQueryServiceGrpc.newBlockingStub(channels.lookup("acme.search.v1.SearchIndexQueryService"));
    }

    @Bean
    public SearchIndexQueryServiceGrpc.SearchIndexQueryServiceFutureStub searchSearchIndexQueryServiceFuture(ChannelRegistry channels) {
        return SearchIndexQueryServiceGrpc.newFutureStub(channels.lookup("acme.search.v1.SearchIndexQueryService"));
    }

    @Bean
    public SearchIndexServiceGrpc.SearchIndexServiceStub searchSearchIndexServiceAsync(ChannelRegistry channels) {
        return SearchIndexServiceGrpc.newStub(channels.lookup("acme.search.v1.SearchIndexService"));
    }

    @Bean
    public SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking(ChannelRegistry channels) {
        return SearchIndexServiceGrpc.newBlockingStub(channels.lookup("acme.search.v1.SearchIndexService"));
    }

    @Bean
    public SearchIndexServiceGrpc.SearchIndexServiceFutureStub searchSearchIndexServiceFuture(ChannelRegistry channels) {
        return SearchIndexServiceGrpc.newFutureStub(channels.lookup("acme.search.v1.SearchIndexService"));
    }

    @Bean
    public SearchIndexValidationServiceGrpc.SearchIndexValidationServiceStub searchSearchIndexValidationServiceAsync(ChannelRegistry channels) {
        return SearchIndexValidationServiceGrpc.newStub(channels.lookup("acme.search.v1.SearchIndexValidationService"));
    }

    @Bean
    public SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking(ChannelRegistry channels) {
        return SearchIndexValidationServiceGrpc.newBlockingStub(channels.lookup("acme.search.v1.SearchIndexValidationService"));
    }

    @Bean
    public SearchIndexValidationServiceGrpc.SearchIndexValidationServiceFutureStub searchSearchIndexValidationServiceFuture(ChannelRegistry channels) {
        return SearchIndexValidationServiceGrpc.newFutureStub(channels.lookup("acme.search.v1.SearchIndexValidationService"));
    }
}
