package com.acme.finance.platform.clients;

import com.acme.content.v1.ArticleQueryServiceGrpc;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the content contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ContentClients {
    @Bean
    public ArticleQueryServiceGrpc.ArticleQueryServiceBlockingStub contentArticleQueryServiceBlocking(ChannelRegistry channels) {
        return ArticleQueryServiceGrpc.newBlockingStub(channels.lookup("acme.content.v1.ArticleQueryService"));
    }

    @Bean
    public ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking(ChannelRegistry channels) {
        return ArticleServiceGrpc.newBlockingStub(channels.lookup("acme.content.v1.ArticleService"));
    }

    @Bean
    public ArticleServiceGrpc.ArticleServiceFutureStub contentArticleServiceFuture(ChannelRegistry channels) {
        return ArticleServiceGrpc.newFutureStub(channels.lookup("acme.content.v1.ArticleService"));
    }
}
