package com.acme.finance.platform.clients;

import com.acme.community.v1.ForumQueryServiceGrpc;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the community contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CommunityClients {
    @Bean
    public ForumQueryServiceGrpc.ForumQueryServiceBlockingStub communityForumQueryServiceBlocking(ChannelRegistry channels) {
        return ForumQueryServiceGrpc.newBlockingStub(channels.lookup("acme.community.v1.ForumQueryService"));
    }

    @Bean
    public ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking(ChannelRegistry channels) {
        return ForumServiceGrpc.newBlockingStub(channels.lookup("acme.community.v1.ForumService"));
    }
}
