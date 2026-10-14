package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.acme.messaging.v1.ConversationQueryServiceGrpc;
import com.acme.messaging.v1.ConversationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the messaging contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class MessagingClients {
    @Bean
    public ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking(ChannelRegistry channels) {
        return ConversationCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.messaging.v1.ConversationCalculationService"));
    }

    @Bean
    public ConversationQueryServiceGrpc.ConversationQueryServiceBlockingStub messagingConversationQueryServiceBlocking(ChannelRegistry channels) {
        return ConversationQueryServiceGrpc.newBlockingStub(channels.lookup("acme.messaging.v1.ConversationQueryService"));
    }

    @Bean
    public ConversationServiceGrpc.ConversationServiceBlockingStub messagingConversationServiceBlocking(ChannelRegistry channels) {
        return ConversationServiceGrpc.newBlockingStub(channels.lookup("acme.messaging.v1.ConversationService"));
    }
}
