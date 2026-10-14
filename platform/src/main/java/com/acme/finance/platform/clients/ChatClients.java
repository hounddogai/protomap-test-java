package com.acme.finance.platform.clients;

import com.acme.chat.v1.ChatRoomCalculationServiceGrpc;
import com.acme.chat.v1.ChatRoomServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the chat contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ChatClients {
    @Bean
    public ChatRoomCalculationServiceGrpc.ChatRoomCalculationServiceBlockingStub chatChatRoomCalculationServiceBlocking(ChannelRegistry channels) {
        return ChatRoomCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.chat.v1.ChatRoomCalculationService"));
    }

    @Bean
    public ChatRoomServiceGrpc.ChatRoomServiceBlockingStub chatChatRoomServiceBlocking(ChannelRegistry channels) {
        return ChatRoomServiceGrpc.newBlockingStub(channels.lookup("acme.chat.v1.ChatRoomService"));
    }
}
