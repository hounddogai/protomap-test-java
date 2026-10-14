package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.UserValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the identity contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class IdentityClients {
    @Bean
    public UserQueryServiceGrpc.UserQueryServiceStub identityUserQueryServiceAsync(ChannelRegistry channels) {
        return UserQueryServiceGrpc.newStub(channels.lookup("acme.identity.v1.UserQueryService"));
    }

    @Bean
    public UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking(ChannelRegistry channels) {
        return UserQueryServiceGrpc.newBlockingStub(channels.lookup("acme.identity.v1.UserQueryService"));
    }

    @Bean
    public UserServiceGrpc.UserServiceStub identityUserServiceAsync(ChannelRegistry channels) {
        return UserServiceGrpc.newStub(channels.lookup("acme.identity.v1.UserService"));
    }

    @Bean
    public UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking(ChannelRegistry channels) {
        return UserServiceGrpc.newBlockingStub(channels.lookup("acme.identity.v1.UserService"));
    }

    @Bean
    public UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking(ChannelRegistry channels) {
        return UserValidationServiceGrpc.newBlockingStub(channels.lookup("acme.identity.v1.UserValidationService"));
    }

    @Bean
    public UserValidationServiceGrpc.UserValidationServiceFutureStub identityUserValidationServiceFuture(ChannelRegistry channels) {
        return UserValidationServiceGrpc.newFutureStub(channels.lookup("acme.identity.v1.UserValidationService"));
    }
}
