package com.acme.finance.platform.clients;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the authorization contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AuthorizationClients {
    @Bean
    public AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceStub authorizationAccessPolicyQueryServiceAsync(ChannelRegistry channels) {
        return AccessPolicyQueryServiceGrpc.newStub(channels.lookup("acme.authorization.v1.AccessPolicyQueryService"));
    }

    @Bean
    public AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking(ChannelRegistry channels) {
        return AccessPolicyQueryServiceGrpc.newBlockingStub(channels.lookup("acme.authorization.v1.AccessPolicyQueryService"));
    }

    @Bean
    public AccessPolicyServiceGrpc.AccessPolicyServiceStub authorizationAccessPolicyServiceAsync(ChannelRegistry channels) {
        return AccessPolicyServiceGrpc.newStub(channels.lookup("acme.authorization.v1.AccessPolicyService"));
    }

    @Bean
    public AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking(ChannelRegistry channels) {
        return AccessPolicyServiceGrpc.newBlockingStub(channels.lookup("acme.authorization.v1.AccessPolicyService"));
    }

    @Bean
    public AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking(ChannelRegistry channels) {
        return AccessPolicyValidationServiceGrpc.newBlockingStub(channels.lookup("acme.authorization.v1.AccessPolicyValidationService"));
    }
}
