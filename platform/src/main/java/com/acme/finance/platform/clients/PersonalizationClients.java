package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.personalization.v1.ProfileServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the personalization contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PersonalizationClients {
    @Bean
    public ProfileServiceGrpc.ProfileServiceBlockingStub personalizationProfileServiceBlocking(ChannelRegistry channels) {
        return ProfileServiceGrpc.newBlockingStub(channels.lookup("acme.personalization.v1.ProfileService"));
    }
}
