package com.acme.finance.platform.clients;

import com.acme.audiences.v1.AudienceQueryServiceGrpc;
import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the audiences contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AudiencesClients {
    @Bean
    public AudienceQueryServiceGrpc.AudienceQueryServiceBlockingStub audiencesAudienceQueryServiceBlocking(ChannelRegistry channels) {
        return AudienceQueryServiceGrpc.newBlockingStub(channels.lookup("acme.audiences.v1.AudienceQueryService"));
    }

    @Bean
    public AudienceServiceGrpc.AudienceServiceBlockingStub audiencesAudienceServiceBlocking(ChannelRegistry channels) {
        return AudienceServiceGrpc.newBlockingStub(channels.lookup("acme.audiences.v1.AudienceService"));
    }
}
