package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.telehealth.v1.VideoVisitServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the telehealth contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class TelehealthClients {
    @Bean
    public VideoVisitServiceGrpc.VideoVisitServiceBlockingStub telehealthVideoVisitServiceBlocking(ChannelRegistry channels) {
        return VideoVisitServiceGrpc.newBlockingStub(channels.lookup("acme.telehealth.v1.VideoVisitService"));
    }
}
