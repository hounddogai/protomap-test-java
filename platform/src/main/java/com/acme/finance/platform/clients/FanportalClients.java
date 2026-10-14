package com.acme.finance.platform.clients;

import com.acme.fanportal.v1.FanSessionQueryServiceGrpc;
import com.acme.fanportal.v1.FanSessionServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the fanportal contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FanportalClients {
    @Bean
    public FanSessionQueryServiceGrpc.FanSessionQueryServiceStub fanportalFanSessionQueryServiceAsync(ChannelRegistry channels) {
        return FanSessionQueryServiceGrpc.newStub(channels.lookup("acme.fanportal.v1.FanSessionQueryService"));
    }

    @Bean
    public FanSessionServiceGrpc.FanSessionServiceBlockingStub fanportalFanSessionServiceBlocking(ChannelRegistry channels) {
        return FanSessionServiceGrpc.newBlockingStub(channels.lookup("acme.fanportal.v1.FanSessionService"));
    }
}
