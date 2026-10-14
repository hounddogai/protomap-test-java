package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.webbff.v1.PageCompositionServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the webbff contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class WebbffClients {
    @Bean
    public PageCompositionServiceGrpc.PageCompositionServiceBlockingStub webbffPageCompositionServiceBlocking(ChannelRegistry channels) {
        return PageCompositionServiceGrpc.newBlockingStub(channels.lookup("acme.webbff.v1.PageCompositionService"));
    }
}
