package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.forms.v1.FormServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the forms contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FormsClients {
    @Bean
    public FormQueryServiceGrpc.FormQueryServiceBlockingStub formsFormQueryServiceBlocking(ChannelRegistry channels) {
        return FormQueryServiceGrpc.newBlockingStub(channels.lookup("acme.forms.v1.FormQueryService"));
    }

    @Bean
    public FormServiceGrpc.FormServiceBlockingStub formsFormServiceBlocking(ChannelRegistry channels) {
        return FormServiceGrpc.newBlockingStub(channels.lookup("acme.forms.v1.FormService"));
    }
}
