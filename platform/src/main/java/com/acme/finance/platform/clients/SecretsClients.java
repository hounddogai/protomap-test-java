package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.secrets.v1.SecretQueryServiceGrpc;
import com.acme.secrets.v1.SecretServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the secrets contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SecretsClients {
    @Bean
    public SecretQueryServiceGrpc.SecretQueryServiceBlockingStub secretsSecretQueryServiceBlocking(ChannelRegistry channels) {
        return SecretQueryServiceGrpc.newBlockingStub(channels.lookup("acme.secrets.v1.SecretQueryService"));
    }

    @Bean
    public SecretServiceGrpc.SecretServiceBlockingStub secretsSecretServiceBlocking(ChannelRegistry channels) {
        return SecretServiceGrpc.newBlockingStub(channels.lookup("acme.secrets.v1.SecretService"));
    }
}
