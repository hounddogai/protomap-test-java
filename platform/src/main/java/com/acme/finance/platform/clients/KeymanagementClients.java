package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.keymanagement.v1.CryptoKeyQueryServiceGrpc;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the keymanagement contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class KeymanagementClients {
    @Bean
    public CryptoKeyQueryServiceGrpc.CryptoKeyQueryServiceBlockingStub keymanagementCryptoKeyQueryServiceBlocking(ChannelRegistry channels) {
        return CryptoKeyQueryServiceGrpc.newBlockingStub(channels.lookup("acme.keymanagement.v1.CryptoKeyQueryService"));
    }

    @Bean
    public CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking(ChannelRegistry channels) {
        return CryptoKeyServiceGrpc.newBlockingStub(channels.lookup("acme.keymanagement.v1.CryptoKeyService"));
    }
}
