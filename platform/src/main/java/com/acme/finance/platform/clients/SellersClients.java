package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.sellers.v1.SellerQueryServiceGrpc;
import com.acme.sellers.v1.SellerServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the sellers contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SellersClients {
    @Bean
    public SellerQueryServiceGrpc.SellerQueryServiceBlockingStub sellersSellerQueryServiceBlocking(ChannelRegistry channels) {
        return SellerQueryServiceGrpc.newBlockingStub(channels.lookup("acme.sellers.v1.SellerQueryService"));
    }

    @Bean
    public SellerServiceGrpc.SellerServiceBlockingStub sellersSellerServiceBlocking(ChannelRegistry channels) {
        return SellerServiceGrpc.newBlockingStub(channels.lookup("acme.sellers.v1.SellerService"));
    }
}
