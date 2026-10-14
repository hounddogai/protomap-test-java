package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.giftcards.v1.GiftCardCalculationServiceGrpc;
import com.acme.giftcards.v1.GiftCardQueryServiceGrpc;
import com.acme.giftcards.v1.GiftCardServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the giftcards contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class GiftcardsClients {
    @Bean
    public GiftCardCalculationServiceGrpc.GiftCardCalculationServiceBlockingStub giftcardsGiftCardCalculationServiceBlocking(ChannelRegistry channels) {
        return GiftCardCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.giftcards.v1.GiftCardCalculationService"));
    }

    @Bean
    public GiftCardQueryServiceGrpc.GiftCardQueryServiceBlockingStub giftcardsGiftCardQueryServiceBlocking(ChannelRegistry channels) {
        return GiftCardQueryServiceGrpc.newBlockingStub(channels.lookup("acme.giftcards.v1.GiftCardQueryService"));
    }

    @Bean
    public GiftCardServiceGrpc.GiftCardServiceBlockingStub giftcardsGiftCardServiceBlocking(ChannelRegistry channels) {
        return GiftCardServiceGrpc.newBlockingStub(channels.lookup("acme.giftcards.v1.GiftCardService"));
    }
}
