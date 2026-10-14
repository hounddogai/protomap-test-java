package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.loyalty.v1.LoyaltyMemberCalculationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the loyalty contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LoyaltyClients {
    @Bean
    public LoyaltyMemberCalculationServiceGrpc.LoyaltyMemberCalculationServiceBlockingStub loyaltyLoyaltyMemberCalculationServiceBlocking(ChannelRegistry channels) {
        return LoyaltyMemberCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.loyalty.v1.LoyaltyMemberCalculationService"));
    }
}
