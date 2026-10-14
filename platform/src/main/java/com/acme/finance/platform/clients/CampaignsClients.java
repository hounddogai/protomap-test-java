package com.acme.finance.platform.clients;

import com.acme.campaigns.v1.MarketingCampaignQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the campaigns contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CampaignsClients {
    @Bean
    public MarketingCampaignQueryServiceGrpc.MarketingCampaignQueryServiceBlockingStub campaignsMarketingCampaignQueryServiceBlocking(ChannelRegistry channels) {
        return MarketingCampaignQueryServiceGrpc.newBlockingStub(channels.lookup("acme.campaigns.v1.MarketingCampaignQueryService"));
    }
}
