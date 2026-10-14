package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.fraud.v1.RiskAssessmentServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the fraud contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FraudClients {
    @Bean
    public RiskAssessmentServiceGrpc.RiskAssessmentServiceFutureStub fraudRiskAssessmentServiceFuture(ChannelRegistry channels) {
        return RiskAssessmentServiceGrpc.newFutureStub(channels.lookup("acme.fraud.v1.RiskAssessmentService"));
    }
}
