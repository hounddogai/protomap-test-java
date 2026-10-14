package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.surveys.v1.SurveyQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the surveys contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SurveysClients {
    @Bean
    public SurveyQueryServiceGrpc.SurveyQueryServiceBlockingStub surveysSurveyQueryServiceBlocking(ChannelRegistry channels) {
        return SurveyQueryServiceGrpc.newBlockingStub(channels.lookup("acme.surveys.v1.SurveyQueryService"));
    }
}
