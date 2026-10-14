package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.returns.v1.ReturnRequestCalculationServiceGrpc;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the returns contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ReturnsClients {
    @Bean
    public ReturnRequestCalculationServiceGrpc.ReturnRequestCalculationServiceBlockingStub returnsReturnRequestCalculationServiceBlocking(ChannelRegistry channels) {
        return ReturnRequestCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.returns.v1.ReturnRequestCalculationService"));
    }

    @Bean
    public ReturnRequestServiceGrpc.ReturnRequestServiceBlockingStub returnsReturnRequestServiceBlocking(ChannelRegistry channels) {
        return ReturnRequestServiceGrpc.newBlockingStub(channels.lookup("acme.returns.v1.ReturnRequestService"));
    }
}
