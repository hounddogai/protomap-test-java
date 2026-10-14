package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.payments.v1.PaymentCalculationServiceGrpc;
import com.acme.payments.v1.PaymentQueryServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the payments contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PaymentsClients {
    @Bean
    public PaymentCalculationServiceGrpc.PaymentCalculationServiceStub paymentsPaymentCalculationServiceAsync(ChannelRegistry channels) {
        return PaymentCalculationServiceGrpc.newStub(channels.lookup("acme.payments.v1.PaymentCalculationService"));
    }

    @Bean
    public PaymentCalculationServiceGrpc.PaymentCalculationServiceBlockingStub paymentsPaymentCalculationServiceBlocking(ChannelRegistry channels) {
        return PaymentCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.payments.v1.PaymentCalculationService"));
    }

    @Bean
    public PaymentQueryServiceGrpc.PaymentQueryServiceBlockingStub paymentsPaymentQueryServiceBlocking(ChannelRegistry channels) {
        return PaymentQueryServiceGrpc.newBlockingStub(channels.lookup("acme.payments.v1.PaymentQueryService"));
    }
}
