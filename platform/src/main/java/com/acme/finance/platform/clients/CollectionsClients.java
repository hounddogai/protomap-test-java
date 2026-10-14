package com.acme.finance.platform.clients;

import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the collections contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CollectionsClients {
    @Bean
    public CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceStub collectionsCollectionCaseApprovalServiceAsync(ChannelRegistry channels) {
        return CollectionCaseApprovalServiceGrpc.newStub(channels.lookup("acme.collections.v1.CollectionCaseApprovalService"));
    }

    @Bean
    public CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking(ChannelRegistry channels) {
        return CollectionCaseApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.CollectionCaseApprovalService"));
    }

    @Bean
    public CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceFutureStub collectionsCollectionCaseApprovalServiceFuture(ChannelRegistry channels) {
        return CollectionCaseApprovalServiceGrpc.newFutureStub(channels.lookup("acme.collections.v1.CollectionCaseApprovalService"));
    }

    @Bean
    public CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking(ChannelRegistry channels) {
        return CollectionCaseQueryServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.CollectionCaseQueryService"));
    }

    @Bean
    public CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync(ChannelRegistry channels) {
        return CollectionCaseServiceGrpc.newStub(channels.lookup("acme.collections.v1.CollectionCaseService"));
    }

    @Bean
    public CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking(ChannelRegistry channels) {
        return CollectionCaseServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.CollectionCaseService"));
    }

    @Bean
    public CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking(ChannelRegistry channels) {
        return CollectionCaseWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.CollectionCaseWorkflowService"));
    }

    @Bean
    public CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceFutureStub collectionsCollectionCaseWorkflowServiceFuture(ChannelRegistry channels) {
        return CollectionCaseWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.collections.v1.CollectionCaseWorkflowService"));
    }

    @Bean
    public PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking(ChannelRegistry channels) {
        return PaymentPromiseApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.PaymentPromiseApprovalService"));
    }

    @Bean
    public PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceFutureStub collectionsPaymentPromiseApprovalServiceFuture(ChannelRegistry channels) {
        return PaymentPromiseApprovalServiceGrpc.newFutureStub(channels.lookup("acme.collections.v1.PaymentPromiseApprovalService"));
    }

    @Bean
    public PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceBlockingStub collectionsPaymentPromiseQueryServiceBlocking(ChannelRegistry channels) {
        return PaymentPromiseQueryServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.PaymentPromiseQueryService"));
    }

    @Bean
    public PaymentPromiseQueryServiceGrpc.PaymentPromiseQueryServiceFutureStub collectionsPaymentPromiseQueryServiceFuture(ChannelRegistry channels) {
        return PaymentPromiseQueryServiceGrpc.newFutureStub(channels.lookup("acme.collections.v1.PaymentPromiseQueryService"));
    }

    @Bean
    public PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceStub collectionsPaymentPromiseWorkflowServiceAsync(ChannelRegistry channels) {
        return PaymentPromiseWorkflowServiceGrpc.newStub(channels.lookup("acme.collections.v1.PaymentPromiseWorkflowService"));
    }

    @Bean
    public PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking(ChannelRegistry channels) {
        return PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.PaymentPromiseWorkflowService"));
    }

    @Bean
    public WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking(ChannelRegistry channels) {
        return WriteOffApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.WriteOffApprovalService"));
    }

    @Bean
    public WriteOffValidationServiceGrpc.WriteOffValidationServiceStub collectionsWriteOffValidationServiceAsync(ChannelRegistry channels) {
        return WriteOffValidationServiceGrpc.newStub(channels.lookup("acme.collections.v1.WriteOffValidationService"));
    }

    @Bean
    public WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking(ChannelRegistry channels) {
        return WriteOffValidationServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.WriteOffValidationService"));
    }

    @Bean
    public WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking(ChannelRegistry channels) {
        return WriteOffWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.collections.v1.WriteOffWorkflowService"));
    }
}
