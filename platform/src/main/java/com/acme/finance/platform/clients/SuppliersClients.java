package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierContractWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the suppliers contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class SuppliersClients {
    @Bean
    public SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking(ChannelRegistry channels) {
        return SupplierContractApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierContractApprovalService"));
    }

    @Bean
    public SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceFutureStub suppliersSupplierContractApprovalServiceFuture(ChannelRegistry channels) {
        return SupplierContractApprovalServiceGrpc.newFutureStub(channels.lookup("acme.suppliers.v1.SupplierContractApprovalService"));
    }

    @Bean
    public SupplierContractQueryServiceGrpc.SupplierContractQueryServiceStub suppliersSupplierContractQueryServiceAsync(ChannelRegistry channels) {
        return SupplierContractQueryServiceGrpc.newStub(channels.lookup("acme.suppliers.v1.SupplierContractQueryService"));
    }

    @Bean
    public SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking(ChannelRegistry channels) {
        return SupplierContractQueryServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierContractQueryService"));
    }

    @Bean
    public SupplierContractQueryServiceGrpc.SupplierContractQueryServiceFutureStub suppliersSupplierContractQueryServiceFuture(ChannelRegistry channels) {
        return SupplierContractQueryServiceGrpc.newFutureStub(channels.lookup("acme.suppliers.v1.SupplierContractQueryService"));
    }

    @Bean
    public SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceBlockingStub suppliersSupplierContractWorkflowServiceBlocking(ChannelRegistry channels) {
        return SupplierContractWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierContractWorkflowService"));
    }

    @Bean
    public SupplierContractWorkflowServiceGrpc.SupplierContractWorkflowServiceFutureStub suppliersSupplierContractWorkflowServiceFuture(ChannelRegistry channels) {
        return SupplierContractWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.suppliers.v1.SupplierContractWorkflowService"));
    }

    @Bean
    public SupplierQueryServiceGrpc.SupplierQueryServiceStub suppliersSupplierQueryServiceAsync(ChannelRegistry channels) {
        return SupplierQueryServiceGrpc.newStub(channels.lookup("acme.suppliers.v1.SupplierQueryService"));
    }

    @Bean
    public SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking(ChannelRegistry channels) {
        return SupplierQueryServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierQueryService"));
    }

    @Bean
    public SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking(ChannelRegistry channels) {
        return SupplierRatingApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierRatingApprovalService"));
    }

    @Bean
    public SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceStub suppliersSupplierRatingValidationServiceAsync(ChannelRegistry channels) {
        return SupplierRatingValidationServiceGrpc.newStub(channels.lookup("acme.suppliers.v1.SupplierRatingValidationService"));
    }

    @Bean
    public SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking(ChannelRegistry channels) {
        return SupplierRatingValidationServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierRatingValidationService"));
    }

    @Bean
    public SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceFutureStub suppliersSupplierRatingValidationServiceFuture(ChannelRegistry channels) {
        return SupplierRatingValidationServiceGrpc.newFutureStub(channels.lookup("acme.suppliers.v1.SupplierRatingValidationService"));
    }

    @Bean
    public SupplierRatingWorkflowServiceGrpc.SupplierRatingWorkflowServiceBlockingStub suppliersSupplierRatingWorkflowServiceBlocking(ChannelRegistry channels) {
        return SupplierRatingWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierRatingWorkflowService"));
    }

    @Bean
    public SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceBlockingStub suppliersSupplierRiskApprovalServiceBlocking(ChannelRegistry channels) {
        return SupplierRiskApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierRiskApprovalService"));
    }

    @Bean
    public SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceFutureStub suppliersSupplierRiskApprovalServiceFuture(ChannelRegistry channels) {
        return SupplierRiskApprovalServiceGrpc.newFutureStub(channels.lookup("acme.suppliers.v1.SupplierRiskApprovalService"));
    }

    @Bean
    public SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking(ChannelRegistry channels) {
        return SupplierRiskValidationServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierRiskValidationService"));
    }

    @Bean
    public SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync(ChannelRegistry channels) {
        return SupplierServiceGrpc.newStub(channels.lookup("acme.suppliers.v1.SupplierService"));
    }

    @Bean
    public SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking(ChannelRegistry channels) {
        return SupplierServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierService"));
    }

    @Bean
    public SupplierServiceGrpc.SupplierServiceFutureStub suppliersSupplierServiceFuture(ChannelRegistry channels) {
        return SupplierServiceGrpc.newFutureStub(channels.lookup("acme.suppliers.v1.SupplierService"));
    }

    @Bean
    public SupplierWorkflowServiceGrpc.SupplierWorkflowServiceBlockingStub suppliersSupplierWorkflowServiceBlocking(ChannelRegistry channels) {
        return SupplierWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.suppliers.v1.SupplierWorkflowService"));
    }
}
