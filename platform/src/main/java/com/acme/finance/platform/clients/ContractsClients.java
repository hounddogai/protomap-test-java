package com.acme.finance.platform.clients;

import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.ObligationApprovalServiceGrpc;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.RenewalApprovalServiceGrpc;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the contracts contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ContractsClients {
    @Bean
    public ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking(ChannelRegistry channels) {
        return ContractApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ContractApprovalService"));
    }

    @Bean
    public ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking(ChannelRegistry channels) {
        return ContractClauseApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ContractClauseApprovalService"));
    }

    @Bean
    public ContractClauseQueryServiceGrpc.ContractClauseQueryServiceStub contractsContractClauseQueryServiceAsync(ChannelRegistry channels) {
        return ContractClauseQueryServiceGrpc.newStub(channels.lookup("acme.contracts.v1.ContractClauseQueryService"));
    }

    @Bean
    public ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking(ChannelRegistry channels) {
        return ContractClauseQueryServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ContractClauseQueryService"));
    }

    @Bean
    public ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking(ChannelRegistry channels) {
        return ContractClauseWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ContractClauseWorkflowService"));
    }

    @Bean
    public ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking(ChannelRegistry channels) {
        return ContractQueryServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ContractQueryService"));
    }

    @Bean
    public ContractQueryServiceGrpc.ContractQueryServiceFutureStub contractsContractQueryServiceFuture(ChannelRegistry channels) {
        return ContractQueryServiceGrpc.newFutureStub(channels.lookup("acme.contracts.v1.ContractQueryService"));
    }

    @Bean
    public ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync(ChannelRegistry channels) {
        return ContractServiceGrpc.newStub(channels.lookup("acme.contracts.v1.ContractService"));
    }

    @Bean
    public ContractServiceGrpc.ContractServiceBlockingStub contractsContractServiceBlocking(ChannelRegistry channels) {
        return ContractServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ContractService"));
    }

    @Bean
    public ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking(ChannelRegistry channels) {
        return ContractWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ContractWorkflowService"));
    }

    @Bean
    public ObligationApprovalServiceGrpc.ObligationApprovalServiceStub contractsObligationApprovalServiceAsync(ChannelRegistry channels) {
        return ObligationApprovalServiceGrpc.newStub(channels.lookup("acme.contracts.v1.ObligationApprovalService"));
    }

    @Bean
    public ObligationApprovalServiceGrpc.ObligationApprovalServiceBlockingStub contractsObligationApprovalServiceBlocking(ChannelRegistry channels) {
        return ObligationApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ObligationApprovalService"));
    }

    @Bean
    public ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking(ChannelRegistry channels) {
        return ObligationCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ObligationCalculationService"));
    }

    @Bean
    public ObligationCalculationServiceGrpc.ObligationCalculationServiceFutureStub contractsObligationCalculationServiceFuture(ChannelRegistry channels) {
        return ObligationCalculationServiceGrpc.newFutureStub(channels.lookup("acme.contracts.v1.ObligationCalculationService"));
    }

    @Bean
    public ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking(ChannelRegistry channels) {
        return ObligationValidationServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.ObligationValidationService"));
    }

    @Bean
    public RenewalApprovalServiceGrpc.RenewalApprovalServiceBlockingStub contractsRenewalApprovalServiceBlocking(ChannelRegistry channels) {
        return RenewalApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.RenewalApprovalService"));
    }

    @Bean
    public RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking(ChannelRegistry channels) {
        return RenewalValidationServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.RenewalValidationService"));
    }

    @Bean
    public RenewalValidationServiceGrpc.RenewalValidationServiceFutureStub contractsRenewalValidationServiceFuture(ChannelRegistry channels) {
        return RenewalValidationServiceGrpc.newFutureStub(channels.lookup("acme.contracts.v1.RenewalValidationService"));
    }

    @Bean
    public RenewalWorkflowServiceGrpc.RenewalWorkflowServiceBlockingStub contractsRenewalWorkflowServiceBlocking(ChannelRegistry channels) {
        return RenewalWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.contracts.v1.RenewalWorkflowService"));
    }

    @Bean
    public RenewalWorkflowServiceGrpc.RenewalWorkflowServiceFutureStub contractsRenewalWorkflowServiceFuture(ChannelRegistry channels) {
        return RenewalWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.contracts.v1.RenewalWorkflowService"));
    }
}
