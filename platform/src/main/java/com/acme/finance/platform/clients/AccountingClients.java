package com.acme.finance.platform.clients;

import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.AllocationWorkflowServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsWorkflowServiceGrpc;
import com.acme.accounting.v1.ReconciliationApprovalServiceGrpc;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationLifecycleServiceGrpc;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the accounting contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AccountingClients {
    @Bean
    public AccrualApprovalServiceGrpc.AccrualApprovalServiceBlockingStub accountingAccrualApprovalServiceBlocking(ChannelRegistry channels) {
        return AccrualApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.AccrualApprovalService"));
    }

    @Bean
    public AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking(ChannelRegistry channels) {
        return AccrualQueryServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.AccrualQueryService"));
    }

    @Bean
    public AccrualValidationServiceGrpc.AccrualValidationServiceStub accountingAccrualValidationServiceAsync(ChannelRegistry channels) {
        return AccrualValidationServiceGrpc.newStub(channels.lookup("acme.accounting.v1.AccrualValidationService"));
    }

    @Bean
    public AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking(ChannelRegistry channels) {
        return AccrualValidationServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.AccrualValidationService"));
    }

    @Bean
    public AccrualValidationServiceGrpc.AccrualValidationServiceFutureStub accountingAccrualValidationServiceFuture(ChannelRegistry channels) {
        return AccrualValidationServiceGrpc.newFutureStub(channels.lookup("acme.accounting.v1.AccrualValidationService"));
    }

    @Bean
    public AllocationApprovalServiceGrpc.AllocationApprovalServiceBlockingStub accountingAllocationApprovalServiceBlocking(ChannelRegistry channels) {
        return AllocationApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.AllocationApprovalService"));
    }

    @Bean
    public AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking(ChannelRegistry channels) {
        return AllocationCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.AllocationCalculationService"));
    }

    @Bean
    public AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking(ChannelRegistry channels) {
        return AllocationValidationServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.AllocationValidationService"));
    }

    @Bean
    public AllocationWorkflowServiceGrpc.AllocationWorkflowServiceBlockingStub accountingAllocationWorkflowServiceBlocking(ChannelRegistry channels) {
        return AllocationWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.AllocationWorkflowService"));
    }

    @Bean
    public ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceStub accountingChartOfAccountsApprovalServiceAsync(ChannelRegistry channels) {
        return ChartOfAccountsApprovalServiceGrpc.newStub(channels.lookup("acme.accounting.v1.ChartOfAccountsApprovalService"));
    }

    @Bean
    public ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking(ChannelRegistry channels) {
        return ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ChartOfAccountsApprovalService"));
    }

    @Bean
    public ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceBlockingStub accountingChartOfAccountsQueryServiceBlocking(ChannelRegistry channels) {
        return ChartOfAccountsQueryServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ChartOfAccountsQueryService"));
    }

    @Bean
    public ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync(ChannelRegistry channels) {
        return ChartOfAccountsServiceGrpc.newStub(channels.lookup("acme.accounting.v1.ChartOfAccountsService"));
    }

    @Bean
    public ChartOfAccountsServiceGrpc.ChartOfAccountsServiceBlockingStub accountingChartOfAccountsServiceBlocking(ChannelRegistry channels) {
        return ChartOfAccountsServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ChartOfAccountsService"));
    }

    @Bean
    public ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking(ChannelRegistry channels) {
        return ChartOfAccountsValidationServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ChartOfAccountsValidationService"));
    }

    @Bean
    public ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceFutureStub accountingChartOfAccountsValidationServiceFuture(ChannelRegistry channels) {
        return ChartOfAccountsValidationServiceGrpc.newFutureStub(channels.lookup("acme.accounting.v1.ChartOfAccountsValidationService"));
    }

    @Bean
    public ChartOfAccountsWorkflowServiceGrpc.ChartOfAccountsWorkflowServiceFutureStub accountingChartOfAccountsWorkflowServiceFuture(ChannelRegistry channels) {
        return ChartOfAccountsWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.accounting.v1.ChartOfAccountsWorkflowService"));
    }

    @Bean
    public ReconciliationApprovalServiceGrpc.ReconciliationApprovalServiceBlockingStub accountingReconciliationApprovalServiceBlocking(ChannelRegistry channels) {
        return ReconciliationApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ReconciliationApprovalService"));
    }

    @Bean
    public ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking(ChannelRegistry channels) {
        return ReconciliationCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ReconciliationCalculationService"));
    }

    @Bean
    public ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceFutureStub accountingReconciliationCalculationServiceFuture(ChannelRegistry channels) {
        return ReconciliationCalculationServiceGrpc.newFutureStub(channels.lookup("acme.accounting.v1.ReconciliationCalculationService"));
    }

    @Bean
    public ReconciliationLifecycleServiceGrpc.ReconciliationLifecycleServiceBlockingStub accountingReconciliationLifecycleServiceBlocking(ChannelRegistry channels) {
        return ReconciliationLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ReconciliationLifecycleService"));
    }

    @Bean
    public ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking(ChannelRegistry channels) {
        return ReconciliationValidationServiceGrpc.newBlockingStub(channels.lookup("acme.accounting.v1.ReconciliationValidationService"));
    }

    @Bean
    public ReconciliationValidationServiceGrpc.ReconciliationValidationServiceFutureStub accountingReconciliationValidationServiceFuture(ChannelRegistry channels) {
        return ReconciliationValidationServiceGrpc.newFutureStub(channels.lookup("acme.accounting.v1.ReconciliationValidationService"));
    }
}
