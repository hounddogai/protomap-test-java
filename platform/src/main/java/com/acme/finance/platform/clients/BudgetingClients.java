package com.acme.finance.platform.clients;

import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLifecycleServiceGrpc;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineLifecycleServiceGrpc;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastLifecycleServiceGrpc;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.ForecastWorkflowServiceGrpc;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.VarianceLifecycleServiceGrpc;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the budgeting contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class BudgetingClients {
    @Bean
    public BudgetApprovalServiceGrpc.BudgetApprovalServiceStub budgetingBudgetApprovalServiceAsync(ChannelRegistry channels) {
        return BudgetApprovalServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.BudgetApprovalService"));
    }

    @Bean
    public BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking(ChannelRegistry channels) {
        return BudgetApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetApprovalService"));
    }

    @Bean
    public BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking(ChannelRegistry channels) {
        return BudgetCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetCalculationService"));
    }

    @Bean
    public BudgetCalculationServiceGrpc.BudgetCalculationServiceFutureStub budgetingBudgetCalculationServiceFuture(ChannelRegistry channels) {
        return BudgetCalculationServiceGrpc.newFutureStub(channels.lookup("acme.budgeting.v1.BudgetCalculationService"));
    }

    @Bean
    public BudgetLifecycleServiceGrpc.BudgetLifecycleServiceBlockingStub budgetingBudgetLifecycleServiceBlocking(ChannelRegistry channels) {
        return BudgetLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLifecycleService"));
    }

    @Bean
    public BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking(ChannelRegistry channels) {
        return BudgetLineApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLineApprovalService"));
    }

    @Bean
    public BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceStub budgetingBudgetLineCalculationServiceAsync(ChannelRegistry channels) {
        return BudgetLineCalculationServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.BudgetLineCalculationService"));
    }

    @Bean
    public BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking(ChannelRegistry channels) {
        return BudgetLineCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLineCalculationService"));
    }

    @Bean
    public BudgetLineLifecycleServiceGrpc.BudgetLineLifecycleServiceBlockingStub budgetingBudgetLineLifecycleServiceBlocking(ChannelRegistry channels) {
        return BudgetLineLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLineLifecycleService"));
    }

    @Bean
    public BudgetLineQueryServiceGrpc.BudgetLineQueryServiceStub budgetingBudgetLineQueryServiceAsync(ChannelRegistry channels) {
        return BudgetLineQueryServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.BudgetLineQueryService"));
    }

    @Bean
    public BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking(ChannelRegistry channels) {
        return BudgetLineQueryServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLineQueryService"));
    }

    @Bean
    public BudgetLineReportingServiceGrpc.BudgetLineReportingServiceStub budgetingBudgetLineReportingServiceAsync(ChannelRegistry channels) {
        return BudgetLineReportingServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.BudgetLineReportingService"));
    }

    @Bean
    public BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking(ChannelRegistry channels) {
        return BudgetLineReportingServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLineReportingService"));
    }

    @Bean
    public BudgetLineValidationServiceGrpc.BudgetLineValidationServiceStub budgetingBudgetLineValidationServiceAsync(ChannelRegistry channels) {
        return BudgetLineValidationServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.BudgetLineValidationService"));
    }

    @Bean
    public BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking(ChannelRegistry channels) {
        return BudgetLineValidationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLineValidationService"));
    }

    @Bean
    public BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceStub budgetingBudgetLineWorkflowServiceAsync(ChannelRegistry channels) {
        return BudgetLineWorkflowServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.BudgetLineWorkflowService"));
    }

    @Bean
    public BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceBlockingStub budgetingBudgetLineWorkflowServiceBlocking(ChannelRegistry channels) {
        return BudgetLineWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetLineWorkflowService"));
    }

    @Bean
    public BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking(ChannelRegistry channels) {
        return BudgetQueryServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetQueryService"));
    }

    @Bean
    public BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync(ChannelRegistry channels) {
        return BudgetServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.BudgetService"));
    }

    @Bean
    public BudgetServiceGrpc.BudgetServiceBlockingStub budgetingBudgetServiceBlocking(ChannelRegistry channels) {
        return BudgetServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetService"));
    }

    @Bean
    public BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking(ChannelRegistry channels) {
        return BudgetValidationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetValidationService"));
    }

    @Bean
    public BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking(ChannelRegistry channels) {
        return BudgetWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.BudgetWorkflowService"));
    }

    @Bean
    public ForecastApprovalServiceGrpc.ForecastApprovalServiceBlockingStub budgetingForecastApprovalServiceBlocking(ChannelRegistry channels) {
        return ForecastApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.ForecastApprovalService"));
    }

    @Bean
    public ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking(ChannelRegistry channels) {
        return ForecastCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.ForecastCalculationService"));
    }

    @Bean
    public ForecastCalculationServiceGrpc.ForecastCalculationServiceFutureStub budgetingForecastCalculationServiceFuture(ChannelRegistry channels) {
        return ForecastCalculationServiceGrpc.newFutureStub(channels.lookup("acme.budgeting.v1.ForecastCalculationService"));
    }

    @Bean
    public ForecastLifecycleServiceGrpc.ForecastLifecycleServiceBlockingStub budgetingForecastLifecycleServiceBlocking(ChannelRegistry channels) {
        return ForecastLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.ForecastLifecycleService"));
    }

    @Bean
    public ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking(ChannelRegistry channels) {
        return ForecastReportingServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.ForecastReportingService"));
    }

    @Bean
    public ForecastSyncServiceGrpc.ForecastSyncServiceStub budgetingForecastSyncServiceAsync(ChannelRegistry channels) {
        return ForecastSyncServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.ForecastSyncService"));
    }

    @Bean
    public ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking(ChannelRegistry channels) {
        return ForecastSyncServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.ForecastSyncService"));
    }

    @Bean
    public ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking(ChannelRegistry channels) {
        return ForecastValidationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.ForecastValidationService"));
    }

    @Bean
    public ForecastValidationServiceGrpc.ForecastValidationServiceFutureStub budgetingForecastValidationServiceFuture(ChannelRegistry channels) {
        return ForecastValidationServiceGrpc.newFutureStub(channels.lookup("acme.budgeting.v1.ForecastValidationService"));
    }

    @Bean
    public ForecastWorkflowServiceGrpc.ForecastWorkflowServiceStub budgetingForecastWorkflowServiceAsync(ChannelRegistry channels) {
        return ForecastWorkflowServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.ForecastWorkflowService"));
    }

    @Bean
    public ForecastWorkflowServiceGrpc.ForecastWorkflowServiceBlockingStub budgetingForecastWorkflowServiceBlocking(ChannelRegistry channels) {
        return ForecastWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.ForecastWorkflowService"));
    }

    @Bean
    public VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking(ChannelRegistry channels) {
        return VarianceApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.VarianceApprovalService"));
    }

    @Bean
    public VarianceCalculationServiceGrpc.VarianceCalculationServiceStub budgetingVarianceCalculationServiceAsync(ChannelRegistry channels) {
        return VarianceCalculationServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.VarianceCalculationService"));
    }

    @Bean
    public VarianceCalculationServiceGrpc.VarianceCalculationServiceBlockingStub budgetingVarianceCalculationServiceBlocking(ChannelRegistry channels) {
        return VarianceCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.VarianceCalculationService"));
    }

    @Bean
    public VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync(ChannelRegistry channels) {
        return VarianceImportServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.VarianceImportService"));
    }

    @Bean
    public VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking(ChannelRegistry channels) {
        return VarianceImportServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.VarianceImportService"));
    }

    @Bean
    public VarianceLifecycleServiceGrpc.VarianceLifecycleServiceBlockingStub budgetingVarianceLifecycleServiceBlocking(ChannelRegistry channels) {
        return VarianceLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.VarianceLifecycleService"));
    }

    @Bean
    public VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking(ChannelRegistry channels) {
        return VarianceReportingServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.VarianceReportingService"));
    }

    @Bean
    public VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync(ChannelRegistry channels) {
        return VarianceSyncServiceGrpc.newStub(channels.lookup("acme.budgeting.v1.VarianceSyncService"));
    }

    @Bean
    public VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking(ChannelRegistry channels) {
        return VarianceSyncServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.VarianceSyncService"));
    }

    @Bean
    public VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking(ChannelRegistry channels) {
        return VarianceValidationServiceGrpc.newBlockingStub(channels.lookup("acme.budgeting.v1.VarianceValidationService"));
    }
}
