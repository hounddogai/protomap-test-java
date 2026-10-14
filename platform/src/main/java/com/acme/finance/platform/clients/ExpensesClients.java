package com.acme.finance.platform.clients;

import com.acme.expenses.v1.ExpensePolicyApprovalServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.expenses.v1.MileageClaimApprovalServiceGrpc;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the expenses contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ExpensesClients {
    @Bean
    public ExpensePolicyApprovalServiceGrpc.ExpensePolicyApprovalServiceBlockingStub expensesExpensePolicyApprovalServiceBlocking(ChannelRegistry channels) {
        return ExpensePolicyApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpensePolicyApprovalService"));
    }

    @Bean
    public ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking(ChannelRegistry channels) {
        return ExpensePolicyCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpensePolicyCalculationService"));
    }

    @Bean
    public ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking(ChannelRegistry channels) {
        return ExpensePolicyLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpensePolicyLifecycleService"));
    }

    @Bean
    public ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceFutureStub expensesExpensePolicyLifecycleServiceFuture(ChannelRegistry channels) {
        return ExpensePolicyLifecycleServiceGrpc.newFutureStub(channels.lookup("acme.expenses.v1.ExpensePolicyLifecycleService"));
    }

    @Bean
    public ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceBlockingStub expensesExpensePolicyReportingServiceBlocking(ChannelRegistry channels) {
        return ExpensePolicyReportingServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpensePolicyReportingService"));
    }

    @Bean
    public ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceStub expensesExpensePolicySyncServiceAsync(ChannelRegistry channels) {
        return ExpensePolicySyncServiceGrpc.newStub(channels.lookup("acme.expenses.v1.ExpensePolicySyncService"));
    }

    @Bean
    public ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking(ChannelRegistry channels) {
        return ExpensePolicySyncServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpensePolicySyncService"));
    }

    @Bean
    public ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking(ChannelRegistry channels) {
        return ExpensePolicyValidationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpensePolicyValidationService"));
    }

    @Bean
    public ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking(ChannelRegistry channels) {
        return ExpenseReportApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpenseReportApprovalService"));
    }

    @Bean
    public ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceStub expensesExpenseReportCalculationServiceAsync(ChannelRegistry channels) {
        return ExpenseReportCalculationServiceGrpc.newStub(channels.lookup("acme.expenses.v1.ExpenseReportCalculationService"));
    }

    @Bean
    public ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking(ChannelRegistry channels) {
        return ExpenseReportCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpenseReportCalculationService"));
    }

    @Bean
    public ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceFutureStub expensesExpenseReportCalculationServiceFuture(ChannelRegistry channels) {
        return ExpenseReportCalculationServiceGrpc.newFutureStub(channels.lookup("acme.expenses.v1.ExpenseReportCalculationService"));
    }

    @Bean
    public ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking(ChannelRegistry channels) {
        return ExpenseReportQueryServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpenseReportQueryService"));
    }

    @Bean
    public ExpenseReportServiceGrpc.ExpenseReportServiceStub expensesExpenseReportServiceAsync(ChannelRegistry channels) {
        return ExpenseReportServiceGrpc.newStub(channels.lookup("acme.expenses.v1.ExpenseReportService"));
    }

    @Bean
    public ExpenseReportServiceGrpc.ExpenseReportServiceBlockingStub expensesExpenseReportServiceBlocking(ChannelRegistry channels) {
        return ExpenseReportServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpenseReportService"));
    }

    @Bean
    public ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking(ChannelRegistry channels) {
        return ExpenseReportValidationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpenseReportValidationService"));
    }

    @Bean
    public ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceFutureStub expensesExpenseReportValidationServiceFuture(ChannelRegistry channels) {
        return ExpenseReportValidationServiceGrpc.newFutureStub(channels.lookup("acme.expenses.v1.ExpenseReportValidationService"));
    }

    @Bean
    public ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceBlockingStub expensesExpenseReportWorkflowServiceBlocking(ChannelRegistry channels) {
        return ExpenseReportWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ExpenseReportWorkflowService"));
    }

    @Bean
    public MileageClaimApprovalServiceGrpc.MileageClaimApprovalServiceBlockingStub expensesMileageClaimApprovalServiceBlocking(ChannelRegistry channels) {
        return MileageClaimApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.MileageClaimApprovalService"));
    }

    @Bean
    public MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking(ChannelRegistry channels) {
        return MileageClaimCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.MileageClaimCalculationService"));
    }

    @Bean
    public MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceStub expensesMileageClaimLifecycleServiceAsync(ChannelRegistry channels) {
        return MileageClaimLifecycleServiceGrpc.newStub(channels.lookup("acme.expenses.v1.MileageClaimLifecycleService"));
    }

    @Bean
    public MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceBlockingStub expensesMileageClaimLifecycleServiceBlocking(ChannelRegistry channels) {
        return MileageClaimLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.MileageClaimLifecycleService"));
    }

    @Bean
    public MileageClaimReportingServiceGrpc.MileageClaimReportingServiceBlockingStub expensesMileageClaimReportingServiceBlocking(ChannelRegistry channels) {
        return MileageClaimReportingServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.MileageClaimReportingService"));
    }

    @Bean
    public MileageClaimReportingServiceGrpc.MileageClaimReportingServiceFutureStub expensesMileageClaimReportingServiceFuture(ChannelRegistry channels) {
        return MileageClaimReportingServiceGrpc.newFutureStub(channels.lookup("acme.expenses.v1.MileageClaimReportingService"));
    }

    @Bean
    public MileageClaimValidationServiceGrpc.MileageClaimValidationServiceBlockingStub expensesMileageClaimValidationServiceBlocking(ChannelRegistry channels) {
        return MileageClaimValidationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.MileageClaimValidationService"));
    }

    @Bean
    public MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceBlockingStub expensesMileageClaimWorkflowServiceBlocking(ChannelRegistry channels) {
        return MileageClaimWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.MileageClaimWorkflowService"));
    }

    @Bean
    public ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking(ChannelRegistry channels) {
        return ReceiptApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ReceiptApprovalService"));
    }

    @Bean
    public ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking(ChannelRegistry channels) {
        return ReceiptCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ReceiptCalculationService"));
    }

    @Bean
    public ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceBlockingStub expensesReceiptLifecycleServiceBlocking(ChannelRegistry channels) {
        return ReceiptLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ReceiptLifecycleService"));
    }

    @Bean
    public ReceiptQueryServiceGrpc.ReceiptQueryServiceBlockingStub expensesReceiptQueryServiceBlocking(ChannelRegistry channels) {
        return ReceiptQueryServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ReceiptQueryService"));
    }

    @Bean
    public ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking(ChannelRegistry channels) {
        return ReceiptValidationServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ReceiptValidationService"));
    }

    @Bean
    public ReceiptValidationServiceGrpc.ReceiptValidationServiceFutureStub expensesReceiptValidationServiceFuture(ChannelRegistry channels) {
        return ReceiptValidationServiceGrpc.newFutureStub(channels.lookup("acme.expenses.v1.ReceiptValidationService"));
    }

    @Bean
    public ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking(ChannelRegistry channels) {
        return ReceiptWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.expenses.v1.ReceiptWorkflowService"));
    }
}
