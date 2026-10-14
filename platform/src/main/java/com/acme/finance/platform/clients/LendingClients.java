package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralLifecycleServiceGrpc;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the lending contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class LendingClients {
    @Bean
    public CollateralApprovalServiceGrpc.CollateralApprovalServiceBlockingStub lendingCollateralApprovalServiceBlocking(ChannelRegistry channels) {
        return CollateralApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.CollateralApprovalService"));
    }

    @Bean
    public CollateralCalculationServiceGrpc.CollateralCalculationServiceStub lendingCollateralCalculationServiceAsync(ChannelRegistry channels) {
        return CollateralCalculationServiceGrpc.newStub(channels.lookup("acme.lending.v1.CollateralCalculationService"));
    }

    @Bean
    public CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking(ChannelRegistry channels) {
        return CollateralCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.CollateralCalculationService"));
    }

    @Bean
    public CollateralLifecycleServiceGrpc.CollateralLifecycleServiceBlockingStub lendingCollateralLifecycleServiceBlocking(ChannelRegistry channels) {
        return CollateralLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.CollateralLifecycleService"));
    }

    @Bean
    public CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking(ChannelRegistry channels) {
        return CollateralValidationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.CollateralValidationService"));
    }

    @Bean
    public CollateralValidationServiceGrpc.CollateralValidationServiceFutureStub lendingCollateralValidationServiceFuture(ChannelRegistry channels) {
        return CollateralValidationServiceGrpc.newFutureStub(channels.lookup("acme.lending.v1.CollateralValidationService"));
    }

    @Bean
    public CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking(ChannelRegistry channels) {
        return CollateralWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.CollateralWorkflowService"));
    }

    @Bean
    public CollateralWorkflowServiceGrpc.CollateralWorkflowServiceFutureStub lendingCollateralWorkflowServiceFuture(ChannelRegistry channels) {
        return CollateralWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.lending.v1.CollateralWorkflowService"));
    }

    @Bean
    public LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking(ChannelRegistry channels) {
        return LoanApplicationApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanApplicationApprovalService"));
    }

    @Bean
    public LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking(ChannelRegistry channels) {
        return LoanApplicationCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanApplicationCalculationService"));
    }

    @Bean
    public LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceStub lendingLoanApplicationQueryServiceAsync(ChannelRegistry channels) {
        return LoanApplicationQueryServiceGrpc.newStub(channels.lookup("acme.lending.v1.LoanApplicationQueryService"));
    }

    @Bean
    public LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking(ChannelRegistry channels) {
        return LoanApplicationQueryServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanApplicationQueryService"));
    }

    @Bean
    public LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceFutureStub lendingLoanApplicationQueryServiceFuture(ChannelRegistry channels) {
        return LoanApplicationQueryServiceGrpc.newFutureStub(channels.lookup("acme.lending.v1.LoanApplicationQueryService"));
    }

    @Bean
    public LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking(ChannelRegistry channels) {
        return LoanApplicationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanApplicationService"));
    }

    @Bean
    public LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceStub lendingLoanApplicationValidationServiceAsync(ChannelRegistry channels) {
        return LoanApplicationValidationServiceGrpc.newStub(channels.lookup("acme.lending.v1.LoanApplicationValidationService"));
    }

    @Bean
    public LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking(ChannelRegistry channels) {
        return LoanApplicationValidationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanApplicationValidationService"));
    }

    @Bean
    public LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking(ChannelRegistry channels) {
        return LoanApplicationWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanApplicationWorkflowService"));
    }

    @Bean
    public LoanApprovalServiceGrpc.LoanApprovalServiceStub lendingLoanApprovalServiceAsync(ChannelRegistry channels) {
        return LoanApprovalServiceGrpc.newStub(channels.lookup("acme.lending.v1.LoanApprovalService"));
    }

    @Bean
    public LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking(ChannelRegistry channels) {
        return LoanApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanApprovalService"));
    }

    @Bean
    public LoanCalculationServiceGrpc.LoanCalculationServiceStub lendingLoanCalculationServiceAsync(ChannelRegistry channels) {
        return LoanCalculationServiceGrpc.newStub(channels.lookup("acme.lending.v1.LoanCalculationService"));
    }

    @Bean
    public LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking(ChannelRegistry channels) {
        return LoanCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanCalculationService"));
    }

    @Bean
    public LoanCalculationServiceGrpc.LoanCalculationServiceFutureStub lendingLoanCalculationServiceFuture(ChannelRegistry channels) {
        return LoanCalculationServiceGrpc.newFutureStub(channels.lookup("acme.lending.v1.LoanCalculationService"));
    }

    @Bean
    public LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking(ChannelRegistry channels) {
        return LoanLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanLifecycleService"));
    }

    @Bean
    public LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking(ChannelRegistry channels) {
        return LoanQueryServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanQueryService"));
    }

    @Bean
    public LoanQueryServiceGrpc.LoanQueryServiceFutureStub lendingLoanQueryServiceFuture(ChannelRegistry channels) {
        return LoanQueryServiceGrpc.newFutureStub(channels.lookup("acme.lending.v1.LoanQueryService"));
    }

    @Bean
    public LoanValidationServiceGrpc.LoanValidationServiceStub lendingLoanValidationServiceAsync(ChannelRegistry channels) {
        return LoanValidationServiceGrpc.newStub(channels.lookup("acme.lending.v1.LoanValidationService"));
    }

    @Bean
    public LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking(ChannelRegistry channels) {
        return LoanValidationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanValidationService"));
    }

    @Bean
    public LoanValidationServiceGrpc.LoanValidationServiceFutureStub lendingLoanValidationServiceFuture(ChannelRegistry channels) {
        return LoanValidationServiceGrpc.newFutureStub(channels.lookup("acme.lending.v1.LoanValidationService"));
    }

    @Bean
    public LoanWorkflowServiceGrpc.LoanWorkflowServiceBlockingStub lendingLoanWorkflowServiceBlocking(ChannelRegistry channels) {
        return LoanWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.LoanWorkflowService"));
    }

    @Bean
    public RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking(ChannelRegistry channels) {
        return RepaymentApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.RepaymentApprovalService"));
    }

    @Bean
    public RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking(ChannelRegistry channels) {
        return RepaymentCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.RepaymentCalculationService"));
    }

    @Bean
    public RepaymentLifecycleServiceGrpc.RepaymentLifecycleServiceBlockingStub lendingRepaymentLifecycleServiceBlocking(ChannelRegistry channels) {
        return RepaymentLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.RepaymentLifecycleService"));
    }

    @Bean
    public RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking(ChannelRegistry channels) {
        return RepaymentReportingServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.RepaymentReportingService"));
    }

    @Bean
    public RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking(ChannelRegistry channels) {
        return RepaymentValidationServiceGrpc.newBlockingStub(channels.lookup("acme.lending.v1.RepaymentValidationService"));
    }
}
