package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.payroll.v1.DeductionApprovalServiceGrpc;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.DeductionWorkflowServiceGrpc;
import com.acme.payroll.v1.PayRunApprovalServiceGrpc;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayRunWorkflowServiceGrpc;
import com.acme.payroll.v1.PayStubApprovalServiceGrpc;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingApprovalServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingLifecycleServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the payroll contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class PayrollClients {
    @Bean
    public DeductionApprovalServiceGrpc.DeductionApprovalServiceBlockingStub payrollDeductionApprovalServiceBlocking(ChannelRegistry channels) {
        return DeductionApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.DeductionApprovalService"));
    }

    @Bean
    public DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking(ChannelRegistry channels) {
        return DeductionCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.DeductionCalculationService"));
    }

    @Bean
    public DeductionCalculationServiceGrpc.DeductionCalculationServiceFutureStub payrollDeductionCalculationServiceFuture(ChannelRegistry channels) {
        return DeductionCalculationServiceGrpc.newFutureStub(channels.lookup("acme.payroll.v1.DeductionCalculationService"));
    }

    @Bean
    public DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking(ChannelRegistry channels) {
        return DeductionLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.DeductionLifecycleService"));
    }

    @Bean
    public DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking(ChannelRegistry channels) {
        return DeductionReportingServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.DeductionReportingService"));
    }

    @Bean
    public DeductionValidationServiceGrpc.DeductionValidationServiceStub payrollDeductionValidationServiceAsync(ChannelRegistry channels) {
        return DeductionValidationServiceGrpc.newStub(channels.lookup("acme.payroll.v1.DeductionValidationService"));
    }

    @Bean
    public DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking(ChannelRegistry channels) {
        return DeductionValidationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.DeductionValidationService"));
    }

    @Bean
    public DeductionValidationServiceGrpc.DeductionValidationServiceFutureStub payrollDeductionValidationServiceFuture(ChannelRegistry channels) {
        return DeductionValidationServiceGrpc.newFutureStub(channels.lookup("acme.payroll.v1.DeductionValidationService"));
    }

    @Bean
    public DeductionWorkflowServiceGrpc.DeductionWorkflowServiceBlockingStub payrollDeductionWorkflowServiceBlocking(ChannelRegistry channels) {
        return DeductionWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.DeductionWorkflowService"));
    }

    @Bean
    public PayRunApprovalServiceGrpc.PayRunApprovalServiceBlockingStub payrollPayRunApprovalServiceBlocking(ChannelRegistry channels) {
        return PayRunApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayRunApprovalService"));
    }

    @Bean
    public PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking(ChannelRegistry channels) {
        return PayRunCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayRunCalculationService"));
    }

    @Bean
    public PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking(ChannelRegistry channels) {
        return PayRunQueryServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayRunQueryService"));
    }

    @Bean
    public PayRunQueryServiceGrpc.PayRunQueryServiceFutureStub payrollPayRunQueryServiceFuture(ChannelRegistry channels) {
        return PayRunQueryServiceGrpc.newFutureStub(channels.lookup("acme.payroll.v1.PayRunQueryService"));
    }

    @Bean
    public PayRunServiceGrpc.PayRunServiceStub payrollPayRunServiceAsync(ChannelRegistry channels) {
        return PayRunServiceGrpc.newStub(channels.lookup("acme.payroll.v1.PayRunService"));
    }

    @Bean
    public PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking(ChannelRegistry channels) {
        return PayRunServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayRunService"));
    }

    @Bean
    public PayRunServiceGrpc.PayRunServiceFutureStub payrollPayRunServiceFuture(ChannelRegistry channels) {
        return PayRunServiceGrpc.newFutureStub(channels.lookup("acme.payroll.v1.PayRunService"));
    }

    @Bean
    public PayRunValidationServiceGrpc.PayRunValidationServiceStub payrollPayRunValidationServiceAsync(ChannelRegistry channels) {
        return PayRunValidationServiceGrpc.newStub(channels.lookup("acme.payroll.v1.PayRunValidationService"));
    }

    @Bean
    public PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking(ChannelRegistry channels) {
        return PayRunValidationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayRunValidationService"));
    }

    @Bean
    public PayRunWorkflowServiceGrpc.PayRunWorkflowServiceBlockingStub payrollPayRunWorkflowServiceBlocking(ChannelRegistry channels) {
        return PayRunWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayRunWorkflowService"));
    }

    @Bean
    public PayStubApprovalServiceGrpc.PayStubApprovalServiceBlockingStub payrollPayStubApprovalServiceBlocking(ChannelRegistry channels) {
        return PayStubApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayStubApprovalService"));
    }

    @Bean
    public PayStubApprovalServiceGrpc.PayStubApprovalServiceFutureStub payrollPayStubApprovalServiceFuture(ChannelRegistry channels) {
        return PayStubApprovalServiceGrpc.newFutureStub(channels.lookup("acme.payroll.v1.PayStubApprovalService"));
    }

    @Bean
    public PayStubCalculationServiceGrpc.PayStubCalculationServiceStub payrollPayStubCalculationServiceAsync(ChannelRegistry channels) {
        return PayStubCalculationServiceGrpc.newStub(channels.lookup("acme.payroll.v1.PayStubCalculationService"));
    }

    @Bean
    public PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking(ChannelRegistry channels) {
        return PayStubCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayStubCalculationService"));
    }

    @Bean
    public PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking(ChannelRegistry channels) {
        return PayStubQueryServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayStubQueryService"));
    }

    @Bean
    public PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking(ChannelRegistry channels) {
        return PayStubValidationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayStubValidationService"));
    }

    @Bean
    public PayStubWorkflowServiceGrpc.PayStubWorkflowServiceBlockingStub payrollPayStubWorkflowServiceBlocking(ChannelRegistry channels) {
        return PayStubWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.PayStubWorkflowService"));
    }

    @Bean
    public TaxWithholdingApprovalServiceGrpc.TaxWithholdingApprovalServiceBlockingStub payrollTaxWithholdingApprovalServiceBlocking(ChannelRegistry channels) {
        return TaxWithholdingApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.TaxWithholdingApprovalService"));
    }

    @Bean
    public TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceStub payrollTaxWithholdingCalculationServiceAsync(ChannelRegistry channels) {
        return TaxWithholdingCalculationServiceGrpc.newStub(channels.lookup("acme.payroll.v1.TaxWithholdingCalculationService"));
    }

    @Bean
    public TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking(ChannelRegistry channels) {
        return TaxWithholdingCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.TaxWithholdingCalculationService"));
    }

    @Bean
    public TaxWithholdingLifecycleServiceGrpc.TaxWithholdingLifecycleServiceBlockingStub payrollTaxWithholdingLifecycleServiceBlocking(ChannelRegistry channels) {
        return TaxWithholdingLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.TaxWithholdingLifecycleService"));
    }

    @Bean
    public TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceStub payrollTaxWithholdingReportingServiceAsync(ChannelRegistry channels) {
        return TaxWithholdingReportingServiceGrpc.newStub(channels.lookup("acme.payroll.v1.TaxWithholdingReportingService"));
    }

    @Bean
    public TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking(ChannelRegistry channels) {
        return TaxWithholdingReportingServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.TaxWithholdingReportingService"));
    }

    @Bean
    public TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking(ChannelRegistry channels) {
        return TaxWithholdingValidationServiceGrpc.newBlockingStub(channels.lookup("acme.payroll.v1.TaxWithholdingValidationService"));
    }
}
