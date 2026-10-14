package com.acme.finance.domains.lending;

import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class LoanApplicationValidationApplicationEvents {
    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceStub auditRetentionPolicyValidationService;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceStub lendingLoanCalculationService;

    public LoanApplicationValidationApplicationEvents(RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceStub auditRetentionPolicyValidationService, LoanCalculationServiceGrpc.LoanCalculationServiceStub lendingLoanCalculationService) {
        this.auditRetentionPolicyValidationService = auditRetentionPolicyValidationService;
        this.lendingLoanCalculationService = lendingLoanCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LoanApplicationValidationApplication.PreviewLoanApplicationStage1Requested event) {
        event.context().run(() -> {
            try {
                auditRetentionPolicyValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .scoreRetentionPolicy(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LoanApplicationValidationApplication.PreviewLoanApplicationStage2Requested event) {
        event.context().run(() -> {
            try {
                lendingLoanCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .simulateLoan(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
