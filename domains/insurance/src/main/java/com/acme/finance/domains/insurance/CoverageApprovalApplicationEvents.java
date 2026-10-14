package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CoverageApprovalApplicationEvents {
    private final AccrualValidationServiceGrpc.AccrualValidationServiceStub accountingAccrualValidationService;

    private final StressTestValidationServiceGrpc.StressTestValidationServiceStub riskStressTestValidationService;

    public CoverageApprovalApplicationEvents(AccrualValidationServiceGrpc.AccrualValidationServiceStub accountingAccrualValidationService, StressTestValidationServiceGrpc.StressTestValidationServiceStub riskStressTestValidationService) {
        this.accountingAccrualValidationService = accountingAccrualValidationService;
        this.riskStressTestValidationService = riskStressTestValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CoverageApprovalApplication.ApproveCoverageStage0Requested event) {
        event.context().run(() -> {
            try {
                riskStressTestValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkStressTestEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CoverageApprovalApplication.ListPendingCoveragesStage0Requested event) {
        event.context().run(() -> {
            try {
                accountingAccrualValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkAccrualEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
