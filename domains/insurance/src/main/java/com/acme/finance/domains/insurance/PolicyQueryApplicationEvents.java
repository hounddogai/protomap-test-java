package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PolicyQueryApplicationEvents {
    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceStub auditAuditTrailHistoryService;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceStub insurancePremiumValidationService;

    public PolicyQueryApplicationEvents(AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceStub auditAuditTrailHistoryService, PremiumValidationServiceGrpc.PremiumValidationServiceStub insurancePremiumValidationService) {
        this.auditAuditTrailHistoryService = auditAuditTrailHistoryService;
        this.insurancePremiumValidationService = insurancePremiumValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PolicyQueryApplication.SummarizePoliciesStage0Requested event) {
        event.context().run(() -> {
            try {
                insurancePremiumValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkPremiumEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PolicyQueryApplication.SummarizePoliciesStage1Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailHistoryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .revertAuditTrail(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
