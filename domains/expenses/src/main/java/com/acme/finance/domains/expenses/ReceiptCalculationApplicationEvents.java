package com.acme.finance.domains.expenses;

import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ReceiptCalculationApplicationEvents {
    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceStub auditAuditTrailValidationService;

    public ReceiptCalculationApplicationEvents(AuditTrailValidationServiceGrpc.AuditTrailValidationServiceStub auditAuditTrailValidationService) {
        this.auditAuditTrailValidationService = auditAuditTrailValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReceiptCalculationApplication.ExplainReceiptCalculationStage1Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkAuditTrailEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
