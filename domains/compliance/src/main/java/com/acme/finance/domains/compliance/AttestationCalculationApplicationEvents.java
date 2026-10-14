package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AttestationCalculationApplicationEvents {
    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceStub auditAuditTrailHistoryService;

    public AttestationCalculationApplicationEvents(AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceStub auditAuditTrailHistoryService) {
        this.auditAuditTrailHistoryService = auditAuditTrailHistoryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AttestationCalculationApplication.EstimateAttestationStage1Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailHistoryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .compareAuditTrailVersions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
