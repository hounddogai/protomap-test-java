package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ApprovalInboxWorkflowApplicationEvents {
    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceStub auditAuditTrailSyncService;

    public ApprovalInboxWorkflowApplicationEvents(AuditTrailSyncServiceGrpc.AuditTrailSyncServiceStub auditAuditTrailSyncService) {
        this.auditAuditTrailSyncService = auditAuditTrailSyncService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ApprovalInboxWorkflowApplication.PauseApprovalInboxWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailSyncService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listAuditTrailConflicts(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
