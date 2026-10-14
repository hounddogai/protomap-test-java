package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AttestationReportingApplicationEvents {
    private final AuditTrailAdminServiceGrpc.AuditTrailAdminServiceStub auditAuditTrailAdminService;

    public AttestationReportingApplicationEvents(AuditTrailAdminServiceGrpc.AuditTrailAdminServiceStub auditAuditTrailAdminService) {
        this.auditAuditTrailAdminService = auditAuditTrailAdminService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AttestationReportingApplication.CountAttestationTotalsStage0Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailAdminService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .repairAuditTrail(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
