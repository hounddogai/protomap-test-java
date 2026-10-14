package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CoverageCalculationApplicationEvents {
    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceStub auditAuditTrailExportService;

    public CoverageCalculationApplicationEvents(AuditTrailExportServiceGrpc.AuditTrailExportServiceStub auditAuditTrailExportService) {
        this.auditAuditTrailExportService = auditAuditTrailExportService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CoverageCalculationApplication.CalculateCoverageStage1Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailExportService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listAuditTrailExports(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
