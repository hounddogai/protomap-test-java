package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceCalculationApplicationEvents {
    private final AuditEventExportServiceGrpc.AuditEventExportServiceStub auditAuditEventExportService;

    public InvoiceCalculationApplicationEvents(AuditEventExportServiceGrpc.AuditEventExportServiceStub auditAuditEventExportService) {
        this.auditAuditEventExportService = auditAuditEventExportService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceCalculationApplication.ExplainInvoiceCalculationStage1Requested event) {
        event.context().run(() -> {
            try {
                auditAuditEventExportService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listAuditEventExports(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
