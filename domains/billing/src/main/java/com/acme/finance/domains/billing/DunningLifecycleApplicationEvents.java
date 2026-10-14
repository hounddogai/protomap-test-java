package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DunningLifecycleApplicationEvents {
    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventService;

    private final DunningReportingServiceGrpc.DunningReportingServiceStub billingDunningReportingService;

    public DunningLifecycleApplicationEvents(AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventService, DunningReportingServiceGrpc.DunningReportingServiceStub billingDunningReportingService) {
        this.auditAuditEventService = auditAuditEventService;
        this.billingDunningReportingService = billingDunningReportingService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DunningLifecycleApplication.ActivateDunningStage0Requested event) {
        event.context().run(() -> {
            try {
                auditAuditEventService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .batchGetAuditEvents(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DunningLifecycleApplication.TerminateDunningStage0Requested event) {
        event.context().run(() -> {
            try {
                billingDunningReportingService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .fetchDunningReport(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
