package com.acme.finance.domains.banking;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class StatementQueryApplicationEvents {
    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventService;

    public StatementQueryApplicationEvents(AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventService) {
        this.auditAuditEventService = auditAuditEventService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(StatementQueryApplication.ExportStatementsStage0Requested event) {
        event.context().run(() -> {
            try {
                auditAuditEventService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .searchAuditEvents(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
