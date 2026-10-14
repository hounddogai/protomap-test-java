package com.acme.finance.domains.suppliers;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class SupplierContractQueryApplicationEvents {
    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceStub auditAuditEventNotificationService;

    public SupplierContractQueryApplicationEvents(AuditEventNotificationServiceGrpc.AuditEventNotificationServiceStub auditAuditEventNotificationService) {
        this.auditAuditEventNotificationService = auditAuditEventNotificationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(SupplierContractQueryApplication.SuggestSupplierContractsStage0Requested event) {
        event.context().run(() -> {
            try {
                auditAuditEventNotificationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .subscribeAuditEventUpdates(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
