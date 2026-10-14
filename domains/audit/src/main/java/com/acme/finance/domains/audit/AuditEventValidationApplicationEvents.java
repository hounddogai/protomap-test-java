package com.acme.finance.domains.audit;

import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AuditEventValidationApplicationEvents {
    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncService;

    public AuditEventValidationApplicationEvents(RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncService) {
        this.auditRetentionPolicySyncService = auditRetentionPolicySyncService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AuditEventValidationApplication.GetAuditEventRulesStage0Requested event) {
        event.context().run(() -> {
            try {
                auditRetentionPolicySyncService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .syncRetentionPolicies(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
