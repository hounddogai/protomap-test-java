package com.acme.finance.domains.collections;

import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CollectionCaseApprovalApplicationEvents {
    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceStub auditRetentionPolicyNotificationService;

    public CollectionCaseApprovalApplicationEvents(RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceStub auditRetentionPolicyNotificationService) {
        this.auditRetentionPolicyNotificationService = auditRetentionPolicyNotificationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CollectionCaseApprovalApplication.ApproveCollectionCaseStage0Requested event) {
        event.context().run(() -> {
            try {
                auditRetentionPolicyNotificationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .subscribeRetentionPolicyUpdates(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
