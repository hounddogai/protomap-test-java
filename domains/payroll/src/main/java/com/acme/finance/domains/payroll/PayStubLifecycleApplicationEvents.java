package com.acme.finance.domains.payroll;

import com.acme.audit.v1.RetentionPolicyAdminServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PayStubLifecycleApplicationEvents {
    private final RetentionPolicyAdminServiceGrpc.RetentionPolicyAdminServiceStub auditRetentionPolicyAdminService;

    public PayStubLifecycleApplicationEvents(RetentionPolicyAdminServiceGrpc.RetentionPolicyAdminServiceStub auditRetentionPolicyAdminService) {
        this.auditRetentionPolicyAdminService = auditRetentionPolicyAdminService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PayStubLifecycleApplication.TerminatePayStubStage1Requested event) {
        event.context().run(() -> {
            try {
                auditRetentionPolicyAdminService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .repairRetentionPolicy(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
