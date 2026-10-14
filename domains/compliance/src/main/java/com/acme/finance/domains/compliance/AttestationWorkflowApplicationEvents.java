package com.acme.finance.domains.compliance;

import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AttestationWorkflowApplicationEvents {
    private final AccessPolicyServiceGrpc.AccessPolicyServiceStub authorizationAccessPolicyService;

    public AttestationWorkflowApplicationEvents(AccessPolicyServiceGrpc.AccessPolicyServiceStub authorizationAccessPolicyService) {
        this.authorizationAccessPolicyService = authorizationAccessPolicyService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AttestationWorkflowApplication.SignalAttestationWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                authorizationAccessPolicyService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .cloneAccessPolicy(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
