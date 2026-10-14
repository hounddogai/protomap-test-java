package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.ObligationApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RenewalWorkflowApplicationEvents {
    private final ObligationApprovalServiceGrpc.ObligationApprovalServiceStub contractsObligationApprovalService;

    public RenewalWorkflowApplicationEvents(ObligationApprovalServiceGrpc.ObligationApprovalServiceStub contractsObligationApprovalService) {
        this.contractsObligationApprovalService = contractsObligationApprovalService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RenewalWorkflowApplication.RetryRenewalWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                contractsObligationApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .rejectObligation(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
