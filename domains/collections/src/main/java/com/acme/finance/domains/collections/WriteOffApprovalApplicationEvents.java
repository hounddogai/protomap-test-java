package com.acme.finance.domains.collections;

import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class WriteOffApprovalApplicationEvents {
    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceStub bankingStandingOrderApprovalService;

    public WriteOffApprovalApplicationEvents(StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceStub bankingStandingOrderApprovalService) {
        this.bankingStandingOrderApprovalService = bankingStandingOrderApprovalService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(WriteOffApprovalApplication.ListPendingWriteOffsStage1Requested event) {
        event.context().run(() -> {
            try {
                bankingStandingOrderApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .escalateStandingOrder(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
