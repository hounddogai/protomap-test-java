package com.acme.finance.domains.procurement;

import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PurchaseOrderWorkflowApplicationEvents {
    private final DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceStub assetsDepreciationWorkflowService;

    public PurchaseOrderWorkflowApplicationEvents(DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceStub assetsDepreciationWorkflowService) {
        this.assetsDepreciationWorkflowService = assetsDepreciationWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PurchaseOrderWorkflowApplication.StartPurchaseOrderWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                assetsDepreciationWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .pauseDepreciationWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
