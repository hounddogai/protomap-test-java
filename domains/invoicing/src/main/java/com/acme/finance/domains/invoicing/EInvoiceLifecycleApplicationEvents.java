package com.acme.finance.domains.invoicing;

import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class EInvoiceLifecycleApplicationEvents {
    private final ControlWorkflowServiceGrpc.ControlWorkflowServiceStub complianceControlWorkflowService;

    public EInvoiceLifecycleApplicationEvents(ControlWorkflowServiceGrpc.ControlWorkflowServiceStub complianceControlWorkflowService) {
        this.complianceControlWorkflowService = complianceControlWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(EInvoiceLifecycleApplication.ReinstateEInvoiceStage2Requested event) {
        event.context().run(() -> {
            try {
                complianceControlWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .cancelControlWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
