package com.acme.finance.domains.accounting;

import com.acme.expenses.v1.MileageClaimLifecycleServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ReconciliationLifecycleApplicationEvents {
    private final MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceStub expensesMileageClaimLifecycleService;

    private final ExposureWorkflowServiceGrpc.ExposureWorkflowServiceStub riskExposureWorkflowService;

    public ReconciliationLifecycleApplicationEvents(MileageClaimLifecycleServiceGrpc.MileageClaimLifecycleServiceStub expensesMileageClaimLifecycleService, ExposureWorkflowServiceGrpc.ExposureWorkflowServiceStub riskExposureWorkflowService) {
        this.expensesMileageClaimLifecycleService = expensesMileageClaimLifecycleService;
        this.riskExposureWorkflowService = riskExposureWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReconciliationLifecycleApplication.RenewReconciliationStage0Requested event) {
        event.context().run(() -> {
            try {
                expensesMileageClaimLifecycleService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .scheduleMileageClaimExpiry(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReconciliationLifecycleApplication.ExpireReconciliationStage1Requested event) {
        event.context().run(() -> {
            try {
                riskExposureWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .advanceExposureWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
