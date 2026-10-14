package com.acme.finance.domains.contracts;

import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ObligationApprovalApplicationEvents {
    private final BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceStub budgetingBudgetLineWorkflowService;

    public ObligationApprovalApplicationEvents(BudgetLineWorkflowServiceGrpc.BudgetLineWorkflowServiceStub budgetingBudgetLineWorkflowService) {
        this.budgetingBudgetLineWorkflowService = budgetingBudgetLineWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ObligationApprovalApplication.EscalateObligationStage3Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetLineWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .retryBudgetLineWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
