package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ForecastWorkflowApplicationEvents {
    private final BudgetApprovalServiceGrpc.BudgetApprovalServiceStub budgetingBudgetApprovalService;

    public ForecastWorkflowApplicationEvents(BudgetApprovalServiceGrpc.BudgetApprovalServiceStub budgetingBudgetApprovalService) {
        this.budgetingBudgetApprovalService = budgetingBudgetApprovalService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ForecastWorkflowApplication.CancelForecastWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .escalateBudget(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
