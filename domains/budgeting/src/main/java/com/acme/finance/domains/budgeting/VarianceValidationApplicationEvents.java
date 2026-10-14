package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class VarianceValidationApplicationEvents {
    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceStub budgetingBudgetLineValidationService;

    public VarianceValidationApplicationEvents(BudgetLineValidationServiceGrpc.BudgetLineValidationServiceStub budgetingBudgetLineValidationService) {
        this.budgetingBudgetLineValidationService = budgetingBudgetLineValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(VarianceValidationApplication.ExplainVarianceDecisionStage2Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetLineValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewBudgetLine(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
