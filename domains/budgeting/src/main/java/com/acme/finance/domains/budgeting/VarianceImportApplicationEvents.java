package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class VarianceImportApplicationEvents {
    private final BudgetLineQueryServiceGrpc.BudgetLineQueryServiceStub budgetingBudgetLineQueryService;

    public VarianceImportApplicationEvents(BudgetLineQueryServiceGrpc.BudgetLineQueryServiceStub budgetingBudgetLineQueryService) {
        this.budgetingBudgetLineQueryService = budgetingBudgetLineQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(VarianceImportApplication.MapVarianceImportColumnsStage0Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetLineQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .resolveBudgetLines(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
