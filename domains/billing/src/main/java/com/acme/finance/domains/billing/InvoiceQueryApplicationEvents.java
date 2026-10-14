package com.acme.finance.domains.billing;

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
public class InvoiceQueryApplicationEvents {
    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceStub budgetingBudgetLineValidationService;

    public InvoiceQueryApplicationEvents(BudgetLineValidationServiceGrpc.BudgetLineValidationServiceStub budgetingBudgetLineValidationService) {
        this.budgetingBudgetLineValidationService = budgetingBudgetLineValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceQueryApplication.LookupInvoiceStage0Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetLineValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .normalizeBudgetLine(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
