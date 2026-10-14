package com.acme.finance.domains.expenses;

import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ExpensePolicyCalculationApplicationEvents {
    private final MandateValidationServiceGrpc.MandateValidationServiceStub bankingMandateValidationService;

    public ExpensePolicyCalculationApplicationEvents(MandateValidationServiceGrpc.MandateValidationServiceStub bankingMandateValidationService) {
        this.bankingMandateValidationService = bankingMandateValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ExpensePolicyCalculationApplication.RecalculateExpensePoliciesStage1Requested event) {
        event.context().run(() -> {
            try {
                bankingMandateValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .scoreMandate(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
