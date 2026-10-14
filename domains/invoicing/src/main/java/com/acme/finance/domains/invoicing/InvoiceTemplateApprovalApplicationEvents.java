package com.acme.finance.domains.invoicing;

import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceTemplateApprovalApplicationEvents {
    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceStub customersHouseholdCalculationService;

    public InvoiceTemplateApprovalApplicationEvents(HouseholdCalculationServiceGrpc.HouseholdCalculationServiceStub customersHouseholdCalculationService) {
        this.customersHouseholdCalculationService = customersHouseholdCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceTemplateApprovalApplication.RejectInvoiceTemplateStage1Requested event) {
        event.context().run(() -> {
            try {
                customersHouseholdCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainHouseholdCalculation(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
