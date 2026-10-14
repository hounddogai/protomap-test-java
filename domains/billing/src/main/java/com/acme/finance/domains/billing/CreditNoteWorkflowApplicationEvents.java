package com.acme.finance.domains.billing;

import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CreditNoteWorkflowApplicationEvents {
    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceStub billingCreditNoteCalculationService;

    private final CustomerServiceGrpc.CustomerServiceStub customersCustomerService;

    public CreditNoteWorkflowApplicationEvents(CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceStub billingCreditNoteCalculationService, CustomerServiceGrpc.CustomerServiceStub customersCustomerService) {
        this.billingCreditNoteCalculationService = billingCreditNoteCalculationService;
        this.customersCustomerService = customersCustomerService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CreditNoteWorkflowApplication.PauseCreditNoteWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                billingCreditNoteCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .getCreditNoteRates(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CreditNoteWorkflowApplication.InspectCreditNoteWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                customersCustomerService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .batchGetCustomers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
