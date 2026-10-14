package com.acme.finance.domains.insurance;

import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ClaimLifecycleApplicationEvents {
    private final CustomerQueryServiceGrpc.CustomerQueryServiceStub customersCustomerQueryService;

    private final EInvoiceValidationServiceGrpc.EInvoiceValidationServiceStub invoicingEInvoiceValidationService;

    public ClaimLifecycleApplicationEvents(CustomerQueryServiceGrpc.CustomerQueryServiceStub customersCustomerQueryService, EInvoiceValidationServiceGrpc.EInvoiceValidationServiceStub invoicingEInvoiceValidationService) {
        this.customersCustomerQueryService = customersCustomerQueryService;
        this.invoicingEInvoiceValidationService = invoicingEInvoiceValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ClaimLifecycleApplication.SuspendClaimStage1Requested event) {
        event.context().run(() -> {
            try {
                invoicingEInvoiceValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainEInvoiceDecision(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ClaimLifecycleApplication.RenewClaimStage0Requested event) {
        event.context().run(() -> {
            try {
                customersCustomerQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .summarizeCustomers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
