package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class EInvoiceCalculationApplicationEvents {
    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceStub invoicingInvoiceBatchValidationService;

    public EInvoiceCalculationApplicationEvents(InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceStub invoicingInvoiceBatchValidationService) {
        this.invoicingInvoiceBatchValidationService = invoicingInvoiceBatchValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(EInvoiceCalculationApplication.EstimateEInvoiceStage0Requested event) {
        event.context().run(() -> {
            try {
                invoicingInvoiceBatchValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkInvoiceBatchEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
