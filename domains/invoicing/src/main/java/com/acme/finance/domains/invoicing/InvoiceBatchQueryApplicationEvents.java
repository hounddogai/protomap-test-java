package com.acme.finance.domains.invoicing;

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
public class InvoiceBatchQueryApplicationEvents {
    private final EInvoiceValidationServiceGrpc.EInvoiceValidationServiceStub invoicingEInvoiceValidationService;

    public InvoiceBatchQueryApplicationEvents(EInvoiceValidationServiceGrpc.EInvoiceValidationServiceStub invoicingEInvoiceValidationService) {
        this.invoicingEInvoiceValidationService = invoicingEInvoiceValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceBatchQueryApplication.ExportInvoiceBatchesStage0Requested event) {
        event.context().run(() -> {
            try {
                invoicingEInvoiceValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validateEInvoice(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
