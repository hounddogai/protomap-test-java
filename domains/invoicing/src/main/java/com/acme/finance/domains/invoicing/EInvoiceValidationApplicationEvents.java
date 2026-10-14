package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class EInvoiceValidationApplicationEvents {
    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceStub invoicingInvoiceBatchCalculationService;

    public EInvoiceValidationApplicationEvents(InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceStub invoicingInvoiceBatchCalculationService) {
        this.invoicingInvoiceBatchCalculationService = invoicingInvoiceBatchCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(EInvoiceValidationApplication.NormalizeEInvoiceStage0Requested event) {
        event.context().run(() -> {
            try {
                invoicingInvoiceBatchCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .quoteInvoiceBatch(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
