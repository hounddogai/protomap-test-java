package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceTemplateLifecycleApplicationEvents {
    private final InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceStub invoicingInvoiceTemplateQueryService;

    public InvoiceTemplateLifecycleApplicationEvents(InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceStub invoicingInvoiceTemplateQueryService) {
        this.invoicingInvoiceTemplateQueryService = invoicingInvoiceTemplateQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceTemplateLifecycleApplication.ExpireInvoiceTemplateStage0Requested event) {
        event.context().run(() -> {
            try {
                invoicingInvoiceTemplateQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .resolveInvoiceTemplates(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
