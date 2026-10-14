package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceTemplateCalculationApplicationEvents {
    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceStub ledgerJournalEntryQueryService;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceStub payrollPayStubCalculationService;

    public InvoiceTemplateCalculationApplicationEvents(JournalEntryQueryServiceGrpc.JournalEntryQueryServiceStub ledgerJournalEntryQueryService, PayStubCalculationServiceGrpc.PayStubCalculationServiceStub payrollPayStubCalculationService) {
        this.ledgerJournalEntryQueryService = ledgerJournalEntryQueryService;
        this.payrollPayStubCalculationService = payrollPayStubCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceTemplateCalculationApplication.EstimateInvoiceTemplateStage1Requested event) {
        event.context().run(() -> {
            try {
                payrollPayStubCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .estimatePayStub(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceTemplateCalculationApplication.ExplainInvoiceTemplateCalculationStage0Requested event) {
        event.context().run(() -> {
            try {
                ledgerJournalEntryQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .browseJournalEntries(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
