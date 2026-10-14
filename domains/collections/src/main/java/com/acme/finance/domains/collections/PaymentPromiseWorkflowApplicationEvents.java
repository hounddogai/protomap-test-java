package com.acme.finance.domains.collections;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PaymentPromiseWorkflowApplicationEvents {
    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceStub ledgerJournalEntryNotificationService;

    public PaymentPromiseWorkflowApplicationEvents(JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceStub ledgerJournalEntryNotificationService) {
        this.ledgerJournalEntryNotificationService = ledgerJournalEntryNotificationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PaymentPromiseWorkflowApplication.InspectPaymentPromiseWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                ledgerJournalEntryNotificationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .sendJournalEntryTestNotice(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
