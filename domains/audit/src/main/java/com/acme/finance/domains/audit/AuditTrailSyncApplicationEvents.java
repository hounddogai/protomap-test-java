package com.acme.finance.domains.audit;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AuditTrailSyncApplicationEvents {
    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceStub ledgerJournalEntryValidationService;

    public AuditTrailSyncApplicationEvents(JournalEntryValidationServiceGrpc.JournalEntryValidationServiceStub ledgerJournalEntryValidationService) {
        this.ledgerJournalEntryValidationService = ledgerJournalEntryValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AuditTrailSyncApplication.GetAuditTrailSyncStateStage0Requested event) {
        event.context().run(() -> {
            try {
                ledgerJournalEntryValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .normalizeJournalEntry(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
