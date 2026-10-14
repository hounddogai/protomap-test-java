package com.acme.finance.domains.accounting;

import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ChartOfAccountsValidationApplicationEvents {
    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceStub auditAuditEventValidationService;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceStub auditAuditTrailValidationService;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceStub ledgerJournalEntryHistoryService;

    public ChartOfAccountsValidationApplicationEvents(AuditEventValidationServiceGrpc.AuditEventValidationServiceStub auditAuditEventValidationService, AuditTrailValidationServiceGrpc.AuditTrailValidationServiceStub auditAuditTrailValidationService, JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceStub ledgerJournalEntryHistoryService) {
        this.auditAuditEventValidationService = auditAuditEventValidationService;
        this.auditAuditTrailValidationService = auditAuditTrailValidationService;
        this.ledgerJournalEntryHistoryService = ledgerJournalEntryHistoryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ChartOfAccountsValidationApplication.ExplainChartOfAccountsDecisionStage1Requested event) {
        event.context().run(() -> {
            try {
                auditAuditEventValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainAuditEventDecision(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ChartOfAccountsValidationApplication.ExplainChartOfAccountsDecisionStage2Requested event) {
        event.context().run(() -> {
            try {
                ledgerJournalEntryHistoryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listJournalEntryVersions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ChartOfAccountsValidationApplication.ScoreChartOfAccountsStage2Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .scoreAuditTrail(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
