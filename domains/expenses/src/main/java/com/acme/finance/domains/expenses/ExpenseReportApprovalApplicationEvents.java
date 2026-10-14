package com.acme.finance.domains.expenses;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.media.v1.MediaAssetCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ExpenseReportApprovalApplicationEvents {
    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceStub ledgerLedgerAccountQueryService;

    private final MediaAssetCalculationServiceGrpc.MediaAssetCalculationServiceStub mediaMediaAssetCalculationService;

    public ExpenseReportApprovalApplicationEvents(LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceStub ledgerLedgerAccountQueryService, MediaAssetCalculationServiceGrpc.MediaAssetCalculationServiceStub mediaMediaAssetCalculationService) {
        this.ledgerLedgerAccountQueryService = ledgerLedgerAccountQueryService;
        this.mediaMediaAssetCalculationService = mediaMediaAssetCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ExpenseReportApprovalApplication.EscalateExpenseReportStage0Requested event) {
        event.context().run(() -> {
            try {
                ledgerLedgerAccountQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .findLedgerAccounts(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ExpenseReportApprovalApplication.RecallExpenseReportStage1Requested event) {
        event.context().run(() -> {
            try {
                mediaMediaAssetCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainMediaAssetCalculation(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
