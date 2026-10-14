package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RfqCalculationApplicationEvents {
    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceStub ledgerJournalEntryHistoryService;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceStub riskRiskScenarioQueryService;

    public RfqCalculationApplicationEvents(JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceStub ledgerJournalEntryHistoryService, RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceStub riskRiskScenarioQueryService) {
        this.ledgerJournalEntryHistoryService = ledgerJournalEntryHistoryService;
        this.riskRiskScenarioQueryService = riskRiskScenarioQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RfqCalculationApplication.CalculateRfqStage2Requested event) {
        event.context().run(() -> {
            try {
                riskRiskScenarioQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .countRiskScenarios(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RfqCalculationApplication.SimulateRfqStage0Requested event) {
        event.context().run(() -> {
            try {
                ledgerJournalEntryHistoryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .annotateJournalEntryVersion(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
