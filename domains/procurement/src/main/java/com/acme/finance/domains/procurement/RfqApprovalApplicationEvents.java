package com.acme.finance.domains.procurement;

import com.acme.budgeting.v1.ForecastWorkflowServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RfqApprovalApplicationEvents {
    private final ForecastWorkflowServiceGrpc.ForecastWorkflowServiceStub budgetingForecastWorkflowService;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceStub ledgerLedgerAccountNotificationService;

    public RfqApprovalApplicationEvents(ForecastWorkflowServiceGrpc.ForecastWorkflowServiceStub budgetingForecastWorkflowService, LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceStub ledgerLedgerAccountNotificationService) {
        this.budgetingForecastWorkflowService = budgetingForecastWorkflowService;
        this.ledgerLedgerAccountNotificationService = ledgerLedgerAccountNotificationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RfqApprovalApplication.ApproveRfqStage0Requested event) {
        event.context().run(() -> {
            try {
                budgetingForecastWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .cancelForecastWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RfqApprovalApplication.RejectRfqStage2Requested event) {
        event.context().run(() -> {
            try {
                ledgerLedgerAccountNotificationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listLedgerAccountSubscriptions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
