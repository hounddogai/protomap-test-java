package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class GoodsReceiptCalculationApplicationEvents {
    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceStub auditAuditEventHistoryService;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceStub budgetingBudgetLineCalculationService;

    private final VarianceCalculationServiceGrpc.VarianceCalculationServiceStub budgetingVarianceCalculationService;

    public GoodsReceiptCalculationApplicationEvents(AuditEventHistoryServiceGrpc.AuditEventHistoryServiceStub auditAuditEventHistoryService, BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceStub budgetingBudgetLineCalculationService, VarianceCalculationServiceGrpc.VarianceCalculationServiceStub budgetingVarianceCalculationService) {
        this.auditAuditEventHistoryService = auditAuditEventHistoryService;
        this.budgetingBudgetLineCalculationService = budgetingBudgetLineCalculationService;
        this.budgetingVarianceCalculationService = budgetingVarianceCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(GoodsReceiptCalculationApplication.QuoteGoodsReceiptStage1Requested event) {
        event.context().run(() -> {
            try {
                auditAuditEventHistoryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listAuditEventVersions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(GoodsReceiptCalculationApplication.CompareGoodsReceiptOptionsStage3Requested event) {
        event.context().run(() -> {
            try {
                budgetingVarianceCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .recalculateVariances(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(GoodsReceiptCalculationApplication.RecalculateGoodsReceiptsStage1Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetLineCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .quoteBudgetLine(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
