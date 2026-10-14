package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.cart.v1.CartCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.merchandising.v1.CollectionQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class BudgetLineApprovalApplicationEvents {
    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceStub budgetingBudgetLineReportingService;

    private final CartCalculationServiceGrpc.CartCalculationServiceStub cartCartCalculationService;

    private final CollectionQueryServiceGrpc.CollectionQueryServiceStub merchandisingCollectionQueryService;

    public BudgetLineApprovalApplicationEvents(BudgetLineReportingServiceGrpc.BudgetLineReportingServiceStub budgetingBudgetLineReportingService, CartCalculationServiceGrpc.CartCalculationServiceStub cartCartCalculationService, CollectionQueryServiceGrpc.CollectionQueryServiceStub merchandisingCollectionQueryService) {
        this.budgetingBudgetLineReportingService = budgetingBudgetLineReportingService;
        this.cartCartCalculationService = cartCartCalculationService;
        this.merchandisingCollectionQueryService = merchandisingCollectionQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BudgetLineApprovalApplication.ListPendingBudgetLinesStage1Requested event) {
        event.context().run(() -> {
            try {
                merchandisingCollectionQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .suggestCollections(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BudgetLineApprovalApplication.RecallBudgetLineStage2Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetLineReportingService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .fetchBudgetLineReport(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BudgetLineApprovalApplication.CommentBudgetLineApprovalStage1Requested event) {
        event.context().run(() -> {
            try {
                cartCartCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .estimateCart(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
