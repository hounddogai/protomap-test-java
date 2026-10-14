package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.venues.v1.VenueServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ReceiptQueryApplicationEvents {
    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceStub expensesExpenseReportCalculationService;

    private final VenueServiceGrpc.VenueServiceStub venuesVenueService;

    public ReceiptQueryApplicationEvents(ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceStub expensesExpenseReportCalculationService, VenueServiceGrpc.VenueServiceStub venuesVenueService) {
        this.expensesExpenseReportCalculationService = expensesExpenseReportCalculationService;
        this.venuesVenueService = venuesVenueService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReceiptQueryApplication.FindReceiptsStage0Requested event) {
        event.context().run(() -> {
            try {
                expensesExpenseReportCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .recalculateExpenseReports(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReceiptQueryApplication.SummarizeReceiptsStage2Requested event) {
        event.context().run(() -> {
            try {
                venuesVenueService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .searchVenues(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
