package com.acme.finance.domains.treasury;

import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CashPositionCalculationApplicationEvents {
    private final ContractClauseQueryServiceGrpc.ContractClauseQueryServiceStub contractsContractClauseQueryService;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceStub expensesExpenseReportCalculationService;

    private final LoanValidationServiceGrpc.LoanValidationServiceStub lendingLoanValidationService;

    public CashPositionCalculationApplicationEvents(ContractClauseQueryServiceGrpc.ContractClauseQueryServiceStub contractsContractClauseQueryService, ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceStub expensesExpenseReportCalculationService, LoanValidationServiceGrpc.LoanValidationServiceStub lendingLoanValidationService) {
        this.contractsContractClauseQueryService = contractsContractClauseQueryService;
        this.expensesExpenseReportCalculationService = expensesExpenseReportCalculationService;
        this.lendingLoanValidationService = lendingLoanValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CashPositionCalculationApplication.CalculateCashPositionStage2Requested event) {
        event.context().run(() -> {
            try {
                expensesExpenseReportCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculateExpenseReport(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CashPositionCalculationApplication.QuoteCashPositionStage1Requested event) {
        event.context().run(() -> {
            try {
                lendingLoanValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewLoan(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CashPositionCalculationApplication.SimulateCashPositionStage0Requested event) {
        event.context().run(() -> {
            try {
                contractsContractClauseQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .findContractClauses(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
