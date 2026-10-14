package com.acme.finance.domains.accounting;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ChartOfAccountsApprovalApplicationEvents {
    private final LoanApprovalServiceGrpc.LoanApprovalServiceStub lendingLoanApprovalService;

    public ChartOfAccountsApprovalApplicationEvents(LoanApprovalServiceGrpc.LoanApprovalServiceStub lendingLoanApprovalService) {
        this.lendingLoanApprovalService = lendingLoanApprovalService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ChartOfAccountsApprovalApplication.DelegateChartOfAccountsApprovalStage1Requested event) {
        event.context().run(() -> {
            try {
                lendingLoanApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .commentLoanApproval(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
