package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RiskRegisterApplicationEvents {
    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceStub lendingLoanApplicationValidationService;

    public RiskRegisterApplicationEvents(LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceStub lendingLoanApplicationValidationService) {
        this.lendingLoanApplicationValidationService = lendingLoanApplicationValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RiskRegisterApplication.ListRiskRegistersStage0Requested event) {
        event.context().run(() -> {
            try {
                lendingLoanApplicationValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainLoanApplicationDecision(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
