package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ExposureCalculationApplicationEvents {
    private final LoanCalculationServiceGrpc.LoanCalculationServiceStub lendingLoanCalculationService;

    public ExposureCalculationApplicationEvents(LoanCalculationServiceGrpc.LoanCalculationServiceStub lendingLoanCalculationService) {
        this.lendingLoanCalculationService = lendingLoanCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ExposureCalculationApplication.ExplainExposureCalculationStage0Requested event) {
        event.context().run(() -> {
            try {
                lendingLoanCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .simulateLoan(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
