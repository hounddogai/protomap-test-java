package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class LoanApplicationCalculationApplicationEvents {
    private final ClaimValidationServiceGrpc.ClaimValidationServiceStub insuranceClaimValidationService;

    public LoanApplicationCalculationApplicationEvents(ClaimValidationServiceGrpc.ClaimValidationServiceStub insuranceClaimValidationService) {
        this.insuranceClaimValidationService = insuranceClaimValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LoanApplicationCalculationApplication.ExplainLoanApplicationCalculationStage0Requested event) {
        event.context().run(() -> {
            try {
                insuranceClaimValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .normalizeClaim(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
