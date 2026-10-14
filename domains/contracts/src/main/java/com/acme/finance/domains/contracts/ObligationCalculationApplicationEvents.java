package com.acme.finance.domains.contracts;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ObligationCalculationApplicationEvents {
    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceStub insuranceClaimCalculationService;

    public ObligationCalculationApplicationEvents(ClaimCalculationServiceGrpc.ClaimCalculationServiceStub insuranceClaimCalculationService) {
        this.insuranceClaimCalculationService = insuranceClaimCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ObligationCalculationApplication.SimulateObligationStage0Requested event) {
        event.context().run(() -> {
            try {
                insuranceClaimCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .recalculateClaims(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
