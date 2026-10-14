package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RepaymentValidationApplicationEvents {
    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceStub lendingCollateralCalculationService;

    public RepaymentValidationApplicationEvents(CollateralCalculationServiceGrpc.CollateralCalculationServiceStub lendingCollateralCalculationService) {
        this.lendingCollateralCalculationService = lendingCollateralCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RepaymentValidationApplication.GetRepaymentRulesStage0Requested event) {
        event.context().run(() -> {
            try {
                lendingCollateralCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .simulateCollateral(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
