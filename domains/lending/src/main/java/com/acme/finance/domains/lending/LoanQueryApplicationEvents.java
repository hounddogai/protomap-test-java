package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class LoanQueryApplicationEvents {
    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceStub insuranceCoverageCalculationService;

    public LoanQueryApplicationEvents(CoverageCalculationServiceGrpc.CoverageCalculationServiceStub insuranceCoverageCalculationService) {
        this.insuranceCoverageCalculationService = insuranceCoverageCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LoanQueryApplication.BrowseLoansStage0Requested event) {
        event.context().run(() -> {
            try {
                insuranceCoverageCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculateCoverage(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
