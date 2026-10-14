package com.acme.finance.domains.budgeting;

import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ForecastCalculationApplicationEvents {
    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceStub featureflagsFeatureFlagValidationService;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceStub lendingLoanApplicationQueryService;

    public ForecastCalculationApplicationEvents(FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceStub featureflagsFeatureFlagValidationService, LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceStub lendingLoanApplicationQueryService) {
        this.featureflagsFeatureFlagValidationService = featureflagsFeatureFlagValidationService;
        this.lendingLoanApplicationQueryService = lendingLoanApplicationQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ForecastCalculationApplication.CompareForecastOptionsStage0Requested event) {
        event.context().run(() -> {
            try {
                featureflagsFeatureFlagValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkFeatureFlagEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ForecastCalculationApplication.CompareForecastOptionsStage1Requested event) {
        event.context().run(() -> {
            try {
                lendingLoanApplicationQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .countLoanApplications(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ForecastCalculationApplication.RecalculateForecastsStage2Requested event) {
        event.context().run(() -> {
            try {
                lendingLoanApplicationQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .countLoanApplications(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
