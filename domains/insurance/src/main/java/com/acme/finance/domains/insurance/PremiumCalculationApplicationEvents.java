package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.recruiting.v1.JobOpeningCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PremiumCalculationApplicationEvents {
    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceStub insuranceClaimCalculationService;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceStub insuranceClaimValidationService;

    private final JobOpeningCalculationServiceGrpc.JobOpeningCalculationServiceStub recruitingJobOpeningCalculationService;

    public PremiumCalculationApplicationEvents(ClaimCalculationServiceGrpc.ClaimCalculationServiceStub insuranceClaimCalculationService, ClaimValidationServiceGrpc.ClaimValidationServiceStub insuranceClaimValidationService, JobOpeningCalculationServiceGrpc.JobOpeningCalculationServiceStub recruitingJobOpeningCalculationService) {
        this.insuranceClaimCalculationService = insuranceClaimCalculationService;
        this.insuranceClaimValidationService = insuranceClaimValidationService;
        this.recruitingJobOpeningCalculationService = recruitingJobOpeningCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PremiumCalculationApplication.CalculatePremiumStage2Requested event) {
        event.context().run(() -> {
            try {
                recruitingJobOpeningCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculateJobOpening(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PremiumCalculationApplication.RecalculatePremiumsStage0Requested event) {
        event.context().run(() -> {
            try {
                insuranceClaimValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validateClaim(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PremiumCalculationApplication.RecalculatePremiumsStage3Requested event) {
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
