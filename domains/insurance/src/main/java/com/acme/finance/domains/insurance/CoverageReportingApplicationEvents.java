package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CoverageReportingApplicationEvents {
    private final PolicyValidationServiceGrpc.PolicyValidationServiceStub insurancePolicyValidationService;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceStub insurancePremiumValidationService;

    public CoverageReportingApplicationEvents(PolicyValidationServiceGrpc.PolicyValidationServiceStub insurancePolicyValidationService, PremiumValidationServiceGrpc.PremiumValidationServiceStub insurancePremiumValidationService) {
        this.insurancePolicyValidationService = insurancePolicyValidationService;
        this.insurancePremiumValidationService = insurancePremiumValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CoverageReportingApplication.ShareCoverageReportStage1Requested event) {
        event.context().run(() -> {
            try {
                insurancePolicyValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .lintPolicies(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CoverageReportingApplication.DeleteCoverageReportStage1Requested event) {
        event.context().run(() -> {
            try {
                insurancePremiumValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validatePremium(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
