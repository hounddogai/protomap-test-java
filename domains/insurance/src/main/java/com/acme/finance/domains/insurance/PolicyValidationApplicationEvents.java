package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PolicyValidationApplicationEvents {
    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceStub insurancePolicyCalculationService;

    public PolicyValidationApplicationEvents(PolicyCalculationServiceGrpc.PolicyCalculationServiceStub insurancePolicyCalculationService) {
        this.insurancePolicyCalculationService = insurancePolicyCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PolicyValidationApplication.CheckPolicyEligibilityStage2Requested event) {
        event.context().run(() -> {
            try {
                insurancePolicyCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .recalculatePolicies(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
