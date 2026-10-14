package com.acme.finance.domains.contracts;

import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RenewalValidationApplicationEvents {
    private final VarianceCalculationServiceGrpc.VarianceCalculationServiceStub budgetingVarianceCalculationService;

    private final UserServiceGrpc.UserServiceStub identityUserService;

    public RenewalValidationApplicationEvents(VarianceCalculationServiceGrpc.VarianceCalculationServiceStub budgetingVarianceCalculationService, UserServiceGrpc.UserServiceStub identityUserService) {
        this.budgetingVarianceCalculationService = budgetingVarianceCalculationService;
        this.identityUserService = identityUserService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RenewalValidationApplication.ValidateRenewalStage0Requested event) {
        event.context().run(() -> {
            try {
                budgetingVarianceCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .quoteVariance(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RenewalValidationApplication.PreviewRenewalStage3Requested event) {
        event.context().run(() -> {
            try {
                identityUserService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listUsers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
