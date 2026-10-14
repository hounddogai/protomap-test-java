package com.acme.finance.domains.lending;

import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CollateralLifecycleApplicationEvents {
    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceStub customersCustomerSegmentValidationService;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceStub customersHouseholdCalculationService;

    public CollateralLifecycleApplicationEvents(CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceStub customersCustomerSegmentValidationService, HouseholdCalculationServiceGrpc.HouseholdCalculationServiceStub customersHouseholdCalculationService) {
        this.customersCustomerSegmentValidationService = customersCustomerSegmentValidationService;
        this.customersHouseholdCalculationService = customersHouseholdCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CollateralLifecycleApplication.ActivateCollateralStage0Requested event) {
        event.context().run(() -> {
            try {
                customersHouseholdCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .estimateHousehold(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CollateralLifecycleApplication.GetCollateralLifecycleStage0Requested event) {
        event.context().run(() -> {
            try {
                customersCustomerSegmentValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainCustomerSegmentDecision(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
