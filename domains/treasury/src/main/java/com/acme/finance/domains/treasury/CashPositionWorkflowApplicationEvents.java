package com.acme.finance.domains.treasury;

import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CashPositionWorkflowApplicationEvents {
    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceStub customersHouseholdCalculationService;

    private final CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceStub treasuryCashPositionLifecycleService;

    public CashPositionWorkflowApplicationEvents(HouseholdCalculationServiceGrpc.HouseholdCalculationServiceStub customersHouseholdCalculationService, CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceStub treasuryCashPositionLifecycleService) {
        this.customersHouseholdCalculationService = customersHouseholdCalculationService;
        this.treasuryCashPositionLifecycleService = treasuryCashPositionLifecycleService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CashPositionWorkflowApplication.ResumeCashPositionWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                treasuryCashPositionLifecycleService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .getCashPositionLifecycle(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CashPositionWorkflowApplication.SignalCashPositionWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                customersHouseholdCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .compareHouseholdOptions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
