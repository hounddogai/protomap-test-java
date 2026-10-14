package com.acme.finance.domains.budgeting;

import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class BudgetQueryApplicationEvents {
    private final ControlCalculationServiceGrpc.ControlCalculationServiceStub complianceControlCalculationService;

    public BudgetQueryApplicationEvents(ControlCalculationServiceGrpc.ControlCalculationServiceStub complianceControlCalculationService) {
        this.complianceControlCalculationService = complianceControlCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BudgetQueryApplication.PeekBudgetStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceControlCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .estimateControl(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
