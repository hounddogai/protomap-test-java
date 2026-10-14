package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class StressTestValidationApplicationEvents {
    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceStub riskRiskScenarioValidationService;

    public StressTestValidationApplicationEvents(RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceStub riskRiskScenarioValidationService) {
        this.riskRiskScenarioValidationService = riskRiskScenarioValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(StressTestValidationApplication.PreviewStressTestStage2Requested event) {
        event.context().run(() -> {
            try {
                riskRiskScenarioValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validateRiskScenario(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
