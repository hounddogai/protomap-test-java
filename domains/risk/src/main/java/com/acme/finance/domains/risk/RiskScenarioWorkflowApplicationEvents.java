package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RiskScenarioWorkflowApplicationEvents {
    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceStub riskRiskRegisterWorkflowService;

    public RiskScenarioWorkflowApplicationEvents(RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceStub riskRiskRegisterWorkflowService) {
        this.riskRiskRegisterWorkflowService = riskRiskRegisterWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RiskScenarioWorkflowApplication.SignalRiskScenarioWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                riskRiskRegisterWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .startRiskRegisterWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
