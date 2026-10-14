package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class StressTestLifecycleApplicationEvents {
    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterService;

    public StressTestLifecycleApplicationEvents(RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterService) {
        this.riskRiskRegisterService = riskRiskRegisterService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(StressTestLifecycleApplication.ActivateStressTestStage1Requested event) {
        event.context().run(() -> {
            try {
                riskRiskRegisterService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .updateRiskRegister(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
