package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ExposureApprovalApplicationEvents {
    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceStub riskRiskRegisterValidationService;

    public ExposureApprovalApplicationEvents(RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceStub riskRiskRegisterValidationService) {
        this.riskRiskRegisterValidationService = riskRiskRegisterValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ExposureApprovalApplication.DelegateExposureApprovalStage2Requested event) {
        event.context().run(() -> {
            try {
                riskRiskRegisterValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .lintRiskRegisters(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
