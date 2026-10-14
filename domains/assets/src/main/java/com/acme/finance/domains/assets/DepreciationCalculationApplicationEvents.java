package com.acme.finance.domains.assets;

import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DepreciationCalculationApplicationEvents {
    private final AttestationValidationServiceGrpc.AttestationValidationServiceStub complianceAttestationValidationService;

    public DepreciationCalculationApplicationEvents(AttestationValidationServiceGrpc.AttestationValidationServiceStub complianceAttestationValidationService) {
        this.complianceAttestationValidationService = complianceAttestationValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DepreciationCalculationApplication.CalculateDepreciationStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceAttestationValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validateAttestation(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
