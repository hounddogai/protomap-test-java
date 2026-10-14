package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ControlLifecycleApplicationEvents {
    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceStub complianceComplianceCaseValidationService;

    public ControlLifecycleApplicationEvents(ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceStub complianceComplianceCaseValidationService) {
        this.complianceComplianceCaseValidationService = complianceComplianceCaseValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ControlLifecycleApplication.ExpireControlStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceComplianceCaseValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .normalizeComplianceCase(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
