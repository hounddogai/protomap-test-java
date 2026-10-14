package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AttestationApprovalApplicationEvents {
    private final ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceStub complianceComplianceCaseQueryService;

    public AttestationApprovalApplicationEvents(ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceStub complianceComplianceCaseQueryService) {
        this.complianceComplianceCaseQueryService = complianceComplianceCaseQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AttestationApprovalApplication.CommentAttestationApprovalStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceComplianceCaseQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .summarizeComplianceCases(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
