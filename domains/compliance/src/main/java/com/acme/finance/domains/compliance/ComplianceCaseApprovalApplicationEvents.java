package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
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
public class ComplianceCaseApprovalApplicationEvents {
    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceStub complianceComplianceCaseCalculationService;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceStub complianceControlCalculationService;

    public ComplianceCaseApprovalApplicationEvents(ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceStub complianceComplianceCaseCalculationService, ControlCalculationServiceGrpc.ControlCalculationServiceStub complianceControlCalculationService) {
        this.complianceComplianceCaseCalculationService = complianceComplianceCaseCalculationService;
        this.complianceControlCalculationService = complianceControlCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ComplianceCaseApprovalApplication.RejectComplianceCaseStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceControlCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .estimateControl(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ComplianceCaseApprovalApplication.GetComplianceCaseApprovalStatusStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceComplianceCaseCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculateComplianceCase(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
