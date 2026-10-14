package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ClaimApprovalApplicationEvents {
    private final CoverageReportingServiceGrpc.CoverageReportingServiceStub insuranceCoverageReportingService;

    public ClaimApprovalApplicationEvents(CoverageReportingServiceGrpc.CoverageReportingServiceStub insuranceCoverageReportingService) {
        this.insuranceCoverageReportingService = insuranceCoverageReportingService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ClaimApprovalApplication.ListPendingClaimsStage0Requested event) {
        event.context().run(() -> {
            try {
                insuranceCoverageReportingService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listCoverageReports(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
