package com.acme.finance.domains.collections;

import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
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
public class WriteOffWorkflowApplicationEvents {
    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceStub collectionsPaymentPromiseWorkflowService;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceStub insuranceCoverageReportingService;

    public WriteOffWorkflowApplicationEvents(PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceStub collectionsPaymentPromiseWorkflowService, CoverageReportingServiceGrpc.CoverageReportingServiceStub insuranceCoverageReportingService) {
        this.collectionsPaymentPromiseWorkflowService = collectionsPaymentPromiseWorkflowService;
        this.insuranceCoverageReportingService = insuranceCoverageReportingService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(WriteOffWorkflowApplication.ResumeWriteOffWorkflowStage3Requested event) {
        event.context().run(() -> {
            try {
                insuranceCoverageReportingService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .shareCoverageReport(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(WriteOffWorkflowApplication.InspectWriteOffWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                collectionsPaymentPromiseWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .cancelPaymentPromiseWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
